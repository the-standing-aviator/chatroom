// Package server 实现聊天室服务端：连接管理、鉴权、消息路由与广播。
package server

import (
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"communicate/internal/protocol"
	"communicate/internal/store"
)

const (
	readTimeout = 30 * time.Second // 读超时：超过该时长无任何数据则判定断线
	authTimeout = 10 * time.Second // 鉴权阶段超时
)

// Server 聊天服务器：维护在线会话表，负责广播、私聊与消息路由。
type Server struct {
	addr  string
	log   *zap.Logger
	store store.Store

	mu       sync.RWMutex
	sessions map[string]*Session // 以昵称为键的在线会话表

	saveCh   chan store.Message // 持久化队列：广播先入队，saveLoop 后台落盘
	saveDone chan struct{}      // saveLoop 退出信号，优雅关闭时靠它等 flush 完成
	dropped  atomic.Int64       // 因发送队列满被丢弃的消息计数

	ln       net.Listener    // 监听器，Shutdown 时 Close 以停止接收新连接
	handleWG sync.WaitGroup  // 跟踪 handle goroutine，Shutdown 时等其退出
}

// NewServer 创建一个服务器实例。log 可为 nil（内部用 zap.NewNop 兜底）。
func NewServer(addr string, log *zap.Logger, st store.Store) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	return &Server{
		addr:     addr,
		log:      log,
		store:    st,
		sessions: make(map[string]*Session),
		saveCh:   make(chan store.Message, 1024),
		saveDone: make(chan struct{}),
	}
}

// Start 监听端口，并把每个连接交给 handle 协程处理。阻塞直到监听失败。
func (srv *Server) Start() error {
	ln, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return err
	}
	srv.ln = ln
	srv.log.Info("server started", zap.String("addr", srv.addr))

	go srv.logStats()
	go srv.saveLoop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // Shutdown 关闭了监听器，正常退出
			}
			srv.log.Error("accept error", zap.Error(err))
			continue
		}
		srv.handleWG.Add(1)
		go func() {
			defer srv.handleWG.Done()
			srv.handle(conn)
		}()
	}
}

// saveLoop 后台持久化：从队列取消息写入存储，与广播路径解耦。
// 队列由进程退出时关闭（见优雅关闭），这里 range 消费完即退出。
func (srv *Server) saveLoop() {
	defer close(srv.saveDone)
	for m := range srv.saveCh {
		srv.saveOne(m)
	}
}

// saveOne 落盘一条消息。recover 兜底保证单条落盘 panic 不崩掉 saveLoop
// （saveLoop 是进程级 goroutine，panic 未恢复会崩整个进程，比单连接 panic 更严重）。
func (srv *Server) saveOne(m store.Message) {
	defer func() {
		if r := recover(); r != nil {
			srv.log.Error("save message panic",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())),
			)
		}
	}()
	if err := srv.store.SaveMessage(m); err != nil {
		srv.log.Error("save message failed", zap.Error(err))
	}
}

// enqueue 投递一条消息到持久化队列。recover 兜底：Shutdown 关闭 saveCh 后
// 若仍有极端竞态写入，避免 panic（与 session.send 的防御思路一致）。
func (srv *Server) enqueue(m store.Message) {
	defer func() { _ = recover() }()
	srv.saveCh <- m
}

// broadcast 把一条公聊消息广播给所有在线用户，并异步持久化。
func (srv *Server) broadcast(from, content string) {
	ts := time.Now().Unix()
	m := protocol.Message{Type: protocol.TypeBroadcast, From: from, Content: content, TS: ts}

	srv.mu.RLock()
	for _, s := range srv.sessions {
		s.send(m)
	}
	srv.mu.RUnlock()

	// 异步持久化：只入队不落盘，广播延迟不再被磁盘 I/O 拖累。
	srv.enqueue(store.Message{Type: "chat", From: from, Content: content, TS: time.Unix(ts, 0)})
}

// broadcastSystem 广播一条系统提示（上下线、改名等，不持久化）。
func (srv *Server) broadcastSystem(content string) {
	m := protocol.Message{Type: protocol.TypeSystem, Content: content, TS: time.Now().Unix()}
	srv.mu.RLock()
	for _, s := range srv.sessions {
		s.send(m)
	}
	srv.mu.RUnlock()
}

// private 向指定昵称的用户发送私聊。成功持久化并返回 true。
func (srv *Server) private(from, to, content string) bool {
	ts := time.Now().Unix()

	srv.mu.RLock()
	target, ok := srv.sessions[to]
	if ok {
		target.send(protocol.Message{Type: protocol.TypePrivate, From: from, Content: content, TS: ts})
	}
	srv.mu.RUnlock()

	if ok {
		srv.enqueue(store.Message{Type: "private", From: from, To: to, Content: content, TS: time.Unix(ts, 0)})
	}
	return ok
}

// rename 修改会话昵称。新昵称已被占用时返回 error。
func (srv *Server) rename(s *Session, newName string) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if _, ok := srv.sessions[newName]; ok {
		return fmt.Errorf("昵称 %q 已被使用", newName)
	}
	delete(srv.sessions, s.Name())
	s.setName(newName)
	srv.sessions[newName] = s
	return nil
}

// who 返回当前所有在线用户的昵称列表。
func (srv *Server) who() []string {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	names := make([]string, 0, len(srv.sessions))
	for name := range srv.sessions {
		names = append(names, name)
	}
	return names
}

// addSession 把会话加入在线表。若昵称已在线返回 false。
func (srv *Server) addSession(s *Session) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if _, ok := srv.sessions[s.Name()]; ok {
		return false
	}
	srv.sessions[s.Name()] = s
	return true
}

// removeSession 把会话移出在线表（仅当该昵称仍指向此会话时）。
func (srv *Server) removeSession(s *Session) {
	srv.mu.Lock()
	if cur, ok := srv.sessions[s.Name()]; ok && cur == s {
		delete(srv.sessions, s.Name())
	}
	srv.mu.Unlock()
}

// Shutdown 优雅关闭：停止接收新连接，关闭所有会话，并 flush 持久化队列。
// 顺序是关键——先停生产者（handle），再 close 队列，最后等消费者排空，
// 否则 close(saveCh) 后仍有 handle 写入会 panic。
func (srv *Server) Shutdown() {
	_ = srv.ln.Close() // 停止 Accept，Start 返回

	// 快照在线会话后逐个关闭（close 内部会再拿 srv.mu，不能持锁遍历，否则死锁）。
	srv.mu.Lock()
	sessions := make([]*Session, 0, len(srv.sessions))
	for _, s := range srv.sessions {
		sessions = append(sessions, s)
	}
	srv.mu.Unlock()

	for _, s := range sessions {
		s.close()
	}

	srv.handleWG.Wait() // 等所有 handle 退出，不再有 saveCh 生产者
	close(srv.saveCh)   // 关闭队列，saveLoop 消费完剩余消息后退出
	<-srv.saveDone      // 等 saveLoop flush 完成
}

// logStats 周期性输出统计。V1 只输出在线数与丢消息数，V2 接入 Prometheus。
func (srv *Server) logStats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		srv.mu.RLock()
		online := len(srv.sessions)
		srv.mu.RUnlock()
		srv.log.Info("stats",
			zap.Int("online", online),
			zap.Int64("dropped_messages", srv.dropped.Load()),
		)
	}
}
