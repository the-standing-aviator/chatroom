// Package server 实现聊天室服务端：连接管理、鉴权、消息路由与广播。
package server

import (
	"fmt"
	"net"
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

	dropped atomic.Int64 // 因发送队列满被丢弃的消息计数
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
	}
}

// Start 监听端口，并把每个连接交给 handle 协程处理。阻塞直到监听失败。
func (srv *Server) Start() error {
	ln, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return err
	}
	srv.log.Info("server started", zap.String("addr", srv.addr))

	go srv.logStats()

	for {
		conn, err := ln.Accept()
		if err != nil {
			srv.log.Error("accept error", zap.Error(err))
			continue
		}
		go srv.handle(conn)
	}
}

// broadcast 把一条公聊消息广播给所有在线用户，并持久化。
func (srv *Server) broadcast(from, content string) {
	ts := time.Now().Unix()
	m := protocol.Message{Type: protocol.TypeBroadcast, From: from, Content: content, TS: ts}

	srv.mu.RLock()
	for _, s := range srv.sessions {
		s.send(m)
	}
	srv.mu.RUnlock()

	if err := srv.store.SaveMessage(store.Message{Type: "chat", From: from, Content: content, TS: time.Unix(ts, 0)}); err != nil {
		srv.log.Error("save broadcast failed", zap.Error(err))
	}
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
		if err := srv.store.SaveMessage(store.Message{Type: "private", From: from, To: to, Content: content, TS: time.Unix(ts, 0)}); err != nil {
			srv.log.Error("save private failed", zap.Error(err))
		}
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
