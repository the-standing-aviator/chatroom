// Package gateway 实现聊天室接入节点（gateway）：连接管理、鉴权、消息路由与广播。
package gateway

import (
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/protocol"
	"communicate/internal/store"
)

const (
	readTimeout = 30 * time.Second // 读超时：超过该时长无任何数据则判定断线
	authTimeout = 10 * time.Second // 鉴权阶段超时
)

// 总线主题：跨节点的消息路由约定。
// - subjectBroadcast / subjectSystem：所有节点都订阅，广播时扇出到各节点本地会话。
// - subjectNode + <addr>：定向投递，每个节点只订阅自己的那个主题，私聊据此送到目标节点。
const (
	subjectBroadcast = "chat.broadcast" // 公聊广播
	subjectSystem    = "chat.system"    // 系统广播（上线/下线/改名）
	subjectNode      = "chat.node."     // 定向投递前缀，后接节点地址
)

// Gateway 聊天接入节点：维护在线会话表，负责广播、私聊与消息路由。
type Gateway struct {
	addr  string
	log   *zap.Logger
	store store.Store

	mu       sync.RWMutex
	sessions map[string]*Session // 本地投递表：昵称 → 本节点持有的会话
	presence presence.Presence   // 在线登记表：昵称 → 节点，可外置（内存/Redis）
	auth     *auth.Manager       // JWT 签发/校验（无状态 token）
	bus      bus.Bus             // 消息总线：广播扇出 / 私聊定向，可外置（内存/NATS）

	saveCh   chan store.Message // 持久化队列：广播先入队，saveLoop 后台落盘
	saveDone chan struct{}      // saveLoop 退出信号，优雅关闭时靠它等 flush 完成
	dropped  atomic.Int64       // 因发送队列满被丢弃的消息计数

	ln       net.Listener   // 监听器，Shutdown 时 Close 以停止接收新连接
	handleWG sync.WaitGroup // 跟踪 handle goroutine，Shutdown 时等其退出
}

// NewGateway 创建一个接入节点实例。log 可为 nil（内部用 zap.NewNop 兜底）。
// p 是在线登记表实现（内存/Redis），am 是 JWT 签发/校验器，b 是消息总线（内存/NATS）——ports & adapters。
func NewGateway(addr string, log *zap.Logger, st store.Store, p presence.Presence, am *auth.Manager, b bus.Bus) *Gateway {
	if log == nil {
		log = zap.NewNop()
	}
	gw := &Gateway{
		addr:     addr,
		log:      log,
		store:    st,
		presence: p,
		auth:     am,
		bus:      b,
		sessions: make(map[string]*Session),
		saveCh:   make(chan store.Message, 1024),
		saveDone: make(chan struct{}),
	}
	gw.subscribeBus()
	return gw
}

// subscribeBus 注册本节点的总线订阅：广播/系统消息扇出到本地会话，定向消息投递给指定本地会话。
// 单机用内存 bus 时，Publish 同步回调这些 handler，等价于原来的直接遍历 sessions；
// 分布式换 NATS 后，回调由 NATS 在后台协程触发，这段代码保持不变（ports & adapters 的关键收益）。
func (gw *Gateway) subscribeBus() {
	// 公聊广播：投递给所有本地会话。
	if _, err := gw.bus.Subscribe(subjectBroadcast, func(m protocol.Message) {
		gw.mu.RLock()
		for _, s := range gw.sessions {
			s.send(m)
		}
		gw.mu.RUnlock()
	}); err != nil {
		gw.log.Error("subscribe broadcast failed", zap.Error(err))
	}
	// 系统广播：投递给所有本地会话（走 sendSystem 独立通道，尽力而为）。
	if _, err := gw.bus.Subscribe(subjectSystem, func(m protocol.Message) {
		gw.mu.RLock()
		for _, s := range gw.sessions {
			s.sendSystem(m)
		}
		gw.mu.RUnlock()
	}); err != nil {
		gw.log.Error("subscribe system failed", zap.Error(err))
	}
	// 定向投递：只投递给目标昵称对应的本地会话（私聊）。
	if _, err := gw.bus.Subscribe(subjectNode+gw.addr, func(m protocol.Message) {
		gw.mu.RLock()
		if s := gw.sessions[m.To]; s != nil {
			s.send(m)
		}
		gw.mu.RUnlock()
	}); err != nil {
		gw.log.Error("subscribe node failed", zap.Error(err))
	}
}

// Start 监听端口，并把每个连接交给 handle 协程处理。阻塞直到监听失败。
func (gw *Gateway) Start() error {
	ln, err := net.Listen("tcp", gw.addr)
	if err != nil {
		return err
	}
	gw.ln = ln
	gw.log.Info("gateway started", zap.String("addr", gw.addr))

	go gw.logStats()
	go gw.saveLoop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // Shutdown 关闭了监听器，正常退出
			}
			gw.log.Error("accept error", zap.Error(err))
			continue
		}
		gw.handleWG.Add(1)
		go func() {
			defer gw.handleWG.Done()
			gw.handle(conn)
		}()
	}
}

// saveLoop 后台持久化：从队列取消息写入存储，与广播路径解耦。
// 队列由进程退出时关闭（见优雅关闭），这里 range 消费完即退出。
func (gw *Gateway) saveLoop() {
	defer close(gw.saveDone)
	for m := range gw.saveCh {
		gw.saveOne(m)
	}
}

// saveOne 落盘一条消息。recover 兜底保证单条落盘 panic 不崩掉 saveLoop
// （saveLoop 是进程级 goroutine，panic 未恢复会崩整个进程，比单连接 panic 更严重）。
func (gw *Gateway) saveOne(m store.Message) {
	defer func() {
		if r := recover(); r != nil {
			gw.log.Error("save message panic",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())),
			)
		}
	}()
	if err := gw.store.SaveMessage(m); err != nil {
		gw.log.Error("save message failed", zap.Error(err))
	}
}

// enqueue 投递一条消息到持久化队列。recover 兜底：Shutdown 关闭 saveCh 后
// 若仍有极端竞态写入，避免 panic（与 session.send 的防御思路一致）。
func (gw *Gateway) enqueue(m store.Message) {
	defer func() { _ = recover() }()
	gw.saveCh <- m
}

// broadcast 把一条公聊消息广播给所有在线用户，并异步持久化。
// 投递走总线（subjectBroadcast）：subscribeBus 注册的 handler 扇出到本地会话。
// 单机 = 内存 bus 同步回调，等价于原来直接遍历 sessions；分布式 = NATS 异步回调，代码不变。
func (gw *Gateway) broadcast(from, content string) {
	ts := time.Now().Unix()
	m := protocol.Message{Type: protocol.TypeBroadcast, From: from, Content: content, TS: ts}
	if err := gw.bus.Publish(subjectBroadcast, m); err != nil {
		gw.log.Error("publish broadcast failed", zap.Error(err))
	}
	// 异步持久化：只入队不落盘，广播延迟不再被磁盘 I/O 拖累。
	gw.enqueue(store.Message{Type: "chat", From: from, Content: content, TS: time.Unix(ts, 0)})
}

// broadcastSystem 广播一条系统提示（上下线、改名等，不持久化）。
// 走总线（subjectSystem），handler 用 sendSystem 独立通道投递：系统消息尽力而为，
// 不挤占业务消息的 sendCh（见 P2-3）。
func (gw *Gateway) broadcastSystem(content string) {
	m := protocol.Message{Type: protocol.TypeSystem, Content: content, TS: time.Now().Unix()}
	if err := gw.bus.Publish(subjectSystem, m); err != nil {
		gw.log.Error("publish system failed", zap.Error(err))
	}
}

// private 向指定昵称的用户发送私聊。先查在线登记表（presence）定位目标节点，
// 再经总线定向投递到该节点（subjectNode+node），由目标节点的 handler 投递给本地会话。
// 目标在线则持久化并返回 true；不在线或查询失败返回 false。
func (gw *Gateway) private(from, to, content string) bool {
	ts := time.Now().Unix()

	node, err := gw.presence.Node(to)
	if err != nil {
		if !errors.Is(err, presence.ErrNotFound) {
			gw.log.Error("presence node failed", zap.Error(err))
		}
		return false
	}
	m := protocol.Message{Type: protocol.TypePrivate, From: from, To: to, Content: content, TS: ts}
	if err := gw.bus.Publish(subjectNode+node, m); err != nil {
		gw.log.Error("publish private failed", zap.Error(err))
		return false
	}
	gw.enqueue(store.Message{Type: "private", From: from, To: to, Content: content, TS: time.Unix(ts, 0)})
	return true
}

// rename 修改会话昵称。新昵称已被占用时返回 error。
// 用 presence.Rename 原子判断「旧名在线 + 新名空闲」并搬移，避免并发改名竞态；
// 成功后再同步本地投递表与会话自身昵称。
func (gw *Gateway) rename(s *Session, newName string) error {
	old := s.Name()
	if err := gw.presence.Rename(old, newName); err != nil {
		if errors.Is(err, presence.ErrConflict) {
			return fmt.Errorf("昵称 %q 已被使用", newName)
		}
		return err
	}

	gw.mu.Lock()
	delete(gw.sessions, old)
	s.setName(newName)
	gw.sessions[newName] = s
	gw.mu.Unlock()
	return nil
}

// who 返回当前所有在线用户的昵称列表（读在线登记表，跨节点全局视角）。
func (gw *Gateway) who() []string {
	names, err := gw.presence.Online()
	if err != nil {
		gw.log.Error("presence online failed", zap.Error(err))
		return []string{}
	}
	return names
}

// addSession 把会话登记进在线状态。用 presence.Join 做「全局唯一」判断：
// 已在线（ErrConflict）或登记失败时返回错误；成功后把会话放进本地投递表供本节点投递。
func (gw *Gateway) addSession(s *Session) error {
	if err := gw.presence.Join(s.Name(), gw.addr); err != nil {
		return err
	}
	gw.mu.Lock()
	gw.sessions[s.Name()] = s
	gw.mu.Unlock()
	return nil
}

// removeSession 把会话移出本地投递表（仅当该昵称仍指向此会话时），并注销在线登记。
func (gw *Gateway) removeSession(s *Session) {
	gw.mu.Lock()
	if cur, ok := gw.sessions[s.Name()]; ok && cur == s {
		delete(gw.sessions, s.Name())
	}
	gw.mu.Unlock()
	_ = gw.presence.Leave(s.Name())
}

// Shutdown 优雅关闭：停止接收新连接，关闭所有会话，并 flush 持久化队列。
// 顺序是关键——先停生产者（handle），再 close 队列，最后等消费者排空，
// 否则 close(saveCh) 后仍有 handle 写入会 panic。
func (gw *Gateway) Shutdown() {
	_ = gw.ln.Close() // 停止 Accept，Start 返回

	// 快照在线会话后逐个关闭（close 内部会再拿 gw.mu，不能持锁遍历，否则死锁）。
	gw.mu.Lock()
	sessions := make([]*Session, 0, len(gw.sessions))
	for _, s := range gw.sessions {
		sessions = append(sessions, s)
	}
	gw.mu.Unlock()

	for _, s := range sessions {
		s.close()
	}

	gw.handleWG.Wait() // 等所有 handle 退出，不再有 saveCh 生产者
	close(gw.saveCh)   // 关闭队列，saveLoop 消费完剩余消息后退出
	<-gw.saveDone      // 等 saveLoop flush 完成
}

// logStats 周期性输出统计。V1 只输出在线数与丢消息数，V2 接入 Prometheus。
func (gw *Gateway) logStats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		gw.mu.RLock()
		online := len(gw.sessions)
		gw.mu.RUnlock()
		gw.log.Info("stats",
			zap.Int("online", online),
			zap.Int64("dropped_messages", gw.dropped.Load()),
		)
	}
}
