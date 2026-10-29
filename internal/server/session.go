package server

import (
	"net"
	"sync"
	"time"

	"communicate/internal/protocol"
)

const (
	sendBuffer   = 64               // 发送队列缓冲大小
	writeTimeout = 10 * time.Second // 写超时：单条消息写不出去（慢客户端不读）则断开
)

// Session 表示一个已连接的客户端会话（含鉴权后的在线状态）。
type Session struct {
	conn   net.Conn
	server *Server

	nameMu sync.RWMutex
	name   string // 当前昵称，用 nameMu 保护（修复 P1-3 数据竞态）

	sendCh chan protocol.Message // 发送队列（带缓冲）
	once   sync.Once
}

// newSession 创建一个会话并启动写协程。
func newSession(conn net.Conn, srv *Server) *Session {
	s := &Session{
		conn:   conn,
		server: srv,
		sendCh: make(chan protocol.Message, sendBuffer),
	}
	go s.writeLoop()
	return s
}

// Name 返回当前昵称。
func (s *Session) Name() string {
	s.nameMu.RLock()
	defer s.nameMu.RUnlock()
	return s.name
}

// setName 设置昵称。
func (s *Session) setName(name string) {
	s.nameMu.Lock()
	s.name = name
	s.nameMu.Unlock()
}

// send 非阻塞投递一条消息；队列满时丢弃并计数（避免慢客户端拖垮广播）。
func (s *Session) send(m protocol.Message) {
	// recover 兜底：会话关闭后 sendCh 已 close，若仍有并发 send 会 panic
	// （写失败触发 close 的极小竞态窗口）。正常流程下 close 与 broadcast 的 send
	// 由 srv.mu 保证互斥，不会走到这里；此处只是防御，避免极端情况崩进程。
	defer func() { _ = recover() }()
	select {
	case s.sendCh <- m:
	default:
		s.server.dropped.Add(1)
	}
}

// writeLoop 把发送队列里的消息逐个写出到连接。
// sendCh 被 close（见 close）后，会把剩余消息 flush 完再关闭连接，
// 保证"密码错误"等最后一条提示能送达客户端，而不是随连接一起被丢弃。
func (s *Session) writeLoop() {
	for m := range s.sendCh {
		_ = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err := protocol.WriteFrame(s.conn, m); err != nil {
			_ = s.conn.Close()
			return
		}
	}
	// 队列已关闭且消费完：flush 完成，关闭连接。
	_ = s.conn.Close()
}

// close 幂等关闭会话：移出在线表并广播下线，然后关闭发送队列让写协程 flush 后退出。
// 连接由 writeLoop 在 flush 完成后统一关闭，这里不直接关，否则会打断最后一条消息的写出。
func (s *Session) close() {
	s.once.Do(func() {
		// 已鉴权（有昵称）才广播下线并移出在线表。
		if name := s.Name(); name != "" {
			s.server.removeSession(s)
			s.server.broadcastSystem(name + " 已下线")
		}
		close(s.sendCh)
	})
}
