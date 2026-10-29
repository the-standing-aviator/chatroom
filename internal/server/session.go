package server

import (
	"net"
	"sync"

	"communicate/internal/protocol"
)

const sendBuffer = 64 // 发送队列缓冲大小

// Session 表示一个已连接的客户端会话（含鉴权后的在线状态）。
type Session struct {
	conn   net.Conn
	server *Server

	nameMu sync.RWMutex
	name   string // 当前昵称，用 nameMu 保护（修复 P1-3 数据竞态）

	sendCh chan protocol.Message // 发送队列（带缓冲）
	quit   chan struct{}
	once   sync.Once
}

// newSession 创建一个会话并启动写协程。
func newSession(conn net.Conn, srv *Server) *Session {
	s := &Session{
		conn:   conn,
		server: srv,
		sendCh: make(chan protocol.Message, sendBuffer),
		quit:   make(chan struct{}),
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
	select {
	case s.sendCh <- m:
	default:
		s.server.dropped.Add(1)
	}
}

// writeLoop 把发送队列里的消息逐个写出到连接。写失败或收到退出信号则结束。
func (s *Session) writeLoop() {
	for {
		select {
		case m := <-s.sendCh:
			if err := protocol.WriteFrame(s.conn, m); err != nil {
				s.close()
				return
			}
		case <-s.quit:
			return
		}
	}
}

// close 幂等关闭会话：发送下线广播、移出在线表、关闭连接与写协程。
func (s *Session) close() {
	s.once.Do(func() {
		close(s.quit)
		_ = s.conn.Close()

		// 已鉴权（有昵称）才广播下线并移出在线表。
		if name := s.Name(); name != "" {
			s.server.removeSession(s)
			s.server.broadcastSystem(name + " 已下线")
		}
	})
}
