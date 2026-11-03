package gateway

import (
	"sync"
	"time"

	"communicate/internal/protocol"
)

const (
	sendBuffer   = 64               // 业务消息发送队列缓冲大小
	sysBuffer    = 8                // 系统消息队列缓冲大小（比业务队列小，满则丢）
	writeTimeout = 10 * time.Second // 写超时：单条消息写不出去（慢客户端不读）则断开
)

// Session 表示一个已连接的客户端会话（含鉴权后的在线状态）。
type Session struct {
	conn    conn // 传输连接（tcpConn/wsConn）：Session 只面向「消息」，不关心底层是字节流还是 WebSocket 帧
	gateway *Gateway

	nameMu sync.RWMutex
	name   string // 当前昵称，用 nameMu 保护（修复 P1-3 数据竞态）

	sendCh chan protocol.Message // 业务消息队列（chat/私聊/错误提示等，带缓冲）
	sysCh  chan protocol.Message // 系统消息队列（上下线/改名，尽力而为，满则丢）
	once   sync.Once
}

// newSession 创建一个会话并启动写协程。
func newSession(c conn, gw *Gateway) *Session {
	s := &Session{
		conn:    c,
		gateway: gw,
		sendCh:  make(chan protocol.Message, sendBuffer),
		sysCh:   make(chan protocol.Message, sysBuffer),
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
	// 由 gw.mu 保证互斥，不会走到这里；此处只是防御，避免极端情况崩进程。
	defer func() { _ = recover() }()
	select {
	case s.sendCh <- m:
	default:
		s.gateway.dropped.Add(1)
	}
}

// sendSystem 非阻塞投递一条系统消息（上下线、改名等）。
// 与 send 不同：系统消息尽力而为，队列满时静默丢弃（不计数、不阻塞），
// 因为上下线广播丢一两条无伤大雅，不该为此挤占业务消息的队列或拖慢广播。
func (s *Session) sendSystem(m protocol.Message) {
	defer func() { _ = recover() }()
	select {
	case s.sysCh <- m:
	default:
		// 系统消息满了就丢，不计数（它本身就是"尽力而为"）。
	}
}

// writeLoop 把发送队列里的消息逐个写出到连接。
// 两条队列分级：业务消息（sendCh）优先于系统消息（sysCh）——只要业务队列有货就先写，
// 系统消息只在业务队列空时才写，保证上下线/改名风暴不挤占 chat。
// 队列被 close（见 close）后，把剩余消息 flush 完再关闭连接，
// 保证"密码错误"等最后一条提示能送达客户端，而不是随连接一起被丢弃。
func (s *Session) writeLoop() {
	sendCh, sysCh := s.sendCh, s.sysCh
	for sendCh != nil || sysCh != nil {
		// 第一层：业务消息优先，sendCh 有货立刻写，不让系统消息插队。
		select {
		case m, ok := <-sendCh:
			if !ok {
				sendCh = nil // 业务队列已关闭，之后只写系统消息
				continue
			}
			if !s.writeOne(m) {
				return
			}
			continue
		default:
		}

		// 第二层：业务队列空，等业务消息，或写系统消息。
		select {
		case m, ok := <-sendCh:
			if !ok {
				sendCh = nil
				continue
			}
			if !s.writeOne(m) {
				return
			}
		case m, ok := <-sysCh:
			if !ok {
				sysCh = nil // 系统队列已关闭
				continue
			}
			if !s.writeOne(m) {
				return
			}
		}
	}
	// 两条队列都已关闭且消费完：flush 完成，关闭连接。
	_ = s.conn.Close()
}

// writeOne 写出一帧消息；写失败（对端断开/超时）则关闭连接并返回 false。
func (s *Session) writeOne(m protocol.Message) bool {
	_ = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := s.conn.WriteMessage(m); err != nil {
		_ = s.conn.Close()
		return false
	}
	return true
}

// close 幂等关闭会话：移出在线表并广播下线，然后关闭发送队列让写协程 flush 后退出。
// 连接由 writeLoop 在 flush 完成后统一关闭，这里不直接关，否则会打断最后一条消息的写出。
func (s *Session) close() {
	s.once.Do(func() {
		// 已鉴权（有昵称）才广播下线并移出在线表。
		if name := s.Name(); name != "" {
			s.gateway.removeSession(s)
			s.gateway.broadcastSystem(name + " 已下线")
		}
		close(s.sendCh)
		close(s.sysCh)
	})
}
