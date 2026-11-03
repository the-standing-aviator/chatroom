package gateway

import (
	"net"
	"time"

	"communicate/internal/protocol"
)

// conn 抽象一个「面向消息」的传输连接：读一条消息 / 写一条消息。
// gateway 的会话管理、鉴权、消息路由都只依赖这两件事，不关心底层是
// TCP 字节流（长度前缀分帧）还是 WebSocket 帧——这是 ports & adapters 在传输层
// 上的体现：tcpConn 与（后续的）wsConn 是同一个 port 的两个 adapter。
type conn interface {
	ReadMessage() (protocol.Message, error) // 读一条 Message：TCP 解长度前缀帧，WS 解 JSON 文本帧
	WriteMessage(protocol.Message) error    // 写一条 Message：TCP 封长度前缀帧，WS 发 JSON 文本帧
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	RemoteAddr() string // 对端地址，用于日志
	Close() error
}

// tcpConn 是 conn 的 TCP 实现：把「长度前缀 + JSON」字节流适配成逐条 Message。
// 内嵌 net.Conn 直接复用它的 SetReadDeadline/SetWriteDeadline/Close，
// 只把「字节流 ↔ 消息」这一层补齐。
type tcpConn struct {
	net.Conn
}

func (c *tcpConn) ReadMessage() (protocol.Message, error) {
	return protocol.ReadFrame(c.Conn)
}

func (c *tcpConn) WriteMessage(m protocol.Message) error {
	return protocol.WriteFrame(c.Conn, m)
}

func (c *tcpConn) RemoteAddr() string {
	return c.Conn.RemoteAddr().String()
}
