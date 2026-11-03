package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/protocol"
)

// wsClient 通过 httptest 服务器拨出一个 WebSocket 连接，返回 *websocket.Conn。
// 这是浏览器前端的同款客户端：走 gorilla/websocket 的 JSON 文本帧。
func wsClient(t *testing.T, gw *Gateway) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(gw.HandleWebSocket))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// wsWrite 写一条 JSON 文本帧。
func wsWrite(t *testing.T, c *websocket.Conn, m protocol.Message) {
	t.Helper()
	if err := c.WriteJSON(m); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

// wsRead 读一条 JSON 消息，设读超时避免服务端不写时挂死测试。
func wsRead(t *testing.T, c *websocket.Conn) protocol.Message {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var m protocol.Message
	if err := c.ReadJSON(&m); err != nil {
		t.Fatalf("ws read: %v", err)
	}
	return m
}

// TestWebSocketClientChatsWithTCPClient 是 V4 端到端验收点：证明浏览器（WebSocket）
// 与终端（TCP）走同一套 gateway，能在同一个房间互通——这是 conn 抽象的核心价值。
// alice 用 TCP 连（net.Pipe → tcpConn），bob 用 WebSocket 连（gorilla dialer → wsConn）。
func TestWebSocketClientChatsWithTCPClient(t *testing.T) {
	gw := NewGateway("test-node", ":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour), bus.NewMemory())

	// alice：TCP 客户端（复用 net.Pipe 辅助）。
	tcpConn := startClient(t, gw)
	login(t, tcpConn, "alice", "secret")

	// bob：WebSocket 客户端（浏览器同款）。
	ws := wsClient(t, gw)
	wsWrite(t, ws, protocol.Message{Type: protocol.TypeAuth, Name: "bob", Password: "secret"})

	// bob 自己先收到两条：登录成功（带 token）+ 上线广播。
	if m := wsRead(t, ws); m.Type != protocol.TypeSystem || m.Token == "" {
		t.Fatalf("bob 登录响应 = %+v，期望 system + token", m)
	}
	if m := wsRead(t, ws); m.Type != protocol.TypeSystem || m.Content != "bob 已上线" {
		t.Fatalf("bob 上线广播 = %+v", m)
	}

	// alice（TCP）收到 bob（WS）的上线广播——证明 WS 上线能广播到 TCP 侧。
	if m := readFrame(t, tcpConn); m.Type != protocol.TypeSystem || m.Content != "bob 已上线" {
		t.Fatalf("alice 应收到 bob 上线广播，实际 %+v", m)
	}

	// 跨传输公聊：alice（TCP）发，bob（WS）收。
	writeFrame(t, tcpConn, protocol.Message{Type: protocol.TypeChat, Content: "hello from tcp"})
	if echo := readFrame(t, tcpConn); echo.Type != protocol.TypeBroadcast || echo.Content != "hello from tcp" {
		t.Fatalf("alice 广播回显 = %+v", echo)
	}
	if m := wsRead(t, ws); m.Type != protocol.TypeBroadcast || m.From != "alice" || m.Content != "hello from tcp" {
		t.Fatalf("bob(WS) 收到的广播 = %+v，期望 alice 的 hello from tcp", m)
	}

	// 反向：bob（WS）发，alice（TCP）收。
	wsWrite(t, ws, protocol.Message{Type: protocol.TypeChat, Content: "hello from ws"})
	if echo := wsRead(t, ws); echo.Type != protocol.TypeBroadcast || echo.Content != "hello from ws" {
		t.Fatalf("bob 广播回显 = %+v", echo)
	}
	if m := readFrame(t, tcpConn); m.Type != protocol.TypeBroadcast || m.From != "bob" || m.Content != "hello from ws" {
		t.Fatalf("alice(TCP) 收到的广播 = %+v，期望 bob 的 hello from ws", m)
	}

	// 跨传输在线列表：TCP 的 alice 应看到 WS 的 bob 也在线（presence 全局视角）。
	writeFrame(t, tcpConn, protocol.Message{Type: protocol.TypeWho})
	who := readFrame(t, tcpConn)
	if who.Type != protocol.TypeWhoList || !contains(who.Users, "alice") || !contains(who.Users, "bob") {
		t.Fatalf("alice 在线列表 = %v，期望含 alice 和 bob", who.Users)
	}
}
