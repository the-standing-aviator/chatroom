package gateway

import (
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"communicate/internal/protocol"
)

// wsConn 是 conn 的 WebSocket 实现：把浏览器发来的 JSON 文本帧适配成逐条 Message。
// 内嵌 *websocket.Conn 复用它的 SetReadDeadline/SetWriteDeadline/Close，
// 只需补齐「消息 ↔ JSON 文本帧」这一层。gorilla/websocket 天然按帧分界，无需长度前缀。
type wsConn struct {
	*websocket.Conn
}

// ReadMessage 读一帧文本并解析为 Message。这里必须用 c.Conn.ReadMessage() 显式限定
// 内嵌字段调用，因为本方法名遮蔽了 *websocket.Conn 上同名（不同签名）的 ReadMessage。
func (c *wsConn) ReadMessage() (protocol.Message, error) {
	_, data, err := c.Conn.ReadMessage()
	if err != nil {
		return protocol.Message{}, err
	}
	return protocol.Unmarshal(data)
}

// WriteMessage 把 Message 编码为 JSON 文本帧写出。
func (c *wsConn) WriteMessage(m protocol.Message) error {
	body, err := protocol.Marshal(m)
	if err != nil {
		return err
	}
	return c.Conn.WriteMessage(websocket.TextMessage, body)
}

// RemoteAddr 返回对端地址（覆盖内嵌的 net.Addr 返回，统一成 string）。
func (c *wsConn) RemoteAddr() string {
	return c.Conn.RemoteAddr().String()
}

// wsUpgrader 把 HTTP 连接升级为 WebSocket。ReadBufferSize/WriteBufferSize 与 TCP 分帧
// 缓冲量级一致即可；CheckOrigin 先放行所有来源（本地开发/联调需要），
// 上线公网前必须收紧到实际站点域名，否则别的网站能用你的服务做跨站 WebSocket。
var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// HandleWebSocket 处理 /ws 的升级请求：升级成功后把连接交给 handle，与 TCP 走同一套
// 会话管理 / 鉴权 / 路由逻辑（这是 conn 接口的直接收益，业务代码零改动）。
// SetReadLimit 限制单条消息大小，防恶意客户端发超长帧撑爆内存。
func (gw *Gateway) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	c, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		gw.log.Error("websocket upgrade failed", zap.Error(err))
		return
	}
	c.SetReadLimit(protocol.MaxFrameSize)

	gw.handleWG.Add(1)
	go func() {
		defer gw.handleWG.Done()
		gw.handle(&wsConn{Conn: c})
	}()
}
