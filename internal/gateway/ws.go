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

// newUpgrader 构造一个 WebSocket 升级器。ReadBufferSize/WriteBufferSize 与 TCP 分帧
// 缓冲量级一致即可；CheckOrigin 按白名单收紧——这是上线公网前的安全必做项，
// 否则任何网站都能用你的服务做跨站 WebSocket（CSRF 向量）。
func newUpgrader(allowedOrigins []string) *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			// 未配置白名单：开发/本地联调，放行所有来源。
			if len(allowedOrigins) == 0 {
				return true
			}
			origin := r.Header.Get("Origin")
			// 无 Origin 头：非浏览器客户端（Go 客户端 / curl 等）。它们不携带浏览器
			// 的跨站凭据，无 CSRF 风险，放行。
			if origin == "" {
				return true
			}
			for _, allowed := range allowedOrigins {
				if origin == allowed {
					return true
				}
			}
			return false
		},
	}
}

// SetAllowedOrigins 设置 WebSocket Origin 白名单（origin 字符串列表）。
// 必须在启动 HTTP 服务前调用（之后只读，无并发写）；空列表 = 放行所有来源（开发）。
func (gw *Gateway) SetAllowedOrigins(origins []string) {
	gw.allowedOrigins = origins
}

// HandleWebSocket 处理 /ws 的升级请求：升级成功后把连接交给 handle，与 TCP 走同一套
// 会话管理 / 鉴权 / 路由逻辑（这是 conn 接口的直接收益，业务代码零改动）。
// SetReadLimit 限制单条消息大小，防恶意客户端发超长帧撑爆内存。
func (gw *Gateway) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	c, err := newUpgrader(gw.allowedOrigins).Upgrade(w, r, nil)
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
