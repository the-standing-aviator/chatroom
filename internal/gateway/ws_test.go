package gateway

import (
	"net/http"
	"testing"
)

// TestWebSocketOriginCheck 验证 Origin 白名单逻辑：这是上线公网前的安全必做项。
func TestWebSocketOriginCheck(t *testing.T) {
	// 默认（未配置白名单）：放行所有来源，兼容开发/本地联调。
	if allowAll := newUpgrader(nil); !allowAll.CheckOrigin(reqWithOrigin("https://evil.example")) {
		t.Fatalf("默认应放行所有来源")
	}

	// 配置白名单后：只放行列表内的 Origin。
	strict := newUpgrader([]string{"https://chat.example.com"})

	if !strict.CheckOrigin(reqWithOrigin("https://chat.example.com")) {
		t.Fatalf("白名单内的 Origin 应放行")
	}
	if strict.CheckOrigin(reqWithOrigin("https://evil.example")) {
		t.Fatalf("白名单外的 Origin 应拒绝")
	}

	// 无 Origin 头（非浏览器客户端，如 Go 客户端/curl）：不携带浏览器跨站凭据，
	// 无 CSRF 风险，放行——否则会误伤我们自己的工具与终端客户端。
	if !strict.CheckOrigin(reqWithoutOrigin()) {
		t.Fatalf("无 Origin 头应放行（非浏览器客户端）")
	}
}

func reqWithOrigin(origin string) *http.Request {
	r, _ := http.NewRequest("GET", "/ws", nil)
	r.Header.Set("Origin", origin)
	return r
}

func reqWithoutOrigin() *http.Request {
	r, _ := http.NewRequest("GET", "/ws", nil)
	return r
}
