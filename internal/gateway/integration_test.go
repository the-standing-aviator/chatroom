package gateway

import (
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/protocol"
)

// 本文件是 V3.3「跑两个节点」的验收测试：两个接入节点（不同 node ID）共用
// Redis(presence) + NATS(bus)，验证跨节点在线列表 / 公聊广播 / 私聊定向。
// 复用 server_test.go 的 net.Pipe 辅助（startClient/login/readFrame 等），
// 只把 presence/bus 从内存实现换成真实中间件。任一中间件不可用则 skip。

// redisPresenceForTest 连接 Redis 并清空在线状态；不可用则 skip。
// 只走 Presence 公开接口（不碰底层 client），靠 Online/Leave 完成探测与清理。
func redisPresenceForTest(t *testing.T) presence.Presence {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	p := presence.NewRedis(addr)
	names, err := p.Online()
	if err != nil {
		t.Skipf("Redis 不可用，跳过跨节点集成测试: %v", err)
	}
	for _, n := range names {
		_ = p.Leave(n)
	}
	t.Cleanup(func() {
		if names, err := p.Online(); err == nil {
			for _, n := range names {
				_ = p.Leave(n)
			}
		}
		_ = p.Close()
	})
	return p
}

// natsBusForTest 连接 NATS；不可用则 skip。
func natsBusForTest(t *testing.T) bus.Bus {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	b, err := bus.NewNATS(url)
	if err != nil {
		t.Skipf("NATS 不可用，跳过跨节点集成测试: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// TestTwoNodesCrossNode 是 V3.3 验收点：alice 落在节点 A、bob 落在节点 B，
// 两者经共享的 Redis/NATS 完成跨节点在线列表、公聊广播与私聊定向。
func TestTwoNodesCrossNode(t *testing.T) {
	p := redisPresenceForTest(t)
	b := natsBusForTest(t)
	st := newFakeStore()                     // 存储用内存 fake：鉴权/落库已被单测覆盖，这里聚焦跨节点路由
	am := auth.New("test-secret", time.Hour) // 两节点共用同一 secret，JWT 可互认

	gwA := NewGateway("node-a", ":0", zap.NewNop(), st, p, am, b)
	gwB := NewGateway("node-b", ":0", zap.NewNop(), st, p, am, b)

	connA := startClient(t, gwA)
	login(t, connA, "alice", "secret")

	connB := startClient(t, gwB)
	login(t, connB, "bob", "secret")

	// alice（节点 A）应收到「bob 已上线」——跨节点系统广播经 NATS 扇出。
	if m := readFrame(t, connA); m.Type != protocol.TypeSystem || m.Content != "bob 已上线" {
		t.Fatalf("alice 应收到 bob 上线广播，实际 %+v", m)
	}

	// 在线列表：两个节点各自查询，都应看到全局的 alice+bob（presence 走 Redis）。
	writeFrame(t, connA, protocol.Message{Type: protocol.TypeWho})
	if whoA := readFrame(t, connA); whoA.Type != protocol.TypeWhoList ||
		!contains(whoA.Users, "alice") || !contains(whoA.Users, "bob") {
		t.Fatalf("节点 A 在线列表 = %v，期望含 alice 和 bob", whoA.Users)
	}
	writeFrame(t, connB, protocol.Message{Type: protocol.TypeWho})
	if whoB := readFrame(t, connB); whoB.Type != protocol.TypeWhoList ||
		!contains(whoB.Users, "alice") || !contains(whoB.Users, "bob") {
		t.Fatalf("节点 B 在线列表 = %v，期望含 alice 和 bob", whoB.Users)
	}

	// 跨节点公聊：alice（节点 A）广播，bob（节点 B）收到。
	writeFrame(t, connA, protocol.Message{Type: protocol.TypeChat, Content: "hello all"})
	if echo := readFrame(t, connA); echo.Type != protocol.TypeBroadcast || echo.Content != "hello all" {
		t.Fatalf("alice 广播回显 = %+v", echo)
	}
	if bc := readFrame(t, connB); bc.Type != protocol.TypeBroadcast || bc.From != "alice" || bc.Content != "hello all" {
		t.Fatalf("bob 收到的跨节点广播 = %+v", bc)
	}

	// 跨节点私聊：alice（节点 A）→ bob（节点 B），经 presence.Node 定位 + NATS 定向投递。
	writeFrame(t, connA, protocol.Message{Type: protocol.TypePrivate, To: "bob", Content: "hi bob"})
	if priv := readFrame(t, connB); priv.Type != protocol.TypePrivate || priv.From != "alice" || priv.Content != "hi bob" {
		t.Fatalf("bob 收到的跨节点私聊 = %+v", priv)
	}
}
