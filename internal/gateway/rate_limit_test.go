package gateway

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/protocol"
)

// TestMessageRateLimit 验证消息限流：令牌桶耗尽后，超额消息被拒（回 error），不进广播。
func TestMessageRateLimit(t *testing.T) {
	gw := NewGateway("test-node", ":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour), bus.NewMemory())
	gw.SetMsgRateLimit(1, 2) // 每秒 1 条，突发 2 条

	conn := startClient(t, gw)
	login(t, conn, "alice", "secret")

	// 突发 2 条：令牌够，正常广播并回显。
	for i := 0; i < 2; i++ {
		writeFrame(t, conn, protocol.Message{Type: protocol.TypeChat, Content: fmt.Sprintf("m%d", i)})
		if echo := readFrame(t, conn); echo.Type != protocol.TypeBroadcast || echo.Content != fmt.Sprintf("m%d", i) {
			t.Fatalf("第 %d 条应广播回显，实际 %+v", i, echo)
		}
	}

	// 第 3 条：令牌耗尽（1 条/秒，测试在亚秒内跑完，不会回补），应收到限流 error，而非广播。
	writeFrame(t, conn, protocol.Message{Type: protocol.TypeChat, Content: "m2"})
	resp := readFrame(t, conn)
	if resp.Type != protocol.TypeError || resp.Error != "发送太频繁，请稍后再试" {
		t.Fatalf("第 3 条应被限流，实际 %+v", resp)
	}
}

// TestHeartbeatNotRateLimited 验证心跳豁免限流：极低限流下心跳仍回 pong，
// 否则保活被误拒会导致客户端被误判断线。
func TestHeartbeatNotRateLimited(t *testing.T) {
	gw := NewGateway("test-node", ":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour), bus.NewMemory())
	gw.SetMsgRateLimit(1, 1) // 极低限流：非心跳消息几乎全被拒

	conn := startClient(t, gw)
	login(t, conn, "alice", "secret")

	for i := 0; i < 3; i++ {
		writeFrame(t, conn, protocol.Message{Type: protocol.TypeHeartbeat})
		if pong := readFrame(t, conn); pong.Type != protocol.TypePong {
			t.Fatalf("第 %d 个心跳应回 pong，实际 %+v", i, pong)
		}
	}
}
