package server

import (
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/protocol"
)

// metricValue 从 registry 抓取指定指标名的最新样本值（gauge / counter 通用）。
// 直接读 Gather 的结果，比解析 /metrics 文本输出更精确、不易碎。
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.Metric) == 0 {
			t.Fatalf("指标 %s 无样本", name)
		}
		m := mf.Metric[0]
		if g := m.GetGauge(); g != nil {
			return g.GetValue()
		}
		if c := m.GetCounter(); c != nil {
			return c.GetValue()
		}
		t.Fatalf("指标 %s 既非 gauge 也非 counter", name)
	}
	t.Fatalf("指标 %s 未注册", name)
	return 0
}

// TestMetricsOnlineCount 验证指标从实时状态读取：登录后 chat_online 变 1，goroutines > 0。
func TestMetricsOnlineCount(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour), bus.NewMemory())
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg, srv)

	if got := metricValue(t, reg, "chat_online"); got != 0 {
		t.Fatalf("初始 online = %v，期望 0", got)
	}

	// 一个客户端登录 → 在线数 +1。
	conn := startClient(t, srv)
	login(t, conn, "alice", "secret")

	if got := metricValue(t, reg, "chat_online"); got != 1 {
		t.Fatalf("登录后 online = %v，期望 1", got)
	}

	// goroutine 指标只是 runtime.NumGoroutine 的透传，>0 即可证明已接线。
	if got := metricValue(t, reg, "chat_goroutines"); got <= 0 {
		t.Fatalf("goroutines = %v，期望 > 0", got)
	}
}

// TestMetricsDroppedCounter 验证 dropped 计数被暴露为指标：慢会话 sendCh 填满后数值增加。
func TestMetricsDroppedCounter(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour), bus.NewMemory())
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg, srv)

	if got := metricValue(t, reg, "chat_dropped_messages_total"); got != 0 {
		t.Fatalf("初始 dropped = %v，期望 0", got)
	}

	// 慢会话：net.Pipe 对端不读，writeLoop 阻塞在写第一条，sendCh 被后续消息填满。
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	s := newSession(serverConn, srv)
	defer s.close()

	// 灌超过 sendCh(64) 的消息：前 65 条（1 条在写 + 64 缓冲）消化后，其余被 dropped 计数。
	for i := 0; i < sendBuffer+10; i++ {
		s.send(protocol.Message{Type: protocol.TypeBroadcast, Content: "x"})
	}

	if got := metricValue(t, reg, "chat_dropped_messages_total"); got == 0 {
		t.Fatalf("dropped = %v，期望 > 0（慢会话队列满应触发丢消息计数）", got)
	}
}
