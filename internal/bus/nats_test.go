package bus

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"communicate/internal/protocol"
)

// natsTestURL 返回集成测试用的 NATS 地址，可用 TEST_NATS_URL 覆盖。
func natsTestURL() string {
	if url := os.Getenv("TEST_NATS_URL"); url != "" {
		return url
	}
	return nats.DefaultURL // nats://127.0.0.1:4222
}

// newTestNATS 连接 NATS；NATS 不可用时 skip（单元测试不依赖真实中间件）。
func newTestNATS(t *testing.T) *NATS {
	t.Helper()
	n, err := NewNATS(natsTestURL())
	if err != nil {
		t.Skipf("NATS 不可用，跳过集成测试: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// TestNATSRoundTrip 验证发布→订阅往返。用 channel 同步异步投递，避免轮询竞态
// （NATS 的 handler 在 dispatch 协程里异步执行，不能像内存实现那样同步断言）。
func TestNATSRoundTrip(t *testing.T) {
	b := newTestNATS(t)

	got := make(chan protocol.Message, 1)
	sub, err := b.Subscribe("chat.test.roundtrip", func(m protocol.Message) {
		got <- m
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	msg := protocol.Message{Type: protocol.TypeChat, From: "alice", Content: "hi"}
	if err := b.Publish("chat.test.roundtrip", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case m := <-got:
		if m.From != "alice" || m.Content != "hi" {
			t.Fatalf("收到 %+v，期望 alice/hi", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("等待 NATS 消息超时")
	}
}

// TestNATSFanout 验证同一主题多个订阅者各收到一份（扇出）。
func TestNATSFanout(t *testing.T) {
	b := newTestNATS(t)

	got := make(chan protocol.Message, 2)
	for i := 0; i < 2; i++ {
		if _, err := b.Subscribe("chat.test.fanout", func(m protocol.Message) {
			got <- m
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	if err := b.Publish("chat.test.fanout", protocol.Message{Content: "x"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			t.Fatalf("第 %d 个订阅者未收到消息", i+1)
		}
	}
}

// TestNATSUnsubscribe 验证取消订阅后不再收到，且重复取消幂等。
func TestNATSUnsubscribe(t *testing.T) {
	b := newTestNATS(t)

	got := make(chan protocol.Message, 4)
	sub, err := b.Subscribe("chat.test.unsub", func(m protocol.Message) {
		got <- m
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Publish("chat.test.unsub", protocol.Message{Content: "1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("未收到第一条")
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	// 幂等：重复取消不报错（NATS 原生会返回 ErrBadSubscription，实现里已吞掉）。
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("重复 Unsubscribe 应幂等，实际 %v", err)
	}

	if err := b.Publish("chat.test.unsub", protocol.Message{Content: "2"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case m := <-got:
		t.Fatalf("取消订阅后仍收到 %+v", m)
	case <-time.After(500 * time.Millisecond):
		// 期望超时：不再收到（本机 NATS 投递微秒级，500ms 静默足以证明已退订）
	}
}
