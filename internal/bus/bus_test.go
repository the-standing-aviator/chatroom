package bus

import (
	"sync"
	"testing"

	"communicate/internal/protocol"
)

// testBus 用同一套用例跑接口契约，作用于任意 Bus 实现（内存/NATS）。
// 用互斥锁保护收集切片：内存实现同步投递无竞态，但 NATS 实现会在自己的
// goroutine 里并发回调 handler，加锁让这套用例两种实现都能复用。
func testBus(t *testing.T, b Bus) {
	t.Helper()

	var (
		mu  sync.Mutex
		got []protocol.Message
	)
	collect := func(dst *[]protocol.Message) func(protocol.Message) {
		return func(m protocol.Message) {
			mu.Lock()
			*dst = append(*dst, m)
			mu.Unlock()
		}
	}

	// Publish → Subscribe 往返：订阅者收到消息。
	sub, err := b.Subscribe("chat.broadcast", collect(&got))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	msg := protocol.Message{Type: protocol.TypeChat, From: "alice", Content: "hi"}
	if err := b.Publish("chat.broadcast", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	mu.Lock()
	gotLen, gotContent := len(got), ""
	if gotLen > 0 {
		gotContent = got[0].Content
	}
	mu.Unlock()
	if gotLen != 1 || gotContent != "hi" {
		t.Fatalf("收到 %d 条（首条 %q），期望 1 条 hi", gotLen, gotContent)
	}

	// 扇出：同一主题多个订阅者各收到一份。
	var got2 []protocol.Message
	if _, err := b.Subscribe("chat.broadcast", collect(&got2)); err != nil {
		t.Fatalf("Subscribe2: %v", err)
	}
	_ = b.Publish("chat.broadcast", protocol.Message{Content: "again"})
	mu.Lock()
	gotLen, got2Len := len(got), len(got2)
	mu.Unlock()
	if gotLen != 2 || got2Len != 1 {
		t.Fatalf("扇出后 got=%d got2=%d，期望 2/1", gotLen, got2Len)
	}

	// 主题隔离：不相关主题不受影响。
	var other []protocol.Message
	if _, err := b.Subscribe("other", collect(&other)); err != nil {
		t.Fatalf("Subscribe other: %v", err)
	}
	_ = b.Publish("chat.broadcast", protocol.Message{Content: "third"})
	mu.Lock()
	otherLen := len(other)
	mu.Unlock()
	if otherLen != 0 {
		t.Fatalf("other 收到 %d 条，期望 0（主题隔离）", otherLen)
	}

	// 取消订阅后不再收到。
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	_ = b.Publish("chat.broadcast", protocol.Message{Content: "fourth"})
	mu.Lock()
	gotLen = len(got)
	mu.Unlock()
	if gotLen != 3 {
		t.Fatalf("取消订阅后 got=%d，期望仍为 3（不再增长）", gotLen)
	}

	// 幂等取消：重复 Unsubscribe 不报错。
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("重复 Unsubscribe 应幂等，实际 %v", err)
	}

	// 无人订阅的主题：Publish 不 panic、不报错。
	if err := b.Publish("empty", protocol.Message{Content: "x"}); err != nil {
		t.Fatalf("无人订阅的 Publish 应无错，实际 %v", err)
	}
}

func TestMemory(t *testing.T) {
	testBus(t, NewMemory())
}
