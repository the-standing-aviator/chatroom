package presence

import (
	"context"
	"errors"
	"os"
	"sort"
	"testing"
)

// testPresence 用同一套用例跑接口契约，作用于任意 Presence 实现。
func testPresence(t *testing.T, p Presence) {
	t.Helper()

	// Join + Node
	if err := p.Join("alice", "node-1"); err != nil {
		t.Fatalf("Join alice: %v", err)
	}
	if node, err := p.Node("alice"); err != nil || node != "node-1" {
		t.Fatalf("Node(alice) = %q, %v; want (node-1, nil)", node, err)
	}
	// 重名冲突
	if err := p.Join("alice", "node-2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("Join(alice again) = %v, want ErrConflict", err)
	}
	// 不在线
	if _, err := p.Node("nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Node(nobody) = _, %v; want ErrNotFound", err)
	}

	// Online
	if err := p.Join("bob", "node-2"); err != nil {
		t.Fatalf("Join bob: %v", err)
	}
	names, err := p.Online()
	if err != nil {
		t.Fatalf("Online: %v", err)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("Online() = %v, want [alice bob]", names)
	}

	// Rename：正常搬移
	if err := p.Rename("alice", "carol"); err != nil {
		t.Fatalf("Rename alice->carol: %v", err)
	}
	if _, err := p.Node("alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Node(alice) after rename = _, %v; want ErrNotFound", err)
	}
	if node, err := p.Node("carol"); err != nil || node != "node-1" {
		t.Fatalf("Node(carol) = %q, %v; want (node-1, nil)", node, err)
	}
	// Rename：新名已占用
	if err := p.Rename("bob", "carol"); !errors.Is(err, ErrConflict) {
		t.Fatalf("Rename bob->carol = %v, want ErrConflict", err)
	}
	// Rename：旧名不在线
	if err := p.Rename("nobody", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Rename nobody = %v, want ErrNotFound", err)
	}

	// Leave：幂等
	if err := p.Leave("carol"); err != nil {
		t.Fatalf("Leave carol: %v", err)
	}
	if err := p.Leave("carol"); err != nil {
		t.Fatalf("Leave carol again: %v", err)
	}
	if _, err := p.Node("carol"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Node(carol) after leave = _, %v; want ErrNotFound", err)
	}
}

func TestMemory(t *testing.T) {
	testPresence(t, NewMemory())
}

func TestRedis(t *testing.T) {
	testPresence(t, newTestRedis(t))
}

// redisTestAddr 返回集成测试用的 Redis 地址，可用 TEST_REDIS_ADDR 覆盖。
func redisTestAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newTestRedis 连接 Redis 并清空 presence 键；Redis 不可用时 skip。
func newTestRedis(t *testing.T) *Redis {
	t.Helper()
	r := NewRedis(redisTestAddr())
	if err := r.rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis 不可用，跳过集成测试: %v", err)
	}
	t.Cleanup(func() {
		_ = r.rdb.Del(context.Background(), presenceKey).Err()
		_ = r.Close()
	})
	if err := r.rdb.Del(context.Background(), presenceKey).Err(); err != nil {
		t.Fatalf("clean presence: %v", err)
	}
	return r
}
