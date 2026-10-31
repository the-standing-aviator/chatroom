package store

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// pgTestDSN 返回集成测试用的 PostgreSQL DSN。
// 默认指向本地 docker-compose 起的 chat_test 库（独立于开发库 chat），
// 可通过环境变量 TEST_PG_DSN 覆盖。
func pgTestDSN() string {
	if dsn := os.Getenv("TEST_PG_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://chat:chat@localhost:5432/chat_test?sslmode=disable"
}

// newTestPostgres 连接测试库并清空表；PostgreSQL 不可用时 skip，
// 保证没有 PG 环境的 `go test ./...` 也能过。
func newTestPostgres(t *testing.T) *Postgres {
	t.Helper()
	st, err := OpenPostgres(pgTestDSN())
	if err != nil {
		t.Skipf("PostgreSQL 不可用，跳过集成测试: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	// 清空两张表，保证各测试互相隔离。
	for _, q := range []string{`DELETE FROM messages`, `DELETE FROM users`} {
		if _, err := st.db.Exec(q); err != nil {
			t.Fatalf("clean %q: %v", q, err)
		}
	}
	return st
}

func TestPostgresCreateAndGetUser(t *testing.T) {
	st := newTestPostgres(t)

	if err := st.CreateUser("alice", "hash1"); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	u, err := st.GetUser("alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Name != "alice" || u.PasswordHash != "hash1" || u.ID == 0 {
		t.Fatalf("unexpected user: %+v", u)
	}
	// 不存在的用户返回 ErrNotFound
	if _, err := st.GetUser("nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetUser(nobody) err = %v, want ErrNotFound", err)
	}
	// 重名返回 ErrConflict
	if err := st.CreateUser("alice", "hash2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("CreateUser(alice again) err = %v, want ErrConflict", err)
	}
}

func TestPostgresListMessagesOrderAndLimit(t *testing.T) {
	st := newTestPostgres(t)

	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		m := Message{
			Type:    "chat",
			From:    "alice",
			Content: fmt.Sprintf("msg-%d", i),
			TS:      base.Add(time.Duration(i) * time.Minute),
		}
		if err := st.SaveMessage(m); err != nil {
			t.Fatalf("SaveMessage %d: %v", i, err)
		}
	}

	// limit=3 应返回最近 3 条，且按时间升序（msg-2 → msg-4）
	msgs, err := st.ListMessages(3)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("len = %d, want 3", len(msgs))
	}
	for i, want := range []string{"msg-2", "msg-3", "msg-4"} {
		if msgs[i].Content != want {
			t.Fatalf("msgs[%d].Content = %q, want %q", i, msgs[i].Content, want)
		}
	}

	// limit 超过总数时返回全部
	all, err := st.ListMessages(10)
	if err != nil {
		t.Fatalf("ListMessages(10): %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("ListMessages(10) len = %d, want 5", len(all))
	}
}

func TestPostgresListMessagesEmpty(t *testing.T) {
	st := newTestPostgres(t)

	msgs, err := st.ListMessages(0)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("len = %d, want 0", len(msgs))
	}
}
