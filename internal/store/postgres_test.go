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
	msgs, err := st.ListMessages("alice", 3)
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
	all, err := st.ListMessages("alice", 10)
	if err != nil {
		t.Fatalf("ListMessages(10): %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("ListMessages(10) len = %d, want 5", len(all))
	}
}

func TestPostgresListMessagesEmpty(t *testing.T) {
	st := newTestPostgres(t)

	msgs, err := st.ListMessages("alice", 0)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("len = %d, want 0", len(msgs))
	}
}

// TestPostgresMessageFileFieldsRoundTrip 与 SQLite 版同：验证文件消息三字段落盘读回不丢失。
func TestPostgresMessageFileFieldsRoundTrip(t *testing.T) {
	st := newTestPostgres(t)

	// 普通公聊：文件三列应为零值
	if err := st.SaveMessage(Message{Type: "chat", From: "alice", Content: "hi", TS: time.Now()}); err != nil {
		t.Fatalf("SaveMessage(chat): %v", err)
	}

	// 文件消息：三列带值
	fileMsg := Message{
		Type:     "file",
		From:     "alice",
		FileID:   "0123456789abcdef0123456789abcdef",
		FileName: "report.pdf",
		FileSize: 2048,
		TS:       time.Now(),
	}
	if err := st.SaveMessage(fileMsg); err != nil {
		t.Fatalf("SaveMessage(file): %v", err)
	}

	msgs, err := st.ListMessages("alice", 10)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len = %d, want 2", len(msgs))
	}

	if msgs[0].FileID != "" || msgs[0].FileName != "" || msgs[0].FileSize != 0 {
		t.Fatalf("普通消息文件字段应为零值，实际 %+v", msgs[0])
	}
	if msgs[1].Type != "file" || msgs[1].FileID != fileMsg.FileID ||
		msgs[1].FileName != fileMsg.FileName || msgs[1].FileSize != fileMsg.FileSize {
		t.Fatalf("文件消息字段 = %+v，期望 %+v", msgs[1], fileMsg)
	}
}

// TestPostgresPrivateMessageVisibility 验证私聊不泄漏给第三方（PostgreSQL 实现）。
func TestPostgresPrivateMessageVisibility(t *testing.T) {
	assertPrivateMessagesHidden(t, newTestPostgres(t))
}

// TestPostgresLimitAfterVisibilityFilter 验证 limit 在可见性过滤之后生效（PostgreSQL）。
func TestPostgresLimitAfterVisibilityFilter(t *testing.T) {
	assertLimitAppliesAfterVisibilityFilter(t, newTestPostgres(t))
}

// TestPostgresListMessagesBefore 验证「上滑加载更早」分页（PostgreSQL 实现，无 PG 环境自动 skip）。
func TestPostgresListMessagesBefore(t *testing.T) {
	assertListMessagesBefore(t, newTestPostgres(t))
}
