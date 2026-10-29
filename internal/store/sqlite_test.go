package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// newTestStore 在临时目录建一个 SQLite store，测试结束自动清理。
func newTestStore(t *testing.T) *SQLite {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestCreateAndGetUser(t *testing.T) {
	st := newTestStore(t)

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

func TestListMessagesOrderAndLimit(t *testing.T) {
	st := newTestStore(t)

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

func TestListMessagesEmpty(t *testing.T) {
	st := newTestStore(t)

	msgs, err := st.ListMessages(0)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("len = %d, want 0", len(msgs))
	}
}
