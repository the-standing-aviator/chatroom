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

func TestListMessagesEmpty(t *testing.T) {
	st := newTestStore(t)

	msgs, err := st.ListMessages("alice", 0)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("len = %d, want 0", len(msgs))
	}
}

// TestMessageFileFieldsRoundTrip 验证文件消息三个字段（file_id/file_name/file_size）
// 能正确落盘并读回：普通消息这三列留零值，文件消息保留原始值。
func TestMessageFileFieldsRoundTrip(t *testing.T) {
	st := newTestStore(t)

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

	// 第一条是普通消息，文件字段应为零值
	if msgs[0].FileID != "" || msgs[0].FileName != "" || msgs[0].FileSize != 0 {
		t.Fatalf("普通消息文件字段应为零值，实际 %+v", msgs[0])
	}
	// 第二条是文件消息，字段应完整保留
	if msgs[1].Type != "file" || msgs[1].FileID != fileMsg.FileID ||
		msgs[1].FileName != fileMsg.FileName || msgs[1].FileSize != fileMsg.FileSize {
		t.Fatalf("文件消息字段 = %+v，期望 %+v", msgs[1], fileMsg)
	}
}

// TestSQLitePrivateMessageVisibility 验证私聊不泄漏给第三方（SQLite 实现）。
func TestSQLitePrivateMessageVisibility(t *testing.T) {
	assertPrivateMessagesHidden(t, newTestStore(t))
}

// TestSQLiteLimitAfterVisibilityFilter 验证 limit 在可见性过滤之后生效（SQLite）。
func TestSQLiteLimitAfterVisibilityFilter(t *testing.T) {
	assertLimitAppliesAfterVisibilityFilter(t, newTestStore(t))
}
