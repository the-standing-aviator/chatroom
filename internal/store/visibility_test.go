package store

import (
	"testing"
	"time"
)

// assertPrivateMessagesHidden 验证私聊可见性：公聊人人可见，私聊只有收发双方可见。
//
// SQLite 与 PostgreSQL 两个实现共用这一套断言，保证两种后端行为一致
// （存储层是可替换实现，契约必须相同）。
func assertPrivateMessagesHidden(t *testing.T, st Store) {
	t.Helper()

	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	seed := []Message{
		{Type: "chat", From: "alice", Content: "public-1", TS: base},
		{Type: "private", From: "alice", To: "bob", Content: "secret-alice-bob", TS: base.Add(time.Minute)},
		{Type: "chat", From: "carol", Content: "public-2", TS: base.Add(2 * time.Minute)},
		{Type: "private", From: "bob", To: "carol", Content: "secret-bob-carol", TS: base.Add(3 * time.Minute)},
		{Type: "private", From: "eve", To: "mallory", Content: "secret-eve-mallory", TS: base.Add(4 * time.Minute)},
	}
	for i, m := range seed {
		if err := st.SaveMessage(m); err != nil {
			t.Fatalf("SaveMessage #%d: %v", i, err)
		}
	}

	cases := []struct {
		viewer   string
		wantSeen []string
		wantHide []string
	}{
		{
			viewer:   "alice",
			wantSeen: []string{"public-1", "secret-alice-bob", "public-2"},
			// alice 是发送方，bob->carol 与她的无关
			wantHide: []string{"secret-bob-carol", "secret-eve-mallory"},
		},
		{
			viewer:   "bob",
			wantSeen: []string{"public-1", "secret-alice-bob", "public-2", "secret-bob-carol"},
			wantHide: []string{"secret-eve-mallory"},
		},
		{
			viewer:   "carol",
			wantSeen: []string{"public-1", "public-2", "secret-bob-carol"},
			wantHide: []string{"secret-alice-bob", "secret-eve-mallory"},
		},
		{
			// 完全无关的第三方：只能看到公聊
			viewer:   "eve",
			wantSeen: []string{"public-1", "public-2", "secret-eve-mallory"},
			wantHide: []string{"secret-alice-bob", "secret-bob-carol"},
		},
		{
			// 从未参与过任何对话：只看到公聊
			viewer:   "nobody",
			wantSeen: []string{"public-1", "public-2"},
			wantHide: []string{"secret-alice-bob", "secret-bob-carol", "secret-eve-mallory"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.viewer, func(t *testing.T) {
			msgs, err := st.ListMessages(tc.viewer, 100)
			if err != nil {
				t.Fatalf("ListMessages(%q): %v", tc.viewer, err)
			}
			got := make(map[string]bool, len(msgs))
			for _, m := range msgs {
				got[m.Content] = true
			}

			for _, want := range tc.wantSeen {
				if !got[want] {
					t.Errorf("viewer=%s 应该看到 %q，实际没看到", tc.viewer, want)
				}
			}
			for _, hidden := range tc.wantHide {
				if got[hidden] {
					t.Errorf("viewer=%s 不该看到 %q（私聊泄漏）", tc.viewer, hidden)
				}
			}
		})
	}
}

// assertLimitAppliesAfterVisibilityFilter 验证 limit 是在可见性过滤之后生效的。
//
// 如果实现是「先取 limit 条再过滤」，viewer 拿到的条数会少于 limit；
// 正确实现应该先按可见性筛，再从可见集合里取最近的 limit 条。
func assertLimitAppliesAfterVisibilityFilter(t *testing.T, st Store) {
	t.Helper()

	base := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	// 先塞 10 条 alice 看不到的私聊（bob<->carol）
	for i := 0; i < 10; i++ {
		if err := st.SaveMessage(Message{
			Type: "private", From: "bob", To: "carol",
			Content: "hidden", TS: base.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("SaveMessage hidden #%d: %v", i, err)
		}
	}
	// 再塞 5 条公聊（alice 可见），时间更晚
	for i := 0; i < 5; i++ {
		if err := st.SaveMessage(Message{
			Type: "chat", From: "alice",
			Content: "visible", TS: base.Add(time.Duration(100+i) * time.Second),
		}); err != nil {
			t.Fatalf("SaveMessage visible #%d: %v", i, err)
		}
	}

	// alice 只可见 5 条公聊，limit=5 应该正好全部拿到
	msgs, err := st.ListMessages("alice", 5)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("len = %d, want 5（limit 应在可见性过滤之后生效）", len(msgs))
	}
	for i, m := range msgs {
		if m.Content != "visible" {
			t.Fatalf("msgs[%d] = %q, 不该出现私聊内容", i, m.Content)
		}
	}
}

// assertListMessagesBefore 验证「上滑加载更早」的分页契约：只返回指定会话、
// id < beforeID 的消息，按 id 升序，且私聊不泄漏给无关方。
func assertListMessagesBefore(t *testing.T, st Store) {
	t.Helper()

	save := func(typ, from, to, content string) {
		t.Helper()
		if err := st.SaveMessage(Message{Type: typ, From: from, To: to, Content: content, TS: time.Now()}); err != nil {
			t.Fatalf("SaveMessage(%s): %v", content, err)
		}
	}
	// 保存顺序即 id 顺序：公聊 4 条 + alice↔bob 2 条 + alice↔carol 1 条
	save("chat", "alice", "", "p1")
	save("chat", "alice", "", "p2")
	save("chat", "alice", "", "p3")
	save("chat", "alice", "", "p4")
	save("private", "alice", "bob", "ab1")
	save("private", "bob", "alice", "ab2")
	save("private", "alice", "carol", "ac1")

	// 用 ListMessages 反查 content→id，避免依赖自增序列从 1 开始
	all, err := st.ListMessages("alice", 100)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	idOf := map[string]int64{}
	for _, m := range all {
		idOf[m.Content] = m.ID
	}

	// 公聊分页：beforeID = p4 的 id，应只返回 p1..p3，且升序
	pub, err := st.ListMessagesBefore("alice", "", idOf["p4"], 10)
	if err != nil {
		t.Fatalf("ListMessagesBefore(public): %v", err)
	}
	if got, want := contentsOf(pub), []string{"p1", "p2", "p3"}; !strSlicesEqual(got, want) {
		t.Fatalf("public before p4 = %v, want %v", got, want)
	}

	// 私聊：alice 拉 bob 会话，只返回 ab1、ab2（升序）
	ab, err := st.ListMessagesBefore("alice", "bob", 1<<30, 10)
	if err != nil {
		t.Fatalf("ListMessagesBefore(alice,bob): %v", err)
	}
	if got, want := contentsOf(ab), []string{"ab1", "ab2"}; !strSlicesEqual(got, want) {
		t.Fatalf("alice↔bob = %v, want %v", got, want)
	}

	// 隐私：carol 拉 bob 会话应一无所获（两者之间没有双向私聊）
	cb, err := st.ListMessagesBefore("carol", "bob", 1<<30, 10)
	if err != nil {
		t.Fatalf("ListMessagesBefore(carol,bob): %v", err)
	}
	if len(cb) != 0 {
		t.Fatalf("carol 拉 bob 会话 = %v, want 空（私聊泄漏）", contentsOf(cb))
	}
}

func contentsOf(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Content
	}
	return out
}

func strSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
