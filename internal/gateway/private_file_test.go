package gateway

import (
	"net"
	"testing"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/protocol"
)

// newTestGateway 构造一个用内存后端的 Gateway，供本文件的端到端用例使用。
func newTestGateway(t *testing.T) *Gateway {
	t.Helper()
	return NewGateway("test-node", ":0", zap.NewNop(), newFakeStore(),
		presence.NewMemory(), auth.New("test-secret", time.Hour), bus.NewMemory())
}

// TestPrivateFileDeliveredOnlyToTarget 验证私聊文件只投递给目标用户：
// alice 发给 bob 的文件消息，bob 收到，第三方 carol 收不到。
//
// 这是「私聊传文件不给公聊看到」的核心保证——如果走 broadcastFile 分支，
// carol 会收到，测试就会失败。
func TestPrivateFileDeliveredOnlyToTarget(t *testing.T) {
	gw := newTestGateway(t)

	connA := startClient(t, gw)
	login(t, connA, "alice", "secret")

	connB := startClient(t, gw)
	login(t, connB, "bob", "secret")

	connC := startClient(t, gw)
	login(t, connC, "carol", "secret")

	// 读掉各自的上下线系统广播，避免干扰后续断言。
	drainSystem(t, connA, 2) // bob 上线、carol 上线
	drainSystem(t, connB, 1) // carol 上线
	drainSystem(t, connC, 0)

	const (
		fileID   = "0123456789abcdef0123456789abcdef"
		fileName = "salary.pdf"
		fileSize = int64(4096)
	)

	// alice 发私聊文件给 bob
	writeFrame(t, connA, protocol.Message{
		Type: protocol.TypeFile, To: "bob",
		FileID: fileID, FileName: fileName, FileSize: fileSize,
	})

	// bob 必须收到
	got := readFrame(t, connB)
	if got.Type != protocol.TypeFile || got.FileID != fileID || got.From != "alice" || got.To != "bob" {
		t.Fatalf("bob 收到的私聊文件 = %+v", got)
	}
	if got.FileName != fileName || got.FileSize != fileSize {
		t.Fatalf("bob 收到的文件元数据 = %+v", got)
	}

	// carol 不能收到任何文件消息。给她一个短超时：收到东西就说明泄漏了。
	if leaked := tryReadFrame(t, connC, 300*time.Millisecond); leaked != nil {
		t.Fatalf("carol 不该收到私聊文件，实际收到 %+v", leaked)
	}
}

// TestPrivateFilePersistedWithRecipient 验证私聊文件落库时带 To，
// 这样历史接口的可见性过滤才能把它挡在第三方之外。
//
// 注意：这里不用 startClient + 等异步落盘的路子。saveLoop 只在 Start() 里启动，
// 而测试用 net.Pipe 不走 Start，队列没人消费。改为直接调用 privateFile，
// 从 saveCh 里取出入队的那条消息断言——测的正是「入队时 To 有没有带」。
func TestPrivateFilePersistedWithRecipient(t *testing.T) {
	gw := newTestGateway(t)

	// 让 bob 在线，privateFile 才能定位到节点。
	connB := startClient(t, gw)
	login(t, connB, "bob", "secret")
	drainSystem(t, connB, 0)

	const fileID = "fedcba9876543210fedcba9876543210"
	if !gw.privateFile("alice", "bob", fileID, "secret.pdf", 100) {
		t.Fatal("privateFile 返回 false（bob 应在线）")
	}

	select {
	case m := <-gw.saveCh:
		if m.FileID != fileID {
			t.Fatalf("入队的消息 FileID = %q, want %q", m.FileID, fileID)
		}
		if m.To != "bob" {
			t.Fatalf("入队的 To = %q, want bob（不带 To 会被历史接口泄漏给所有人）", m.To)
		}
		if m.From != "alice" {
			t.Fatalf("入队的 From = %q, want alice", m.From)
		}
	case <-time.After(time.Second):
		t.Fatal("私聊文件没有入持久化队列")
	}
}

// TestPrivateFileToSelfRejected 不能给自己发私聊文件。
func TestPrivateFileToSelfRejected(t *testing.T) {
	gw := newTestGateway(t)
	connA := startClient(t, gw)
	login(t, connA, "alice", "secret")
	drainSystem(t, connA, 0)

	writeFrame(t, connA, protocol.Message{
		Type: protocol.TypeFile, To: "alice",
		FileID: "0123456789abcdef0123456789abcdef", FileName: "x.txt", FileSize: 1,
	})
	resp := readFrame(t, connA)
	if resp.Type != protocol.TypeError {
		t.Fatalf("给自己发私聊文件应报错，实际 %+v", resp)
	}
}

// TestPrivateFileToOfflineUser 目标不在线时返回明确错误，而不是静默丢弃。
func TestPrivateFileToOfflineUser(t *testing.T) {
	gw := newTestGateway(t)
	connA := startClient(t, gw)
	login(t, connA, "alice", "secret")
	drainSystem(t, connA, 0)

	writeFrame(t, connA, protocol.Message{
		Type: protocol.TypeFile, To: "nobody",
		FileID: "0123456789abcdef0123456789abcdef", FileName: "x.txt", FileSize: 1,
	})
	resp := readFrame(t, connA)
	if resp.Type != protocol.TypeError {
		t.Fatalf("发给不在线用户应报错，实际 %+v", resp)
	}
}

// TestPublicFileStillBroadcasts 回归保护：不带 To 的文件消息仍走公聊广播，
// 在线所有人都能收到（不能因为加私聊分支把公聊文件弄坏）。
func TestPublicFileStillBroadcasts(t *testing.T) {
	gw := newTestGateway(t)

	connA := startClient(t, gw)
	login(t, connA, "alice", "secret")
	connB := startClient(t, gw)
	login(t, connB, "bob", "secret")
	connC := startClient(t, gw)
	login(t, connC, "carol", "secret")
	drainSystem(t, connA, 2)
	drainSystem(t, connB, 1)
	drainSystem(t, connC, 0)

	const fileID = "aaaabbbbccccddddeeeeffff00001111"
	writeFrame(t, connA, protocol.Message{
		Type: protocol.TypeFile, FileID: fileID, FileName: "public.pdf", FileSize: 10,
	})

	// bob 和 carol 都应收到公聊文件
	if got := readFrame(t, connB); got.Type != protocol.TypeFile || got.FileID != fileID {
		t.Fatalf("bob 没收到公聊文件: %+v", got)
	}
	if got := readFrame(t, connC); got.Type != protocol.TypeFile || got.FileID != fileID {
		t.Fatalf("carol 没收到公聊文件: %+v", got)
	}
}

// drainSystem 读掉 conn 上积压的 n 条系统广播（上下线提示），避免干扰后续断言。
// 用短超时：没有可读的就直接返回，不阻塞。
func drainSystem(t *testing.T, conn net.Conn, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_ = tryReadFrame(t, conn, 300*time.Millisecond)
	}
}

// tryReadFrame 在 timeout 内尝试读一帧：读到就返回，超时返回 nil。
// 用于断言「不应该收到任何消息」的场景（比如第三方不该收到私聊文件）。
func tryReadFrame(t *testing.T, conn net.Conn, timeout time.Duration) *protocol.Message {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	m, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil // 超时或关闭，都当作「没收到」
	}
	return &m
}
