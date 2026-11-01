package server

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/presence"
	"communicate/internal/protocol"
	"communicate/internal/store"
)

// ---- 测试用的内存 store ----

// fakeStore 是内存版 store.Store，用于 server 测试。
// 为什么不用真实 SQLite：server 层要测的是并发/路由/鉴权逻辑，不关心底层数据库；
// 内存版隔离掉 SQLite 依赖，且可以预置用户、断言落盘消息，让测试更快更可控。
type fakeStore struct {
	mu    sync.Mutex
	users map[string]store.User
	msgs  []store.Message
}

func newFakeStore() *fakeStore {
	return &fakeStore{users: make(map[string]store.User)}
}

func (f *fakeStore) CreateUser(name, hash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.users[name]; ok {
		return store.ErrConflict
	}
	f.users[name] = store.User{Name: name, PasswordHash: hash}
	return nil
}

func (f *fakeStore) GetUser(name string) (store.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.users[name]
	if !ok {
		return store.User{}, store.ErrNotFound
	}
	return u, nil
}

func (f *fakeStore) SaveMessage(m store.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, m)
	return nil
}

func (f *fakeStore) ListMessages(limit int) ([]store.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 || limit > len(f.msgs) {
		limit = len(f.msgs)
	}
	return append([]store.Message(nil), f.msgs[len(f.msgs)-limit:]...), nil
}

func (f *fakeStore) Close() error { return nil }

// ---- net.Pipe 端到端辅助 ----

// startClient 用 net.Pipe 建立一对内存连接：服务端跑 handle，返回客户端连接。
// net.Pipe 是纯内存、同步的（Write 阻塞到对端 Read），适合做无真实端口的端到端测试。
func startClient(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	go srv.handle(serverConn)
	t.Cleanup(func() { _ = clientConn.Close() })
	return clientConn
}

// writeFrame 并发写一帧。net.Pipe 同步，写会阻塞到对端读，所以放 goroutine 加超时，避免死锁卡死测试。
func writeFrame(t *testing.T, conn net.Conn, m protocol.Message) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- protocol.WriteFrame(conn, m) }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("WriteFrame 超时：服务端没有在读？")
	}
}

// readFrame 并发读一帧，同样加超时。
func readFrame(t *testing.T, conn net.Conn) protocol.Message {
	t.Helper()
	type result struct {
		m   protocol.Message
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := protocol.ReadFrame(conn)
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("ReadFrame: %v", r.err)
		}
		return r.m
	case <-time.After(3 * time.Second):
		t.Fatal("ReadFrame 超时：服务端没有在写？")
		return protocol.Message{}
	}
}

// recv 从消息通道读一条，加超时保护。
func recv(t *testing.T, ch <-chan protocol.Message) protocol.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("等待消息超时")
		return protocol.Message{}
	}
}

// seedUser 预置一个已注册用户。用 bcrypt.MinCost 生成 hash 加速测试，
// 因为 CompareHashAndPassword 会从 hash 里读回 cost，与生成时的 cost 无关。
func seedUser(t *testing.T, st store.Store, name, password string) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if err := st.CreateUser(name, string(hash)); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
}

// ---- 测试 ----

// TestHandleRegisterAndJoin 验证端到端：新用户 auth 注册 → 加入在线表 → 广播上线（FR-5）。
func TestHandleRegisterAndJoin(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour))
	conn := startClient(t, srv)

	// 客户端发 auth 帧（新用户，触发注册）
	writeFrame(t, conn, protocol.Message{Type: protocol.TypeAuth, Name: "alice", Password: "secret"})

	// 第一条响应：注册成功
	first := readFrame(t, conn)
	if first.Type != protocol.TypeSystem || first.Content != "注册成功，欢迎 alice" {
		t.Fatalf("第一条响应 = %+v，期望「注册成功」system", first)
	}
	// 注册成功应同时签发 JWT，且能校验回原昵称
	if first.Token == "" {
		t.Fatalf("注册成功应返回 JWT token，实际为空")
	}
	if name, err := srv.auth.Verify(first.Token); err != nil || name != "alice" {
		t.Fatalf("token 校验 = %q, %v; want (alice, nil)", name, err)
	}

	// 第二条响应：上线广播
	second := readFrame(t, conn)
	if second.Type != protocol.TypeSystem || second.Content != "alice 已上线" {
		t.Fatalf("第二条响应 = %+v，期望「已上线」system", second)
	}

	// 在线表应包含 alice
	if !contains(srv.who(), "alice") {
		t.Fatalf("在线表 = %v，期望包含 alice", srv.who())
	}
}

// TestHandleLoginExistingUser 验证已有用户用正确密码能登录并上线（FR-5 的另一半）。
func TestHandleLoginExistingUser(t *testing.T) {
	st := newFakeStore()
	seedUser(t, st, "alice", "secret")
	srv := NewServer(":0", zap.NewNop(), st, presence.NewMemory(), auth.New("test-secret", time.Hour))

	conn := startClient(t, srv)
	writeFrame(t, conn, protocol.Message{Type: protocol.TypeAuth, Name: "alice", Password: "secret"})

	first := readFrame(t, conn)
	if first.Type != protocol.TypeSystem || first.Content != "登录成功，欢迎 alice" {
		t.Fatalf("第一条响应 = %+v，期望「登录成功」system", first)
	}
	// 登录成功应签发 JWT，且能校验回原昵称
	if first.Token == "" {
		t.Fatalf("登录成功应返回 JWT token，实际为空")
	}
	if name, err := srv.auth.Verify(first.Token); err != nil || name != "alice" {
		t.Fatalf("token 校验 = %q, %v; want (alice, nil)", name, err)
	}
	second := readFrame(t, conn)
	if second.Type != protocol.TypeSystem || second.Content != "alice 已上线" {
		t.Fatalf("第二条响应 = %+v，期望「已上线」system", second)
	}
	if !contains(srv.who(), "alice") {
		t.Fatalf("在线表 = %v，期望包含 alice", srv.who())
	}
}

// TestHandleLoginWrongPassword 验证密码错误返回 error 且不加入在线表。
func TestHandleLoginWrongPassword(t *testing.T) {
	st := newFakeStore()
	seedUser(t, st, "alice", "secret")
	srv := NewServer(":0", zap.NewNop(), st, presence.NewMemory(), auth.New("test-secret", time.Hour))

	conn := startClient(t, srv)
	writeFrame(t, conn, protocol.Message{Type: protocol.TypeAuth, Name: "alice", Password: "wrong"})

	resp := readFrame(t, conn)
	if resp.Type != protocol.TypeError || resp.Error != "密码错误" {
		t.Fatalf("响应 = %+v，期望「密码错误」error", resp)
	}
	if len(srv.who()) != 0 {
		t.Fatalf("在线表 = %v，期望为空", srv.who())
	}
}

// TestRouteRejectsBadInput 验证：非法输入返回 error 消息而非 panic（对应 P1-1 的修复）。
func TestRouteRejectsBadInput(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour))
	s := &Session{server: srv, sendCh: make(chan protocol.Message, 64)}

	cases := []struct {
		name string
		in   protocol.Message
		want string
	}{
		{name: "chat空内容", in: protocol.Message{Type: protocol.TypeChat}, want: "消息内容不能为空"},
		{name: "private缺对象", in: protocol.Message{Type: protocol.TypePrivate, Content: "hi"}, want: "私聊需要指定对象和内容"},
		{name: "private缺内容", in: protocol.Message{Type: protocol.TypePrivate, To: "bob"}, want: "私聊需要指定对象和内容"},
		{name: "rename空昵称", in: protocol.Message{Type: protocol.TypeRename}, want: "昵称不能为空"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv.route(s, tc.in)
			resp := recv(t, s.sendCh)
			if resp.Type != protocol.TypeError || resp.Error != tc.want {
				t.Fatalf("响应 = %+v，期望 error %q", resp, tc.want)
			}
		})
	}
}

// TestSessionNameConcurrentAccess 验证：并发改名（写）与读名字（广播）之间无数据竞态（P1-3）。
// 该测试本身不断言具体值（并发写谁赢不确定），核心价值是 go test -race 下不报竞态：
// 若 name 没有用 nameMu 保护，race 检测器会在这里报错。
func TestSessionNameConcurrentAccess(t *testing.T) {
	s := &Session{}

	const (
		writers = 8
		readers = 8
		iters   = 1000
	)

	var wg sync.WaitGroup

	// 写协程：反复改名（模拟 rename）
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				s.setName(fmt.Sprintf("user-%d-%d", id, j))
			}
		}(i)
	}

	// 读协程：反复读名字（模拟广播）
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = s.Name()
			}
		}()
	}

	wg.Wait()
}

// login 完成一次登录/注册，并读掉自己的两条响应（成功提示 + 上线广播）。
// 具体响应内容已在 TestHandleRegisterAndJoin / TestHandleLoginExistingUser 中验证，这里不重复断言。
func login(t *testing.T, conn net.Conn, name, password string) {
	t.Helper()
	writeFrame(t, conn, protocol.Message{Type: protocol.TypeAuth, Name: name, Password: password})
	readFrame(t, conn) // 注册成功 / 登录成功
	readFrame(t, conn) // xx 已上线
}

// TestPrivateMessage 验证端到端私聊：alice 私聊 bob，bob 收到（FR-2）。
func TestPrivateMessage(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour))

	connA := startClient(t, srv)
	login(t, connA, "alice", "secret")

	connB := startClient(t, srv)
	login(t, connB, "bob", "secret")

	// alice 会收到 "bob 已上线" 广播，先读掉
	m := readFrame(t, connA)
	if m.Type != protocol.TypeSystem || m.Content != "bob 已上线" {
		t.Fatalf("alice 应收到 bob 上线广播，实际 %+v", m)
	}

	// alice 私聊 bob
	writeFrame(t, connA, protocol.Message{Type: protocol.TypePrivate, To: "bob", Content: "hello bob"})

	// bob 收到私聊
	resp := readFrame(t, connB)
	if resp.Type != protocol.TypePrivate || resp.From != "alice" || resp.Content != "hello bob" {
		t.Fatalf("bob 收到的私聊 = %+v", resp)
	}
}

// TestRename 验证改名成功与重名冲突（FR-3）。
func TestRename(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour))

	connA := startClient(t, srv)
	login(t, connA, "alice", "secret")

	connB := startClient(t, srv)
	login(t, connB, "bob", "secret")

	// 读掉 alice 收到的 bob 上线广播
	readFrame(t, connA)

	// alice 改名为 carol
	writeFrame(t, connA, protocol.Message{Type: protocol.TypeRename, NewName: "carol"})

	m1 := readFrame(t, connA)
	if m1.Type != protocol.TypeSystem || m1.Content != "已更新昵称为 carol" {
		t.Fatalf("改名响应 = %+v", m1)
	}
	m2 := readFrame(t, connA)
	if m2.Type != protocol.TypeSystem || m2.Content != "alice 改名为 carol" {
		t.Fatalf("改名广播 = %+v", m2)
	}

	// 在线表：carol 在，alice 已移除
	if !contains(srv.who(), "carol") || contains(srv.who(), "alice") {
		t.Fatalf("在线表 = %v，期望 carol 在、alice 不在", srv.who())
	}

	// 重名冲突：carol 改名为已在线的 bob
	writeFrame(t, connA, protocol.Message{Type: protocol.TypeRename, NewName: "bob"})
	resp := readFrame(t, connA)
	if resp.Type != protocol.TypeError || resp.Error != "昵称 \"bob\" 已被使用" {
		t.Fatalf("重名响应 = %+v", resp)
	}
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// panicStore 嵌入 fakeStore，仅让 SaveMessage panic，用于验证 handle 的 recover 兜底。
type panicStore struct {
	*fakeStore
}

func (p *panicStore) SaveMessage(m store.Message) error {
	panic("boom")
}

// TestHandleRecoverFromPanic 验证 saveLoop 的 recover 兜底：落盘 panic 不崩整个进程。
// 异步持久化后 SaveMessage 在 saveLoop goroutine 里执行（不再走 handle），
// 进程级 goroutine 的 panic 若未恢复会崩掉整个进程，所以 saveOne 必须有 recover。
func TestHandleRecoverFromPanic(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), &panicStore{fakeStore: newFakeStore()}, presence.NewMemory(), auth.New("test-secret", time.Hour))
	conn := startClient(t, srv)
	login(t, conn, "alice", "secret")

	// 发公聊触发 panic：broadcast 投递到 saveCh，saveLoop 消费时 SaveMessage panic（见 panicStore）。
	// 若 saveOne 没有 recover 兜底，进程级 goroutine 的 panic 会崩掉整个测试进程。
	// 能执行到下面，说明 panic 已被 recover 捕获。
	writeFrame(t, conn, protocol.Message{Type: protocol.TypeChat, Content: "boom"})

	// 验证服务器仍能服务新连接：落盘 panic 是单条消息级，不应影响进程级状态。
	conn2 := startClient(t, srv)
	login(t, conn2, "bob", "secret")
	if !contains(srv.who(), "bob") {
		t.Fatalf("panic 后在线表 = %v，期望 bob 能正常上线", srv.who())
	}
}

// TestSlowClientDoesNotBlockBroadcast 验证背压隔离：一个不读的慢客户端不拖垮广播。
// 慢客户端的 writeLoop 阻塞在写（net.Pipe 对端不读），sendCh 被填满；
// broadcast 的 send 是非阻塞投递，满了 dropped 计数，快客户端仍正常收到全部广播。
func TestSlowClientDoesNotBlockBroadcast(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour))

	// 慢客户端：登录后不再读，让它的 writeLoop 阻塞、sendCh 填满。
	slowConn := startClient(t, srv)
	login(t, slowConn, "slow", "secret")

	// 快客户端：正常读。上线时 slow 也会收到 "fast 已上线"，但 slow 不读，
	// 它的 writeLoop 因此阻塞在写这一条上（模拟慢消费者）。
	fastConn := startClient(t, srv)
	login(t, fastConn, "fast", "secret")

	// fast 连发 100 条公聊，每条都要能立即收到自己的广播回显（不被 slow 拖慢）。
	for i := 0; i < 100; i++ {
		content := fmt.Sprintf("msg-%d", i)
		writeFrame(t, fastConn, protocol.Message{Type: protocol.TypeChat, Content: content})

		got := readFrame(t, fastConn)
		if got.Type != protocol.TypeBroadcast || got.Content != content {
			t.Fatalf("第 %d 条广播 = %+v，期望收到 %q", i, got, content)
		}
	}

	// 慢客户端 sendCh(64) 被填满，多余消息被 dropped 计数——背压生效，且广播未被拖慢。
	if dropped := srv.dropped.Load(); dropped == 0 {
		t.Fatalf("期望慢客户端的消息被 dropped 计数，实际 0")
	}
}

// TestSendSystemSeparateChannel 验证系统消息走独立通道、满则静默丢弃，
// 不挤占业务消息的 sendCh（P2-3 的核心机制）。
func TestSendSystemSeparateChannel(t *testing.T) {
	srv := NewServer(":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), auth.New("test-secret", time.Hour))

	// net.Pipe：对端不读，writeLoop 会阻塞在写第一条系统消息上（同步内存连接）。
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	s := newSession(serverConn, srv)
	defer s.close()

	// 灌大量系统消息，远超 sysCh 缓冲(8)。满则静默丢，不计数。
	for i := 0; i < 100; i++ {
		s.sendSystem(protocol.Message{Type: protocol.TypeSystem, Content: "x"})
	}

	// 关键断言：业务队列 sendCh 必须为空——系统消息没有挤占 chat 的队列。
	if got := len(s.sendCh); got != 0 {
		t.Fatalf("sendCh 长度 = %d，期望 0（系统消息不应占用业务队列）", got)
	}
	// 系统队列有积压（writeLoop 拿走一条去写后阻塞，其余留在 sysCh）。
	if got := len(s.sysCh); got == 0 {
		t.Fatalf("sysCh 长度 = 0，期望系统消息进入独立通道")
	}
	// 系统消息满则静默丢，不进 dropped 计数（那是慢客户端丢业务消息的信号）。
	if got := srv.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d，期望 0（系统消息满则静默丢，不计数）", got)
	}
}
