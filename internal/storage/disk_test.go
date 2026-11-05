package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newTestDisk 在临时目录建一个 Disk，测试结束自动清理。
func newTestDisk(t *testing.T) *Disk {
	t.Helper()
	d, err := NewDisk(t.TempDir())
	if err != nil {
		t.Fatalf("NewDisk: %v", err)
	}
	return d
}

// TestSaveOpenRoundTrip 验证最基本的契约：存进去的字节能原样读回来。
func TestSaveOpenRoundTrip(t *testing.T) {
	d := newTestDisk(t)
	want := []byte("hello 世界, 文件存储测试")

	id, size, err := d.Save(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", size, len(want))
	}
	// id 必须是 32 位 hex，且由实现生成（不依赖调用方）。
	if !idPattern.MatchString(id) {
		t.Fatalf("id = %q, 不是 32 位 hex", id)
	}

	rc, err := d.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("读回内容不符: %q != %q", got, want)
	}
}

// TestSaveGeneratesUniqueID 验证连续保存不会撞 id（随机 id 的全局唯一性，
// 这是 V5 多节点不依赖单点发号的前提）。
func TestSaveGeneratesUniqueID(t *testing.T) {
	d := newTestDisk(t)
	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		id, _, err := d.Save(strings.NewReader("x"))
		if err != nil {
			t.Fatalf("Save #%d: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("id 重复: %q", id)
		}
		seen[id] = true
	}
}

// TestSaveEmptyFile 空文件是合法输入（0 字节），不该被当成错误。
func TestSaveEmptyFile(t *testing.T) {
	d := newTestDisk(t)
	id, size, err := d.Save(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if size != 0 {
		t.Fatalf("size = %d, want 0", size)
	}
	rc, err := d.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) != 0 {
		t.Fatalf("空文件读回 %d 字节", len(got))
	}
}

// TestOpenMissing 不存在的（但格式合法的）id 应该报错，不能返回空内容冒充成功。
func TestOpenMissing(t *testing.T) {
	d := newTestDisk(t)
	// 格式合法但从未保存过的 id
	if _, err := d.Open(strings.Repeat("a", 32)); err == nil {
		t.Fatal("Open(不存在的 id) 应该报错")
	}
}

// TestInvalidIDRejected 是所有用例里最重要的一个：id 会被拼进文件路径，
// 必须挡住路径穿越。Open/Delete 都走同一个 path() 校验。
func TestInvalidIDRejected(t *testing.T) {
	d := newTestDisk(t)

	// 在存储目录外放一个「机密文件」，确认路径穿越拿不到它。
	secret := filepath.Join(filepath.Dir(d.dir), "secret.txt")
	if err := os.WriteFile(secret, []byte("TOP SECRET"), 0o600); err != nil {
		t.Fatalf("准备 secret 文件: %v", err)
	}

	bad := []struct {
		name string
		id   string
	}{
		{"空字符串", ""},
		{"上级目录穿越", "../secret.txt"},
		{"多级穿越", "../../etc/passwd"},
		{"绝对路径", "/etc/passwd"},
		{"带斜杠的相对路径", "sub/file"},
		{"反斜杠穿越", `..\secret.txt`},
		{"含空字节", "abc\x00def"},
		{"长度不足", "abc123"},
		{"长度超长", strings.Repeat("a", 33)},
		{"含非 hex 字符", strings.Repeat("g", 32)},
		{"大写 hex", strings.ToUpper(strings.Repeat("a", 32))},
		{"hex 混入路径", strings.Repeat("a", 30) + "/."},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := d.Open(tc.id); err == nil {
				t.Errorf("Open(%q) 应该被拒绝", tc.id)
			}
			if err := d.Delete(tc.id); err == nil {
				t.Errorf("Delete(%q) 应该被拒绝", tc.id)
			}
		})
	}

	// 关键断言：机密文件必须原封不动（证明没有任何一次穿越真的落地）。
	got, err := os.ReadFile(secret)
	if err != nil {
		t.Fatalf("secret 文件被删了: %v", err)
	}
	if string(got) != "TOP SECRET" {
		t.Fatalf("secret 文件内容被改了: %q", got)
	}
}

// errAfter 读到第 n 字节后返回错误，用来模拟「传输中途失败」。
type errAfter struct {
	data []byte
	n    int
	read int
}

var errBoom = errors.New("boom")

func (r *errAfter) Read(p []byte) (int, error) {
	if r.read >= r.n {
		return 0, errBoom
	}
	remain := r.n - r.read
	if remain > len(p) {
		remain = len(p)
	}
	n := copy(p, r.data[r.read:r.read+remain])
	r.read += n
	return n, nil
}

// TestSaveFailureCleansUp 验证写到一半失败时不留残缺文件（脏数据）。
// 这是 Save 里那段 os.Remove 的存在理由。
func TestSaveFailureCleansUp(t *testing.T) {
	d := newTestDisk(t)

	_, _, err := d.Save(&errAfter{data: []byte("0123456789"), n: 5})
	if err == nil {
		t.Fatal("Save 应该返回错误")
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("错误应该包裹底层 errBoom, got %v", err)
	}

	// 存储目录必须干净：没有残留的半截文件。
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("失败后残留了 %d 个文件: %v", len(entries), names)
	}
}

// TestDelete 验证删除后文件真的没了，且删不存在的文件会报错。
func TestDelete(t *testing.T) {
	d := newTestDisk(t)
	id, _, err := d.Save(strings.NewReader("to be deleted"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := d.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := d.Open(id); err == nil {
		t.Fatal("Delete 之后 Open 应该报错")
	}
	// 重复删除：文件已不存在，应报错而不是静默成功。
	if err := d.Delete(id); err == nil {
		t.Fatal("重复 Delete 应该报错")
	}
}

// TestNewDiskCreatesDir 验证目录不存在时会自动创建（容器首次启动的场景）。
func TestNewDiskCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "files")
	d, err := NewDisk(dir)
	if err != nil {
		t.Fatalf("NewDisk: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("目录没被创建: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("创建的不是目录")
	}
	// 建好的 Disk 应该立刻可用。
	if _, _, err := d.Save(strings.NewReader("ok")); err != nil {
		t.Fatalf("新目录 Save 失败: %v", err)
	}
}

// TestOpenReturnsReadCloser 确认 Open 返回的流能被正常 Close 且不报错
// （调用方 defer rc.Close() 依赖这一点）。
func TestOpenReturnsReadCloser(t *testing.T) {
	d := newTestDisk(t)
	id, _, err := d.Save(strings.NewReader("data"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	rc, err := d.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
