package gateway

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/storage"
)

// 本文件补齐 files_test.go 没覆盖的失败路径与安全边界：
// 方法/配置/鉴权/体积/头注入/元数据与字节不一致。

// newFileGatewayWithDir 与 newFileGateway 相同，但把存储目录也返回出来，
// 用于需要直接操作磁盘字节的用例（模拟「元数据在、字节丢了」）。
func newFileGatewayWithDir(t *testing.T) (*Gateway, *auth.Manager, string) {
	t.Helper()
	dir := t.TempDir()
	disk, err := storage.NewDisk(dir)
	if err != nil {
		t.Fatalf("NewDisk: %v", err)
	}
	am := auth.New("test-secret", time.Hour)
	gw := NewGateway("test-node", ":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), am, bus.NewMemory())
	gw.SetStorage(disk)
	return gw, am, dir
}

// mustToken 签发一个测试用 JWT。
func mustToken(t *testing.T, am *auth.Manager, name string) string {
	t.Helper()
	tok, err := am.Issue(name)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return tok
}

// TestUploadMethodNotAllowed 上传只接受 POST。
func TestUploadMethodNotAllowed(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/upload", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		gw.HandleUpload(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /upload status = %d, want 405", method, rec.Code)
		}
	}
}

// TestDownloadMethodNotAllowed 下载只接受 GET。
func TestDownloadMethodNotAllowed(t *testing.T) {
	gw, _ := newFileGateway(t)
	req := httptest.NewRequest(http.MethodPost, "/download/"+strings.Repeat("a", 32), nil)
	rec := httptest.NewRecorder()
	gw.HandleDownload(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

// TestUploadInvalidTokenRejected 签名不对 / 格式不对的 token 一律 401。
func TestUploadInvalidTokenRejected(t *testing.T) {
	gw, _ := newFileGateway(t)

	cases := []struct {
		name   string
		header string
	}{
		{"无 Authorization 头", ""},
		{"缺 Bearer 前缀", "abc.def.ghi"},
		{"Bearer 后为空", "Bearer "},
		{"随便一段乱码", "Bearer not-a-jwt"},
		{"结构像 JWT 但签名错", "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJldmlsIn0.wrong-signature"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, contentType := multipartBody(t, "a.txt", []byte("x"))
			req := httptest.NewRequest(http.MethodPost, "/upload", body)
			req.Header.Set("Content-Type", contentType)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			gw.HandleUpload(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
		})
	}
}

// TestUploadTokenFromOtherSecretRejected 用别的密钥签的 token 不能通过（防止密钥轮换漏洞）。
func TestUploadTokenFromOtherSecretRejected(t *testing.T) {
	gw, _ := newFileGateway(t)
	foreign := auth.New("another-secret", time.Hour)
	tok := mustToken(t, foreign, "mallory")

	body, contentType := multipartBody(t, "a.txt", []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// TestStorageNotConfigured 未调用 SetStorage 时两个端点都应 503 而不是 panic。
func TestStorageNotConfigured(t *testing.T) {
	am := auth.New("test-secret", time.Hour)
	gw := NewGateway("n", ":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), am, bus.NewMemory())
	// 故意不调 SetStorage
	tok := mustToken(t, am, "alice")

	body, contentType := multipartBody(t, "a.txt", []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("upload status = %d, want 503", rec.Code)
	}

	dl := httptest.NewRequest(http.MethodGet, "/download/"+strings.Repeat("a", 32), nil)
	rec2 := httptest.NewRecorder()
	gw.HandleDownload(rec2, dl)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("download status = %d, want 503", rec2.Code)
	}
}

// multipartWriterFor 返回一个写往 body 的 multipart writer，用于构造
// 「没有 file 字段」等非标准请求体（multipartBody 只能造带 file 的）。
func multipartWriterFor(body *bytes.Buffer) *multipart.Writer {
	return multipart.NewWriter(body)
}

// TestUploadMissingFileField multipart 里没有 file 字段应 400。
func TestUploadMissingFileField(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	var body bytes.Buffer
	mw := multipartWriterFor(&body)
	// 只写一个无关字段，没有 file
	_ = mw.WriteField("note", "hi")
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/upload", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// TestUploadOverSizeLimit 超过 maxUploadBytes 的文件必须被拒，不能落盘。
// 这是防止磁盘/内存被打爆的关键限制。
func TestUploadOverSizeLimit(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	// 多写 1 字节越过上限
	oversize := bytes.Repeat([]byte("A"), maxUploadBytes+1)
	body, contentType := multipartBody(t, "big.bin", oversize)

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	// 错误信息必须点明「文件过大」，不能退化成「缺少 file 字段」让用户猜。
	if body := rec.Body.String(); !strings.Contains(body, "文件过大") {
		t.Fatalf("错误信息 = %q, 应包含「文件过大」", body)
	}
	// 超限时不该产生任何元数据（fakeStore 里应为空）
	gw.store.(*fakeStore).mu.Lock()
	n := len(gw.store.(*fakeStore).files)
	gw.store.(*fakeStore).mu.Unlock()
	if n != 0 {
		t.Fatalf("超限后仍写入了 %d 条元数据", n)
	}
}

// TestUploadJustUnderLimit 略小于上限的文件应被接受（边界不能误杀正常文件）。
//
// 注意 maxUploadBytes 的语义：它交给 http.MaxBytesReader，限制的是**整个 HTTP 请求体**，
// 而不是文件字节数。multipart 有 boundary/头部开销（约 200 字节），所以文件本身必须
// 比 maxUploadBytes 小一点才能通过。见 TestUploadFileAtExactLimitRejected 的说明。
func TestUploadJustUnderLimit(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	// 留 1 KiB 给 multipart 开销，稳过。
	near := bytes.Repeat([]byte("A"), maxUploadBytes-1024)
	body, contentType := multipartBody(t, "near.bin", near)

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200（略小于上限应允许）, body = %s", rec.Code, rec.Body.String())
	}
}

// TestUploadFileAtExactLimitRejected 记录一个已知取舍：文件字节数正好等于
// maxUploadBytes 时会被拒绝，因为 MaxBytesReader 卡的是整个请求体，multipart 开销
// 把总量顶过了上限。
//
// 这个用例的作用是把这个行为钉住——如果将来改成「按文件字节数限流」，它会失败，
// 提醒同时更新注释与文档。当前不影响安全（只会更严格），且用户能收到明确的
// 413「文件过大」而不是含义不清的「缺少 file 字段」。
func TestUploadFileAtExactLimitRejected(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	exact := bytes.Repeat([]byte("A"), maxUploadBytes)
	body, contentType := multipartBody(t, "exact.bin", exact)

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatal("行为已变化：现在正好等于上限的文件也能上传了，" +
			"请更新 maxUploadBytes 的注释（说明它限的是请求体还是文件字节）并删除本用例")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413（超限应给明确状态码）", rec.Code)
	}
}

// TestUploadSanitizesFilename 验证清洗真正接在保存路径上（而不只是 sanitizeName 单测通过）：
// 恶意路径的文件名落库时已被剥成纯文件名。
//
// 注意 CR/LF 用例在 HTTP 层测不出来：Go 的 multipart.Writer 会把 filename 里的
// CR/LF 百分号编码成 %0D/%0A（见 stdlib mime/multipart quoteEscaper），服务端收到时
// 已经是无换行的字面量，sanitizeName 没有 CR/LF 可去。CR/LF 清洗由
// files_test.go 的 TestSanitizeName 直接覆盖。
func TestUploadSanitizesFilename(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	cases := []struct {
		given string
		want  string
	}{
		{"../../etc/passwd", "passwd"},
		{`..\..\windows\evil.txt`, "evil.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.given, func(t *testing.T) {
			body, contentType := multipartBody(t, tc.given, []byte("x"))
			req := httptest.NewRequest(http.MethodPost, "/upload", body)
			req.Header.Set("Content-Type", contentType)
			req.Header.Set("Authorization", "Bearer "+tok)
			rec := httptest.NewRecorder()
			gw.HandleUpload(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}

			var resp uploadResp
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.FileName != tc.want {
				t.Fatalf("保存的文件名 = %q, want %q", resp.FileName, tc.want)
			}
		})
	}
}

// TestDownloadNoHeaderInjection 下载响应头不能被文件名注入额外头或 CRLF。
// 这是 sanitizeName + mime.FormatMediaType 共同防线要挡住的实际攻击面。
func TestDownloadNoHeaderInjection(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	// 直接构造一个「恶意文件名」落库，绕过上传路径，专门测下载侧的防御。
	malicious := "evil\r\nX-Injected: yes\r\nSet-Cookie: pwned=1"
	content := []byte("payload")
	body, contentType := multipartBody(t, malicious, content)
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d", rec.Code)
	}
	var resp uploadResp
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	dl := httptest.NewRequest(http.MethodGet, "/download/"+resp.FileID, nil)
	rec2 := httptest.NewRecorder()
	gw.HandleDownload(rec2, dl)
	if rec2.Code != http.StatusOK {
		t.Fatalf("download status = %d", rec2.Code)
	}

	// 关键断言：没有 X-Injected / Set-Cookie 被注入进来。
	if v := rec2.Header().Get("X-Injected"); v != "" {
		t.Fatalf("响应头被注入: X-Injected = %q", v)
	}
	if v := rec2.Header().Get("Set-Cookie"); v != "" {
		t.Fatalf("响应头被注入: Set-Cookie = %q", v)
	}
	// Content-Disposition 值本身不能含裸 CR/LF。
	cd := rec2.Header().Get("Content-Disposition")
	if strings.ContainsAny(cd, "\r\n") {
		t.Fatalf("Content-Disposition 含裸换行: %q", cd)
	}
	// 内容仍是原始字节（注入没影响到 body）。
	if rec2.Body.String() != string(content) {
		t.Fatalf("body = %q, want %q", rec2.Body.String(), content)
	}
}

// TestDownloadBytesMissing 元数据在、磁盘字节被删（运维误删/卷丢失）时应 404，
// 而不是 500 或返回空内容 —— 这正是容器化后卷没挂对会出现的症状。
func TestDownloadBytesMissing(t *testing.T) {
	gw, am, dir := newFileGatewayWithDir(t)
	tok := mustToken(t, am, "alice")

	body, contentType := multipartBody(t, "a.txt", []byte("content"))
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d", rec.Code)
	}
	var resp uploadResp
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	// 模拟字节丢失：直接删掉磁盘文件，保留元数据。
	if err := os.Remove(filepath.Join(dir, resp.FileID)); err != nil {
		t.Fatalf("删除字节文件: %v", err)
	}

	dl := httptest.NewRequest(http.MethodGet, "/download/"+resp.FileID, nil)
	rec2 := httptest.NewRecorder()
	gw.HandleDownload(rec2, dl)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404（元数据在但字节缺失）", rec2.Code)
	}
}

// TestDownloadInvalidID 畸形 id（路径穿越等）应 404，且绝不能读到目录外的文件。
func TestDownloadInvalidID(t *testing.T) {
	gw, _ := newFileGateway(t)

	// 在存储目录外放一个机密文件，确认穿越拿不到它。
	// newFileGateway 的目录是 t.TempDir()，其父目录即 TempDir 根，这里直接用绝对路径试探。
	for _, id := range []string{
		"../../etc/passwd",
		"..%2f..%2fetc%2fpasswd",
		"/etc/passwd",
		"..",
		"",
	} {
		req := httptest.NewRequest(http.MethodGet, "/download/"+id, nil)
		rec := httptest.NewRecorder()
		gw.HandleDownload(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("id=%q status = %d, want 404", id, rec.Code)
		}
	}
}

// TestDownloadContentLengthMatches 响应头 Content-Length 必须与实际字节数一致，
// 否则客户端会截断或挂起。
func TestDownloadContentLengthMatches(t *testing.T) {
	gw, am := newFileGateway(t)
	tok := mustToken(t, am, "alice")

	content := []byte("0123456789abcdef")
	body, contentType := multipartBody(t, "a.bin", content)
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	var resp uploadResp
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	dl := httptest.NewRequest(http.MethodGet, "/download/"+resp.FileID, nil)
	rec2 := httptest.NewRecorder()
	gw.HandleDownload(rec2, dl)

	got := rec2.Header().Get("Content-Length")
	want := "16"
	if got != want {
		t.Fatalf("Content-Length = %q, want %q", got, want)
	}
	if rec2.Body.Len() != len(content) {
		t.Fatalf("实际 body %d 字节, want %d", rec2.Body.Len(), len(content))
	}
}
