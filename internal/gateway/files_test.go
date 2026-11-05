package gateway

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/presence"
	"communicate/internal/storage"
)

// newFileGateway 构造一个带真实磁盘 storage + 内存 fakeStore 的 Gateway，用于上传/下载测试。
// storage 用真实 Disk 实现：要真实验证「字节落盘 → 下载读回」的往返，不能用假实现。
func newFileGateway(t *testing.T) (*Gateway, *auth.Manager) {
	t.Helper()
	disk, err := storage.NewDisk(t.TempDir())
	if err != nil {
		t.Fatalf("NewDisk: %v", err)
	}
	am := auth.New("test-secret", time.Hour)
	gw := NewGateway("test-node", ":0", zap.NewNop(), newFakeStore(), presence.NewMemory(), am, bus.NewMemory())
	gw.SetStorage(disk)
	return gw, am
}

// multipartBody 构造一个含单个 file 字段的 multipart 请求体，返回 body 与 Content-Type。
func multipartBody(t *testing.T, filename string, content []byte) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write(content); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	return &body, mw.FormDataContentType()
}

// TestHandleUploadAndDownload 验证完整往返：上传（JWT 鉴权）→ 元数据落库 → 下载读回原字节。
func TestHandleUploadAndDownload(t *testing.T) {
	gw, am := newFileGateway(t)

	token, err := am.Issue("alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	content := []byte("hello file content")
	body, contentType := multipartBody(t, "report.pdf", content)

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token)

	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp uploadResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode upload resp: %v", err)
	}
	if resp.FileID == "" || resp.FileName != "report.pdf" || resp.FileSize != int64(len(content)) {
		t.Fatalf("upload resp = %+v", resp)
	}

	// 元数据已落库（fakeStore），上传者应为 token 里的 alice
	f, err := gw.store.GetFile(resp.FileID)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if f.Name != "report.pdf" || f.Uploader != "alice" || f.Size != int64(len(content)) {
		t.Fatalf("saved file = %+v", f)
	}

	// 下载：GET /download/{id} 应原样返回字节，且 Content-Disposition 带原始文件名
	dl := httptest.NewRequest(http.MethodGet, "/download/"+resp.FileID, nil)
	rec2 := httptest.NewRecorder()
	gw.HandleDownload(rec2, dl)

	if rec2.Code != http.StatusOK {
		t.Fatalf("download status = %d", rec2.Code)
	}
	if rec2.Body.String() != string(content) {
		t.Fatalf("download body = %q, want %q", rec2.Body.String(), content)
	}
	if cd := rec2.Header().Get("Content-Disposition"); !strings.Contains(cd, "report.pdf") {
		t.Fatalf("Content-Disposition = %q, want 含 report.pdf", cd)
	}
}

// TestHandleUploadRequiresAuth 验证：不带 JWT 的上传被拒绝（401）。
func TestHandleUploadRequiresAuth(t *testing.T) {
	gw, _ := newFileGateway(t)

	body, contentType := multipartBody(t, "a.txt", []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", contentType)
	// 不带 Authorization 头

	rec := httptest.NewRecorder()
	gw.HandleUpload(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// TestHandleDownloadNotFound 验证：未知文件 id 返回 404。
func TestHandleDownloadNotFound(t *testing.T) {
	gw, _ := newFileGateway(t)

	req := httptest.NewRequest(http.MethodGet, "/download/0123456789abcdef0123456789abcdef", nil)
	rec := httptest.NewRecorder()
	gw.HandleDownload(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// TestSanitizeName 验证文件名清洗：去路径穿越、去 CR/LF（防响应头注入）、去空白。
func TestSanitizeName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "report.pdf", want: "report.pdf"},
		{in: "../etc/passwd", want: "passwd"},
		{in: "..\\..\\windows\\evil.txt", want: "evil.txt"},
		{in: "a\r\nb.txt", want: "ab.txt"},
		{in: "  spaced.txt  ", want: "spaced.txt"},
		{in: "", want: "unnamed"},
	}
	for _, tc := range cases {
		if got := sanitizeName(tc.in); got != tc.want {
			t.Fatalf("sanitizeName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
