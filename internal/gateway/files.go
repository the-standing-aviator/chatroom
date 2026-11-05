package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/storage"
	"communicate/internal/store"
)

// 文件上传/下载的 HTTP 端点。
//
// 文件字节走 HTTP（不进聊天帧——帧上限 MaxFrameSize=64KB），聊天链路只传元数据
// （见 broadcastFile）。因此拆成两步：
//   1. 客户端 POST /upload 上传字节 → 服务端落盘（storage）+ 记元数据（store.SaveFile）+ 返回 file_id；
//   2. 客户端用 file_id 发一条 WS 的 TypeFile 消息 → broadcastFile 广播元数据给在线用户。
// 下载时其他用户拿 file_id 拼 /download/{id} 即可取回字节。

// maxUploadBytes 上传请求体上限：10 MiB。防恶意/误操作的大文件打爆磁盘与内存；
// 真正支持大文件时再改成可配置或走分片上传。
//
// 注意它限制的是**整个 HTTP 请求体**（交给 http.MaxBytesReader），不是文件字节数：
// multipart 编码的 boundary 与头部有几百字节开销，所以文件本身必须略小于 10 MiB。
// 因此「正好 10 MiB 的文件」会被拒绝（413）——这是当前实现的已知取舍，
// 用 TestUploadFileAtExactLimitRejected 钉住，改动时该用例会失败提醒。
const maxUploadBytes = 10 << 20

// uploadResp 是 /upload 的响应体：客户端拿到 file_id 后，用它发 TypeFile 消息 + 拼下载链接。
type uploadResp struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

// SetStorage 设置文件字节存储实现（本地磁盘 / 以后的对象存储）。nil 表示未配置，
// 此时 /upload、/download 返回 503。必须在启动 HTTP 服务前调用（之后只读，无并发写）。
func (gw *Gateway) SetStorage(s storage.Storage) {
	gw.storage = s
}

// HandleUpload 处理 POST /upload：校验 JWT → 存文件字节 → 记文件元数据 → 返回 file_id。
func (gw *Gateway) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if gw.storage == nil {
		http.Error(w, "文件存储未配置", http.StatusServiceUnavailable)
		return
	}

	// 鉴权：上传必须知道是谁传的（记 uploader），用登录时签发的 JWT。
	uploader, err := gw.uploader(r)
	if err != nil {
		http.Error(w, "未授权：请先登录", http.StatusUnauthorized)
		return
	}

	// 限体积：MaxBytesReader 让后续读取在超过上限时报错（而不是默默读完写满磁盘）。
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)

	file, header, err := r.FormFile("file")
	if err != nil {
		// 超限和「缺字段」都会在这里报错，但含义完全不同：前者要告诉用户文件太大，
		// 后者是请求格式不对。http.MaxBytesReader 触发时返回 *http.MaxBytesError，
		// 用 errors.As 精确识别，别让用户对着「缺少 file 字段」猜自己哪里写错了。
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, fmt.Sprintf("文件过大：请求体上限 %d MiB", maxUploadBytes>>20), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "缺少 file 字段或读取失败", http.StatusBadRequest)
		return
	}
	defer file.Close()

	id, size, err := gw.storage.Save(file)
	if err != nil {
		gw.log.Error("storage save failed", zap.Error(err))
		http.Error(w, "文件保存失败", http.StatusInternalServerError)
		return
	}

	// 元数据落库：字节在 storage（磁盘，以后可换对象存储），这里只记 id→名字/大小/上传者。
	fileName := sanitizeName(header.Filename)
	if err := gw.store.SaveFile(store.File{ID: id, Name: fileName, Size: size, Uploader: uploader, TS: time.Now()}); err != nil {
		gw.log.Error("save file meta failed", zap.Error(err))
		http.Error(w, "文件元数据保存失败", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(uploadResp{FileID: id, FileName: fileName, FileSize: size})
}

// HandleDownload 处理 GET /download/{id}：查元数据 → 打开字节流 → 带原始文件名回放。
//
// 下载**有意免鉴权**（能力 URL 模型）：file_id 是 32 位随机 hex，不可猜；
// 拿到链接的人即可下载。这是一个产品决策而非待办——私聊的接收方应当能把文件
// 转发给别人（类似网盘分享链接）。
//
// 「私聊文件的隐私」由消息投递保证：文件消息本身只发给私聊双方，不广播、不进
// 他人历史（见 privateFile 与 ListMessages 的可见性过滤）。链接一旦被接收方
// 主动转发出去，则视为其有意分享。
//
// 若将来要收紧成「仅双方可下」，改动点在这里：校验 JWT 并比对 files.uploader
// 与 messages 中该 file_id 的 to 字段；同时前端下载要改成 fetch + blob，
// 因为 <a href> 无法携带 Authorization 头。
func (gw *Gateway) HandleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if gw.storage == nil {
		http.Error(w, "文件存储未配置", http.StatusServiceUnavailable)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/download/")
	f, err := gw.store.GetFile(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			gw.log.Error("get file meta failed", zap.Error(err))
			http.Error(w, "查询文件失败", http.StatusInternalServerError)
		}
		return
	}

	rc, err := gw.storage.Open(id)
	if err != nil {
		// 元数据在但字节缺失（磁盘文件被删等）：按 404 处理。
		gw.log.Error("storage open failed", zap.Error(err))
		http.Error(w, "文件不存在", http.StatusNotFound)
		return
	}
	defer rc.Close()

	// application/octet-stream + attachment 强制下载；f.Name 已在上传时清洗，这里再防一手。
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": sanitizeName(f.Name)}))
	w.Header().Set("Content-Length", strconv.FormatInt(f.Size, 10))
	_, _ = io.Copy(w, rc)
}

// uploader 从 Authorization 头提取并校验 JWT，返回 token 的 sub（上传者昵称）。
func (gw *Gateway) uploader(r *http.Request) (string, error) {
	authz := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return "", auth.ErrInvalid
	}
	return gw.auth.Verify(strings.TrimPrefix(authz, prefix))
}

// sanitizeName 清理客户端提供的文件名：去路径（防 ../ 穿越）、去 CR/LF（防响应头注入）、
// 去首尾空白。该名字只用于「展示/下载提示」与 Content-Disposition，不参与磁盘路径拼接
// （磁盘文件名是 storage 自己生成的随机 hex id）。
func sanitizeName(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = path.Base(name)
	name = strings.ReplaceAll(name, "\r", "")
	name = strings.ReplaceAll(name, "\n", "")
	name = strings.TrimSpace(name)
	// path.Base("") 与 path.Base("/") 会返回 "." / "/"，一并当作空名处理。
	if name == "" || name == "." || name == "/" {
		return "unnamed"
	}
	return name
}
