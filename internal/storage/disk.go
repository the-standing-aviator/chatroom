package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

// Disk 是 Storage 的本地磁盘实现：每个文件存成目录下的一个独立文件，
// 文件名就是 id（随机 hex）。V4 单节点够用；多节点时换对象存储实现。
type Disk struct {
	dir string
}

// NewDisk 返回以 dir 为存储目录的 Disk，并确保目录存在。
func NewDisk(dir string) (*Disk, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create dir: %w", err)
	}
	return &Disk{dir: dir}, nil
}

// idPattern 严格校验 id 是 32 位纯十六进制。
// id 会拼进文件路径，校验是为了挡住路径穿越（如 "../etc/passwd"）。
var idPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// newID 用 crypto/rand 生成 16 字节随机数并转成 32 位 hex 字符串。
// 不用自增 id 是给 V5 多机做准备：随机 id 天然全局唯一，不依赖单点发号。
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// path 校验 id 合法性后拼出完整文件路径。
func (d *Disk) path(id string) (string, error) {
	if !idPattern.MatchString(id) {
		return "", fmt.Errorf("storage: invalid file id %q", id)
	}
	return filepath.Join(d.dir, id), nil
}

// Save 把 r 的内容写入一个新文件，返回文件 id 与字节数。
func (d *Disk) Save(r io.Reader) (string, int64, error) {
	id, err := newID()
	if err != nil {
		return "", 0, err
	}
	p, err := d.path(id)
	if err != nil {
		return "", 0, err
	}
	f, err := os.Create(p)
	if err != nil {
		return "", 0, fmt.Errorf("storage: create file: %w", err)
	}
	defer f.Close()

	n, err := io.Copy(f, r)
	if err != nil {
		// 写到一半失败会留下残缺文件，清掉，避免脏数据。
		_ = os.Remove(p)
		return "", 0, fmt.Errorf("storage: write file: %w", err)
	}
	return id, n, nil
}

// Open 按 id 打开文件，返回可读流。
func (d *Disk) Open(id string) (io.ReadCloser, error) {
	p, err := d.path(id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Delete 删除指定 id 的文件。
func (d *Disk) Delete(id string) error {
	p, err := d.path(id)
	if err != nil {
		return err
	}
	return os.Remove(p)
}
