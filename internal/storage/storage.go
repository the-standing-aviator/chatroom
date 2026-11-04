// Package storage 抽象文件字节的存取。
//
// 与 store / presence / bus 同一个套路：上层（HTTP 上传/下载）只依赖 Storage 接口，
// 不关心文件到底存在哪。V4 单节点用本地磁盘实现（disk.go），
// V5 多机时换成 S3 / MinIO 实现同一个接口，上层代码零改动。
package storage

import "io"

// Storage 是文件字节的存取接口。
type Storage interface {
	// Save 把 r 的内容写入存储，返回文件 id 与实际写入的字节数。
	// id 由实现生成，后续 Open / Delete 都拿它当 key。
	Save(r io.Reader) (id string, size int64, err error)
	// Open 按 id 打开文件内容，调用方负责 Close。
	Open(id string) (io.ReadCloser, error)
	// Delete 删除指定 id 的文件。V4 暂不强制用（清理/撤回时再接入），
	// 但接口先预留，避免以后改接口牵动所有实现。
	Delete(id string) error
}
