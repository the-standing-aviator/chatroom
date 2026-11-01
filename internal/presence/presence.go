// Package presence 抽象「谁在线、在哪个节点」——纯元数据，可外置。
//
// 单机用内存实现（memory.go），分布式用 Redis 实现（redis.go）。
// 注意：这里只存可序列化的元数据（user → node 映射），不含连接/通道等
// 进程内对象；消息的最终投递走 bus 包，两者职责不同。
package presence

import "errors"

// 哨兵错误。
var (
	ErrNotFound = errors.New("presence: not online")
	ErrConflict = errors.New("presence: already online")
)

// Presence 是在线状态注册表接口，以用户昵称为键（与当前协议一致）。
type Presence interface {
	// Join 标记用户上线到某节点；已在线返回 ErrConflict。
	Join(name, node string) error
	// Leave 标记用户下线；幂等，不在线也不报错。
	Leave(name string) error
	// Rename 改名：旧名不在线返回 ErrNotFound，新名已在线返回 ErrConflict。
	Rename(oldName, newName string) error
	// Online 返回所有在线用户名（顺序不保证）。
	Online() ([]string, error)
	// Node 返回用户所在节点；不在线返回 ErrNotFound。
	Node(name string) (string, error)
	// Close 释放底层资源。
	Close() error
}
