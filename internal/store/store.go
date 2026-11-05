// Package store 定义聊天室的持久化层：数据模型与接口。
//
// V1 用 SQLite 实现（见 sqlite.go），后续可替换为 PostgreSQL 实现，
// 服务端只依赖 Store 接口，不关心底层数据库。
package store

import (
	"errors"
	"time"
)

// User 表示一个注册用户（账号）。
type User struct {
	ID           int64
	Name         string
	PasswordHash string
}

// Message 表示一条需要持久化的消息。
type Message struct {
	ID       int64
	Type     string // chat / private / file
	From     string
	To       string // 私聊目标，公聊为空
	Content  string
	FileID   string // 文件消息：文件 id（即存储/下载 key），普通消息为空
	FileName string // 文件消息：原始文件名
	FileSize int64  // 文件消息：字节数
	TS       time.Time
}

// File 表示一个已上传文件的元数据（文件字节单独存，见 internal/storage）。
type File struct {
	ID       string    // 全局唯一 id（服务端生成），即下载路径 /download/{id}
	Name     string    // 原始文件名（展示与下载提示用）
	Size     int64     // 字节数
	Uploader string    // 上传者昵称
	TS       time.Time // 上传时间
}

// ErrNotFound 表示记录不存在。
var ErrNotFound = errors.New("record not found")

// ErrConflict 表示唯一约束冲突（如昵称已被注册）。
var ErrConflict = errors.New("conflict")

// Store 是持久化层接口。
type Store interface {
	// CreateUser 注册新用户，昵称重复返回 ErrConflict。
	CreateUser(name, passwordHash string) error
	// GetUser 按昵称查询用户，不存在返回 ErrNotFound。
	GetUser(name string) (User, error)
	// SaveMessage 保存一条消息。
	SaveMessage(m Message) error
	// ListMessages 返回 viewer 有权看到的最近 limit 条消息，按时间升序。
	//
	// viewer 是发起查询的用户昵称，用于过滤私聊：公聊（To 为空）所有人可见，
	// 私聊只有收发双方可见。不传 viewer 就等于把所有人的私聊泄漏给每个请求方。
	ListMessages(viewer string, limit int) ([]Message, error)
	// SaveFile 保存文件元数据。
	SaveFile(f File) error
	// GetFile 按 id 查询文件元数据，不存在返回 ErrNotFound。
	GetFile(id string) (File, error)
	// Close 释放底层资源。
	Close() error
}
