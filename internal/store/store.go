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
	ID      int64
	Type    string // chat / private
	From    string
	To      string // 私聊目标，公聊为空
	Content string
	TS      time.Time
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
	// ListMessages 返回最近的 limit 条消息，按时间升序。
	ListMessages(limit int) ([]Message, error)
	// Close 释放底层资源。
	Close() error
}
