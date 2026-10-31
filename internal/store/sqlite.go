package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 注册 database/sql 驱动，驱动名为 "sqlite"
)

// SQLite 是基于 SQLite 的 Store 实现（纯 Go，无 cgo 依赖）。
type SQLite struct {
	db *sql.DB
}

// Open 打开（不存在则创建）指定路径的 SQLite 数据库并初始化表结构。
func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite 是单写者，限制连接数为 1 可避免并发写触发 "database is locked"。
	db.SetMaxOpenConns(1)

	s := &SQLite{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS users (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	name          TEXT NOT NULL UNIQUE,
	password_hash TEXT NOT NULL,
	created_at    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	id      INTEGER PRIMARY KEY AUTOINCREMENT,
	type    TEXT NOT NULL,
	"from"  TEXT NOT NULL DEFAULT '',
	"to"    TEXT NOT NULL DEFAULT '',
	content TEXT NOT NULL DEFAULT '',
	ts      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// CreateUser 注册新用户，昵称重复返回 ErrConflict。
func (s *SQLite) CreateUser(name, passwordHash string) error {
	_, err := s.db.Exec(
		`INSERT INTO users (name, password_hash, created_at) VALUES (?, ?, ?)`,
		name, passwordHash, time.Now().Unix(),
	)
	if err != nil && isSQLiteUniqueViolation(err) {
		return ErrConflict
	}
	return err
}

// GetUser 按昵称查询用户，不存在返回 ErrNotFound。
func (s *SQLite) GetUser(name string) (User, error) {
	var u User
	err := s.db.QueryRow(
		`SELECT id, name, password_hash FROM users WHERE name = ?`, name,
	).Scan(&u.ID, &u.Name, &u.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// SaveMessage 保存一条消息。
func (s *SQLite) SaveMessage(m Message) error {
	_, err := s.db.Exec(
		`INSERT INTO messages (type, "from", "to", content, ts) VALUES (?, ?, ?, ?, ?)`,
		m.Type, m.From, m.To, m.Content, m.TS.Unix(),
	)
	return err
}

// ListMessages 返回最近的 limit 条消息，按时间升序（最旧在前，最新在后）。
func (s *SQLite) ListMessages(limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 50
	}
	// 先按 id 倒序取最近的 limit 条，再正序返回，保证时间从旧到新。
	rows, err := s.db.Query(`
		SELECT id, type, "from", "to", content, ts FROM (
			SELECT * FROM messages ORDER BY id DESC LIMIT ?
		) ORDER BY id ASC`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Message
	for rows.Next() {
		var m Message
		var ts int64
		if err := rows.Scan(&m.ID, &m.Type, &m.From, &m.To, &m.Content, &ts); err != nil {
			return nil, err
		}
		m.TS = time.Unix(ts, 0)
		out = append(out, m)
	}
	return out, rows.Err()
}

// Close 释放底层资源。
func (s *SQLite) Close() error {
	return s.db.Close()
}

// isSQLiteUniqueViolation 判断错误是否由唯一约束冲突引起。
// modernc.org/sqlite 返回的错误文本形如 "UNIQUE constraint failed: users.name"。
func isSQLiteUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
