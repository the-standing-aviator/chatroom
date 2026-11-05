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
	id        INTEGER PRIMARY KEY AUTOINCREMENT,
	type      TEXT NOT NULL,
	"from"    TEXT NOT NULL DEFAULT '',
	"to"      TEXT NOT NULL DEFAULT '',
	content   TEXT NOT NULL DEFAULT '',
	file_id   TEXT NOT NULL DEFAULT '',
	file_name TEXT NOT NULL DEFAULT '',
	file_size INTEGER NOT NULL DEFAULT 0,
	ts        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE TABLE IF NOT EXISTS files (
	id       TEXT PRIMARY KEY,
	name     TEXT NOT NULL,
	size     INTEGER NOT NULL,
	uploader TEXT NOT NULL,
	ts       INTEGER NOT NULL
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	// 迁移老库：SQLite 的 CREATE TABLE IF NOT EXISTS 不会给已存在的 messages 表补新列，
	// 逐个 ALTER；撞到「列已存在」就忽略，幂等（新库建表已含列、老库在此补上）。
	for _, stmt := range []string{
		`ALTER TABLE messages ADD COLUMN file_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE messages ADD COLUMN file_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE messages ADD COLUMN file_size INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil &&
			!strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}
	return nil
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
		`INSERT INTO messages (type, "from", "to", content, file_id, file_name, file_size, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.Type, m.From, m.To, m.Content, m.FileID, m.FileName, m.FileSize, m.TS.Unix(),
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
		SELECT id, type, "from", "to", content, file_id, file_name, file_size, ts FROM (
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
		if err := rows.Scan(&m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.FileID, &m.FileName, &m.FileSize, &ts); err != nil {
			return nil, err
		}
		m.TS = time.Unix(ts, 0)
		out = append(out, m)
	}
	return out, rows.Err()
}

// SaveFile 保存文件元数据。
func (s *SQLite) SaveFile(f File) error {
	_, err := s.db.Exec(
		`INSERT INTO files (id, name, size, uploader, ts) VALUES (?, ?, ?, ?, ?)`,
		f.ID, f.Name, f.Size, f.Uploader, f.TS.Unix(),
	)
	return err
}

// GetFile 按 id 查询文件元数据，不存在返回 ErrNotFound。
func (s *SQLite) GetFile(id string) (File, error) {
	var f File
	var ts int64
	err := s.db.QueryRow(
		`SELECT id, name, size, uploader, ts FROM files WHERE id = ?`, id,
	).Scan(&f.ID, &f.Name, &f.Size, &f.Uploader, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	f.TS = time.Unix(ts, 0)
	return f, err
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
