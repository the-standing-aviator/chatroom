package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // 注册 database/sql 驱动，驱动名为 "pgx"
)

// Postgres 是基于 PostgreSQL 的 Store 实现。
type Postgres struct {
	db *sql.DB
}

// OpenPostgres 打开指定 DSN 的 PostgreSQL 数据库并初始化表结构。
func OpenPostgres(dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	// 与 SQLite 不同：PostgreSQL 支持并发读写，交给连接池管理，不再限制单连接
	// （这正是 P2-4「单连接串行瓶颈」的修复点）。
	//
	// 但池必须设上限：database/sql 不 SetMaxOpenConns 时默认无上限，高并发下会
	// 开满连接打爆 PG 的 max_connections（默认 100），报 SQLSTATE 53300「too many clients」。
	// 上限取 40：给多节点部署留余量（2 节点共 80 < 100），超出部分在池内排队而非报错。
	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(40)
	db.SetConnMaxLifetime(30 * time.Minute) // 防 PG 侧断开的长连接被复用

	s := &Postgres{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// migrate 建表。逐条执行，因为 pgx 走扩展协议，一次 Exec 只允许一条语句。
func (s *Postgres) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id            BIGSERIAL PRIMARY KEY,
			name          TEXT NOT NULL UNIQUE,
			password_hash TEXT NOT NULL,
			created_at    TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id        BIGSERIAL PRIMARY KEY,
			type      TEXT NOT NULL,
			"from"    TEXT NOT NULL DEFAULT '',
			"to"      TEXT NOT NULL DEFAULT '',
			content   TEXT NOT NULL DEFAULT '',
			file_id   TEXT NOT NULL DEFAULT '',
			file_name TEXT NOT NULL DEFAULT '',
			file_size BIGINT NOT NULL DEFAULT 0,
			ts        TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts)`,
		`CREATE TABLE IF NOT EXISTS files (
			id       TEXT PRIMARY KEY,
			name     TEXT NOT NULL,
			size     BIGINT NOT NULL,
			uploader TEXT NOT NULL,
			ts       TIMESTAMPTZ NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	// 迁移老库：与 SQLite 同理，CREATE TABLE IF NOT EXISTS 不会给已存在的 messages 表补新列，
	// 用 ADD COLUMN IF NOT EXISTS 幂等补齐（新库建表已含列、老库在此补上）。
	for _, stmt := range []string{
		`ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_size BIGINT NOT NULL DEFAULT 0`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// CreateUser 注册新用户，昵称重复返回 ErrConflict。
func (s *Postgres) CreateUser(name, passwordHash string) error {
	_, err := s.db.Exec(
		`INSERT INTO users (name, password_hash, created_at) VALUES ($1, $2, $3)`,
		name, passwordHash, time.Now(),
	)
	if err != nil && isPGUniqueViolation(err) {
		return ErrConflict
	}
	return err
}

// GetUser 按昵称查询用户，不存在返回 ErrNotFound。
func (s *Postgres) GetUser(name string) (User, error) {
	var u User
	err := s.db.QueryRow(
		`SELECT id, name, password_hash FROM users WHERE name = $1`, name,
	).Scan(&u.ID, &u.Name, &u.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// SaveMessage 保存一条消息。
func (s *Postgres) SaveMessage(m Message) error {
	_, err := s.db.Exec(
		`INSERT INTO messages (type, "from", "to", content, file_id, file_name, file_size, ts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		m.Type, m.From, m.To, m.Content, m.FileID, m.FileName, m.FileSize, m.TS,
	)
	return err
}

// ListMessages 返回 viewer 有权看到的最近 limit 条消息，按时间升序（最旧在前，最新在后）。
//
// 可见性规则：公聊（"to" 为空）所有人可见；私聊只有 from/to 是 viewer 本人的可见。
// 过滤放在 SQL 里而不是查出来再筛，是为了让 limit 的语义正确——否则先取 limit 条
// 再过滤，viewer 可能拿到远少于 limit 条消息。
func (s *Postgres) ListMessages(viewer string, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, type, "from", "to", content, file_id, file_name, file_size, ts FROM (
			SELECT * FROM messages
			WHERE "to" = '' OR "from" = $1 OR "to" = $1
			ORDER BY id DESC LIMIT $2
		) ORDER BY id ASC`, viewer, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.FileID, &m.FileName, &m.FileSize, &m.TS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SaveFile 保存文件元数据。
func (s *Postgres) SaveFile(f File) error {
	_, err := s.db.Exec(
		`INSERT INTO files (id, name, size, uploader, ts) VALUES ($1, $2, $3, $4, $5)`,
		f.ID, f.Name, f.Size, f.Uploader, f.TS,
	)
	return err
}

// GetFile 按 id 查询文件元数据，不存在返回 ErrNotFound。
func (s *Postgres) GetFile(id string) (File, error) {
	var f File
	err := s.db.QueryRow(
		`SELECT id, name, size, uploader, ts FROM files WHERE id = $1`, id,
	).Scan(&f.ID, &f.Name, &f.Size, &f.Uploader, &f.TS)
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	return f, err
}

// Close 释放底层资源。
func (s *Postgres) Close() error {
	return s.db.Close()
}

// isPGUniqueViolation 判断错误是否由唯一约束冲突引起。
// PostgreSQL 用标准 SQLSTATE 错误码 23505 表示 unique_violation。
func isPGUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
