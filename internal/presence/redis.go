package presence

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// presenceKey 是 Redis 哈希的键，字段为昵称、值为节点。
const presenceKey = "presence"

// Redis 是 Presence 的 Redis 实现（分布式）。用单个哈希存 name → node。
type Redis struct {
	rdb *redis.Client
}

// NewRedis 连接指定地址的 Redis 并返回 presence 实现。
func NewRedis(addr string) *Redis {
	return &Redis{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (r *Redis) Join(name, node string) error {
	// HSETNX 仅当字段不存在时写入，返回 true 表示新增成功，天然判重。
	added, err := r.rdb.HSetNX(context.Background(), presenceKey, name, node).Result()
	if err != nil {
		return err
	}
	if !added {
		return ErrConflict
	}
	return nil
}

func (r *Redis) Leave(name string) error {
	return r.rdb.HDel(context.Background(), presenceKey, name).Err()
}

// Rename 改名：旧名须存在、新名须不存在，再整体搬移。
// 用 Lua 脚本保证「查旧名 + 查新名 + 搬移」原子，避免并发改名竞态。
// 返回值约定：-1 旧名不在线，-2 新名已在线，0 成功。
func (r *Redis) Rename(oldName, newName string) error {
	const script = `
		if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
			return -1
		end
		if redis.call("HEXISTS", KEYS[1], ARGV[2]) == 1 then
			return -2
		end
		local node = redis.call("HGET", KEYS[1], ARGV[1])
		redis.call("HDEL", KEYS[1], ARGV[1])
		redis.call("HSET", KEYS[1], ARGV[2], node)
		return 0
	`
	n, err := redis.NewScript(script).
		Run(context.Background(), r.rdb, []string{presenceKey}, oldName, newName).
		Int()
	if err != nil {
		return err
	}
	switch n {
	case -1:
		return ErrNotFound
	case -2:
		return ErrConflict
	default:
		return nil
	}
}

func (r *Redis) Online() ([]string, error) {
	return r.rdb.HKeys(context.Background(), presenceKey).Result()
}

func (r *Redis) Node(name string) (string, error) {
	node, err := r.rdb.HGet(context.Background(), presenceKey, name).Result()
	if err == redis.Nil {
		return "", ErrNotFound
	}
	return node, err
}

func (r *Redis) Close() error {
	return r.rdb.Close()
}
