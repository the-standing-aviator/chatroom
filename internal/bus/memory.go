package bus

import (
	"sync"

	"communicate/internal/protocol"
)

// subscriber 是内存实现里的一条订阅记录。
// id 是唯一标识，用于取消订阅——func 值在 Go 里不可比较（只能和 nil 比），
// 不能用 handler 本身当身份，所以额外分配一个递增 id。
type subscriber struct {
	id      uint64
	handler func(protocol.Message)
}

// Memory 是 Bus 的内存实现（单机 / 单元测试用）。
// 订阅者都在同一进程内，Publish 直接遍历调用——等价于进程内直接投递。
type Memory struct {
	mu   sync.RWMutex
	next uint64
	subs map[string][]*subscriber // subject → 订阅列表（保持订阅顺序）
}

// NewMemory 创建一个空的内存总线。
func NewMemory() *Memory {
	return &Memory{subs: make(map[string][]*subscriber)}
}

// memorySub 实现 Subscription 接口，指向所属总线与订阅 id。
type memorySub struct {
	bus     *Memory
	subject string
	id      uint64
}

// Unsubscribe 从总线移除该订阅。幂等：id 不存在时静默返回。
func (s *memorySub) Unsubscribe() error {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	list := s.bus.subs[s.subject]
	for i, sub := range list {
		if sub.id == s.id {
			s.bus.subs[s.subject] = append(list[:i], list[i+1:]...)
			break
		}
	}
	return nil
}

// Subscribe 注册一个 handler 到 subject，返回可取消的订阅句柄。
func (m *Memory) Subscribe(subject string, handler func(protocol.Message)) (Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	sub := &subscriber{id: m.next, handler: handler}
	m.subs[subject] = append(m.subs[subject], sub)
	return &memorySub{bus: m, subject: subject, id: sub.id}, nil
}

// Publish 把消息投递给当前订阅该主题的所有 handler。
// 先在读锁内快照订阅列表，再在锁外调用 handler——handler 可能重新 Subscribe/Unsubscribe，
// 持锁调用会导致死锁；快照也保证「发布那一刻」的订阅集合是确定的。
func (m *Memory) Publish(subject string, msg protocol.Message) error {
	m.mu.RLock()
	snapshot := make([]*subscriber, len(m.subs[subject]))
	copy(snapshot, m.subs[subject])
	m.mu.RUnlock()

	for _, sub := range snapshot {
		sub.handler(msg)
	}
	return nil
}

// Close 清空所有订阅（内存实现无外部资源，只需复位状态）。
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs = make(map[string][]*subscriber)
	return nil
}
