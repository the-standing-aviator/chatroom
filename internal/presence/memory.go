package presence

import "sync"

// Memory 是 Presence 的内存实现（单机 / 单元测试用）。
type Memory struct {
	mu     sync.RWMutex
	online map[string]string // name → node
}

// NewMemory 创建一个空的内存 presence。
func NewMemory() *Memory {
	return &Memory{online: make(map[string]string)}
}

func (m *Memory) Join(name, node string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.online[name]; ok {
		return ErrConflict
	}
	m.online[name] = node
	return nil
}

func (m *Memory) Leave(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.online, name)
	return nil
}

func (m *Memory) Rename(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.online[oldName]; !ok {
		return ErrNotFound
	}
	if _, ok := m.online[newName]; ok {
		return ErrConflict
	}
	m.online[newName] = m.online[oldName]
	delete(m.online, oldName)
	return nil
}

func (m *Memory) Online() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.online))
	for name := range m.online {
		names = append(names, name)
	}
	return names, nil
}

func (m *Memory) Node(name string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.online[name]
	if !ok {
		return "", ErrNotFound
	}
	return node, nil
}

func (m *Memory) Close() error { return nil }
