package memtable

import (
	"sync"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	mu   sync.RWMutex
	data *skiplist.SkipList
	size int64
}

func NewMemtable() *Memtable {
	return &Memtable{data: skiplist.New(skiplist.Bytes)}

}

func (m *Memtable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e := m.data.Get(key); e != nil {
		// If key already exists, adjust size
		oldValue := e.Value.([]byte)
		m.size -= int64(len(oldValue))
	} else {
		// New key, just add key size
		m.size += int64(len(key))
	}
	m.data.Set(key, value)
	m.size += int64(len(value))
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.data.GetValue(key); ok {
		return v.([]byte), true
	}
	return nil, false
}

func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}
