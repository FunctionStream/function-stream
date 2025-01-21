package memory

import (
	"context"
	"github.com/functionstream/function-stream/fs/api"
	"sort"
	"sync"
)

type Store struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func (s *Store) Put(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

func (s *Store) Get(_ context.Context, key string) (value []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	if !ok {
		return nil, api.ErrStateNotFound
	}
	return v, nil
}

func (s *Store) List(_ context.Context, startInclusive string, endExclusive string) (keys []string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k := range s.m {
		if k >= startInclusive && k < endExclusive {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)
	return keys, nil
}

func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

func (s *Store) Close() error {
	return nil
}

func NewMemoryStateStore() api.StateStore {
	return &Store{
		m: make(map[string][]byte),
	}
}
