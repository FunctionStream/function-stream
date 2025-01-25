package memory

import (
	"context"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"sync"
)

type MemoryPackageStorage struct {
	mu sync.RWMutex
	m  map[string]*model.Package
}

func (m *MemoryPackageStorage) Create(_ context.Context, pkg *model.Package) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.m[pkg.Name]; ok {
		return api.ErrPackageAlreadyExists
	}
	m.m[pkg.Name] = pkg
	return nil
}

func (m *MemoryPackageStorage) Read(ctx context.Context, name string) (*model.Package, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pkg, ok := m.m[name]; ok {
		return pkg, nil
	}
	return nil, api.ErrPackageNotFound
}

func (m *MemoryPackageStorage) List(ctx context.Context) ([]*model.Package, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pkgs := make([]*model.Package, 0, len(m.m))
	for _, pkg := range m.m {
		pkgs = append(pkgs, pkg)
	}
	return pkgs, nil
}

func (m *MemoryPackageStorage) Update(ctx context.Context, pkg *model.Package) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.m[pkg.Name]; !ok {
		return api.ErrPackageNotFound
	}
	m.m[pkg.Name] = pkg
	return nil
}

func (m *MemoryPackageStorage) Delete(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.m[name]; !ok {
		return api.ErrPackageNotFound
	}
	delete(m.m, name)
	return nil
}

func NewMemoryPackageStorage() api.PackageStorage {
	return &MemoryPackageStorage{
		m: make(map[string]*model.Package),
	}
}
