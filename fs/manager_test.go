package fs

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/functionstream/function-stream/fs/statestore/memory"
	"github.com/stretchr/testify/require"
	"testing"
)

type MockRuntimeAdapter struct {
	instances map[string]api.Instance
}

func NewMockRuntimeAdapter() *MockRuntimeAdapter {
	return &MockRuntimeAdapter{
		instances: make(map[string]api.Instance),
	}
}

type MockPackageLoader struct {
	pkgs map[string]model.Package
}

func (m *MockPackageLoader) LoadPackage(ctx context.Context, name string) (*model.Package, error) {
	if pkg, ok := m.pkgs[name]; ok {
		return &pkg, nil
	}
	return nil, fmt.Errorf("package %s not found", name)
}

func (m *MockRuntimeAdapter) DeployFunction(ctx context.Context, instance api.Instance) error {
	m.instances[instance.Function().Name] = instance
	return nil
}

func (m *MockRuntimeAdapter) DeleteFunction(ctx context.Context, name string) error {
	if _, ok := m.instances[name]; !ok {
		return fmt.Errorf("function %s not found", name)
	}
	delete(m.instances, name)
	return nil
}

func TestManagerImpl(t *testing.T) {
	pkgLoader := &MockPackageLoader{
		pkgs: map[string]model.Package{
			"test-pkg": {
				Name: "test-pkg",
				Type: "test-runtime",
				Modules: map[string]model.ModuleConfig{
					"test-module": {
						"test-config": {
							Type:     "string",
							Required: "true",
						},
					},
				},
			},
		},
	}

	mockRuntimeAdapter := NewMockRuntimeAdapter()

	m, err := NewManager(ManagerConfig{
		RuntimeMap: map[string]api.RuntimeAdapter{
			"test-runtime": mockRuntimeAdapter,
		},
		PackageLoader: pkgLoader,
		StateStore:    memory.NewMemoryStateStore(),
	})

	require.NoError(t, err)

	f := &model.Function{
		Name:    "test-func",
		Package: "test-pkg",
		Module:  "test-module",
		Config: map[string]string{
			"test-config": "test-value",
		},
	}

	t.Run("Deploy", func(t *testing.T) {
		err = m.Deploy(context.Background(), f)
		require.NoError(t, err)
		ins, ok := mockRuntimeAdapter.instances[f.Name]
		require.True(t, ok)
		insF := ins.Function()
		require.Equal(t, f, insF)
		ss := ins.StateStore()
		require.IsType(t, &memory.Store{}, ss)
	})

	t.Run("Delete", func(t *testing.T) {
		err = m.Delete(context.Background(), f.Name)
		require.NoError(t, err)
		_, ok := mockRuntimeAdapter.instances[f.Name]
		require.False(t, ok)
	})

	t.Run("Deploy with invalid function", func(t *testing.T) {
		err = m.Deploy(context.Background(), &model.Function{})
		require.Error(t, err)
	})
}
