package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func getListener(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	t.Logf("Listening on %s\n", ln.Addr().String())
	return ln
}

type MockManager struct {
	functions sync.Map
}

func (m *MockManager) Deploy(ctx context.Context, f *model.Function) error {
	if _, ok := m.functions.Load(f.Name); ok {
		return api.ErrResourceAlreadyExists
	}
	m.functions.Store(f.Name, f)
	return nil
}

func (m *MockManager) Delete(ctx context.Context, name string) error {
	if _, ok := m.functions.Load(name); !ok {
		return api.ErrResourceNotFound
	}
	m.functions.Delete(name)
	return nil
}

func (m *MockManager) List() []*model.Function {
	var functions []*model.Function
	m.functions.Range(func(key, value interface{}) bool {
		functions = append(functions, value.(*model.Function))
		return true
	})
	return functions
}

func NewMockManager() *MockManager {
	return &MockManager{
		functions: sync.Map{},
	}
}

func TestRestAPI(t *testing.T) {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	log, err := config.Build()
	require.NoError(t, err)

	mockMgr := NewMockManager()
	h := &Handler{
		HttpListener: getListener(t),
		EnableTls:    false,
		Log:          log,
		Manager:      mockMgr,
	}

	go func() {
		_ = h.Run()
	}()
	defer h.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	<-h.WaitForReady(ctx)

	baseURL := "http://" + h.HttpListener.Addr().String() + "/apis/v1/function"

	t.Run("Deploy Function via HTTP", func(t *testing.T) {
		function := model.Function{
			Name:    "test-func",
			Package: "test-pkg",
			Module:  "test-module",
			Config:  map[string]string{"test-config": "test-value"},
		}

		body, _ := json.Marshal(function)
		resp, err := http.Post(baseURL+"/", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		v, ok := mockMgr.functions.Load(function.Name)
		require.True(t, ok)
		require.Equal(t, &function, v.(*model.Function))
	})

	t.Run("List Functions via HTTP", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var functions []*model.Function
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&functions))

		require.Len(t, functions, 1)
		require.Equal(t, "test-func", functions[0].Name)
	})

	t.Run("Delete Function via HTTP", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodDelete, baseURL+"/test-func", nil)
		require.NoError(t, err)

		client := &http.Client{}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		_, ok := mockMgr.functions.Load("test-func")
		require.False(t, ok)
	})

	t.Run("Delete Non-existent Function", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodDelete, baseURL+"/non-existent", nil)
		require.NoError(t, err)

		client := &http.Client{}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("Create Duplicate Function", func(t *testing.T) {
		function := model.Function{Name: "duplicate-func"}

		body, _ := json.Marshal(function)
		resp, _ := http.Post(baseURL+"/", "application/json", bytes.NewReader(body))
		resp.Body.Close()

		resp, err := http.Post(baseURL+"/", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusConflict, resp.StatusCode)
	})
}
