package memory

import (
	"context"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMemoryStateStore(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()
	_, err := s.Get(ctx, "key")
	require.Error(t, err, api.ErrStateNotFound)
	err = s.Put(ctx, "key", []byte("value"))
	require.Nil(t, err)
	value, err := s.Get(ctx, "key")
	require.Nil(t, err)
	require.Equal(t, "value", string(value))
}
