package contube

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"testing"
)

func TestHttpTubeHandleRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := NewHttpTubeFactory(ctx).(*HttpTubeFactory)

	endpoint := "test"
	err := f.Handle(ctx, "test", []byte("test"))
	assert.ErrorIs(t, err, ErrEndpointNotFound)

	config := make(ConfigMap)
	config[EndpointKey] = endpoint
	source, err := f.NewSourceTube(ctx, config)
	assert.NoError(t, err)
	_, err = f.NewSourceTube(ctx, config)
	assert.ErrorIs(t, err, ErrorEndpointAlreadyExists)

	err = f.Handle(ctx, endpoint, []byte("test"))
	assert.Nil(t, err)

	record := <-source
	assert.Equal(t, "test", string(record.GetPayload()))

	cancel()

	assert.Nil(t, <-source)
	err = f.Handle(ctx, endpoint, []byte("test"))
	assert.Error(t, ErrEndpointNotFound)
}
