/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package contube

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestHttpTubeHandleRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := NewHttpTubeFactory(ctx)

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
	assert.Error(t, err, ErrEndpointNotFound)
}

func TestHttpTubeSinkTubeNotImplement(t *testing.T) {
	f := NewHttpTubeFactory(context.Background())
	_, err := f.NewSinkTube(context.Background(), make(ConfigMap))
	assert.ErrorIs(t, err, ErrSinkTubeNotImplemented)
}
