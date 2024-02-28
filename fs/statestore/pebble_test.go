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

package statestore

import (
	"github.com/functionstream/function-stream/fs/api"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"os"
	"testing"
)

func TestPebbleStateStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	assert.Nil(t, err)
	store, err := NewPebbleStateStore(&PebbleStateStoreConfig{DirName: dir}, slog.Default())
	assert.Nil(t, err)

	_, err = store.GetState("key")
	assert.ErrorIs(t, err, api.ErrNotFound)

	err = store.PutState("key", []byte("value"))
	assert.Nil(t, err)

	value, err := store.GetState("key")
	assert.Nil(t, err)
	assert.Equal(t, "value", string(value))
}
