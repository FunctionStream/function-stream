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

package statestore_test

import (
	"context"
	"testing"

	"github.com/functionstream/function-stream/fsold/api"
	"github.com/functionstream/function-stream/fsold/statestore"
	"github.com/stretchr/testify/assert"
)

func TestPebbleStateStore(t *testing.T) {
	ctx := context.Background()
	storeFact, err := statestore.NewDefaultPebbleStateStoreFactory()
	assert.Nil(t, err)
	store, err := storeFact.NewStateStore(nil)
	assert.Nil(t, err)

	_, err = store.GetState(ctx, "key")
	assert.ErrorIs(t, err, api.ErrNotFound)

	err = store.PutState(ctx, "key", []byte("value"))
	assert.Nil(t, err)

	value, err := store.GetState(ctx, "key")
	assert.Nil(t, err)
	assert.Equal(t, "value", string(value))
}
