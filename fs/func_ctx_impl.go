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

package fs

import (
	"context"

	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/pkg/errors"
)

var ErrStateStoreNotLoaded = errors.New("state store not loaded")

type funcCtxImpl struct {
	api.FunctionContext
	stateStore api.StateStore
	sink       chan<- contube.Record
}

func newFuncCtxImpl(store api.StateStore) *funcCtxImpl {
	return &funcCtxImpl{stateStore: store}
}

func (f *funcCtxImpl) checkStateStore() error {
	if f.stateStore == nil {
		return ErrStateStoreNotLoaded
	}
	return nil
}

func (f *funcCtxImpl) PutState(ctx context.Context, key string, value []byte) error {
	if err := f.checkStateStore(); err != nil {
		return err
	}
	return f.stateStore.PutState(ctx, key, value)
}

func (f *funcCtxImpl) GetState(ctx context.Context, key string) ([]byte, error) {
	if err := f.checkStateStore(); err != nil {
		return nil, err
	}
	return f.stateStore.GetState(ctx, key)
}

func (f *funcCtxImpl) ListStates(ctx context.Context, startInclusive string, endExclusive string) ([]string, error) {
	if err := f.checkStateStore(); err != nil {
		return nil, err
	}
	return f.stateStore.ListStates(ctx, startInclusive, endExclusive)
}

func (f *funcCtxImpl) DeleteState(ctx context.Context, key string) error {
	if err := f.checkStateStore(); err != nil {
		return err
	}
	return f.stateStore.DeleteState(ctx, key)
}

func (f *funcCtxImpl) Write(record contube.Record) error {
	if f.sink == nil {
		return errors.New("sink not set")
	}
	f.sink <- record
	return nil
}

func (f *funcCtxImpl) setSink(sink chan<- contube.Record) {
	f.sink = sink
}
