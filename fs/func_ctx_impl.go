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
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/pkg/errors"
)

var ErrStateStoreNotLoaded = errors.New("state store not loaded")

type funcCtxImpl struct {
	api.FunctionContext
	store        api.StateStore
	putStateFunc func(key string, value []byte) error
	getStateFunc func(key string) ([]byte, error)
	sink         chan<- contube.Record
}

func newFuncCtxImpl(store api.StateStore) *funcCtxImpl {
	putStateFunc := func(key string, value []byte) error {
		return ErrStateStoreNotLoaded
	}
	getStateFunc := func(key string) ([]byte, error) {
		return nil, ErrStateStoreNotLoaded
	}
	if store != nil {
		putStateFunc = store.PutState
		getStateFunc = store.GetState
	}
	return &funcCtxImpl{store: store,
		putStateFunc: putStateFunc,
		getStateFunc: getStateFunc}
}

func (f *funcCtxImpl) PutState(key string, value []byte) error {
	return f.putStateFunc(key, value)
}

func (f *funcCtxImpl) GetState(key string) ([]byte, error) {
	return f.getStateFunc(key)
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
