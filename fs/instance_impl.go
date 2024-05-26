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
	"reflect"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/pkg/errors"
)

type FunctionInstanceImpl struct {
	ctx        context.Context
	funcCtx    api.FunctionContext
	cancelFunc context.CancelFunc
	definition *model.Function
	readyCh    chan error
	index      int32
	logger     *common.Logger
}

type CtxKey string

const (
	CtxKeyFunctionName  CtxKey = "function-name"
	CtxKeyInstanceIndex CtxKey = "instance-index"
)

type DefaultInstanceFactory struct {
	api.FunctionInstanceFactory
}

func NewDefaultInstanceFactory() api.FunctionInstanceFactory {
	return &DefaultInstanceFactory{}
}

func (f *DefaultInstanceFactory) NewFunctionInstance(definition *model.Function, funcCtx api.FunctionContext,
	index int32, logger *common.Logger) api.FunctionInstance {
	ctx, cancelFunc := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, CtxKeyFunctionName, definition.Name)
	ctx = context.WithValue(ctx, CtxKeyInstanceIndex, index)
	return &FunctionInstanceImpl{
		ctx:        ctx,
		funcCtx:    funcCtx,
		cancelFunc: cancelFunc,
		definition: definition,
		readyCh:    make(chan error),
		index:      index,
		logger:     logger,
	}
}

func (instance *FunctionInstanceImpl) Run(runtime api.FunctionRuntime, sources []<-chan contube.Record,
	sink chan<- contube.Record) {
	logger := instance.logger
	defer close(sink)

	defer logger.Info("function instance has been stopped")

	logger.Info("function instance is running")

	logCounter := common.LogCounter()
	channels := make([]reflect.SelectCase, len(sources)+1)
	for i, s := range sources {
		channels[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s)}
	}
	channels[len(sources)] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(instance.ctx.Done())}

	for len(channels) > 0 {
		// Use reflect.Select to select a channel from the slice
		chosen, value, ok := reflect.Select(channels)
		if !ok {
			// The selected channel has been closed, remove it from the slice
			channels = append(channels[:chosen], channels[chosen+1:]...)
			continue
		}

		// Convert the selected value to the type Record
		record := value.Interface().(contube.Record)
		if logger.DebugEnabled() {
			logger.Debug("Calling process function", "count", logCounter)
		}

		// Call the processing function
		output, err := runtime.Call(record)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			// Log the error if there's an issue with the processing function
			logger.Error(err, "failed to process record")
			return
		}

		// If the output is nil, continue with the next iteration
		if output == nil {
			continue
		}

		// Try to send the output to the sink, but also listen to the context's Done channel
		select {
		case sink <- output:
		case <-instance.ctx.Done():
			return
		}

		// If the selected channel is the context's Done channel, exit the loop
		if chosen == len(channels)-1 {
			return
		}
	}
}

func (instance *FunctionInstanceImpl) Stop() {
	instance.logger.Info("stopping function instance")
	instance.cancelFunc()
}

func (instance *FunctionInstanceImpl) Context() context.Context {
	return instance.ctx
}

func (instance *FunctionInstanceImpl) FunctionContext() api.FunctionContext {
	return instance.funcCtx
}

func (instance *FunctionInstanceImpl) Definition() *model.Function {
	return instance.definition
}

func (instance *FunctionInstanceImpl) Index() int32 {
	return instance.index
}

func (instance *FunctionInstanceImpl) Logger() *common.Logger {
	return instance.logger
}
