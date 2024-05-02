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
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/pkg/errors"
	"log/slog"
	"reflect"
)

type FunctionInstanceImpl struct {
	ctx        context.Context
	funcCtx    api.FunctionContext
	cancelFunc context.CancelFunc
	definition *model.Function
	readyCh    chan error
	index      int32
	parentLog  *slog.Logger
	log        *slog.Logger
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

func (f *DefaultInstanceFactory) NewFunctionInstance(definition *model.Function, funcCtx api.FunctionContext, index int32, logger *slog.Logger) api.FunctionInstance {
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
		parentLog:  logger,
		log:        logger.With(slog.String("component", "function-instance")),
	}
}

func (instance *FunctionInstanceImpl) Run(runtimeFactory api.FunctionRuntimeFactory, sources []<-chan contube.Record, sink chan<- contube.Record) {
	runtime, err := runtimeFactory.NewFunctionRuntime(instance)
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating runtime")
		return
	}
	defer close(sink)
	err = <-runtime.WaitForReady()
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error waiting for runtime to be ready")
		return
	}

	close(instance.readyCh)
	defer instance.log.InfoContext(instance.ctx, "function instance has been stopped")

	instance.log.Info("function instance is running")

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
		instance.log.DebugContext(instance.ctx, "Calling process function", slog.Any("count", logCounter))

		// Call the processing function
		output, err := runtime.Call(record)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			// Log the error if there's an issue with the processing function
			instance.log.ErrorContext(instance.ctx, "Error calling process function", slog.Any("error", err))
			return
		}

		// If the output is nil, continue with the next iteration
		if output == nil {
			instance.log.DebugContext(instance.ctx, "Output is nil")
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

func (instance *FunctionInstanceImpl) WaitForReady() <-chan error {
	return instance.readyCh
}

func (instance *FunctionInstanceImpl) Stop() {
	instance.log.InfoContext(instance.ctx, "stopping function instance")
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

func (instance *FunctionInstanceImpl) Logger() *slog.Logger {
	return instance.parentLog
}
