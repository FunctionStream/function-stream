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
	"fmt"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/pkg/errors"
	"log/slog"
)

type FunctionInstanceImpl struct {
	api.FunctionInstance
	ctx           context.Context
	cancelFunc    context.CancelFunc
	definition    *model.Function
	sourceFactory contube.SourceTubeFactory
	sinkFactory   contube.SinkTubeFactory
	readyCh       chan error
	index         int32
	parentLog     *slog.Logger
	log           *slog.Logger
}

type CtxKey string

const (
	CtxKeyFunctionName  CtxKey = "function-name"
	CtxKeyInstanceIndex CtxKey = "instance-index"
)

type DefaultInstanceFactory struct{}

func NewDefaultInstanceFactory() api.FunctionInstanceFactory {
	return &DefaultInstanceFactory{}
}

func (f *DefaultInstanceFactory) NewFunctionInstance(definition *model.Function, sourceFactory contube.SourceTubeFactory, sinkFactory contube.SinkTubeFactory, index int32, logger *slog.Logger) api.FunctionInstance {
	ctx, cancelFunc := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, CtxKeyFunctionName, definition.Name)
	ctx = context.WithValue(ctx, CtxKeyInstanceIndex, index)
	return &FunctionInstanceImpl{
		ctx:           ctx,
		cancelFunc:    cancelFunc,
		definition:    definition,
		sourceFactory: sourceFactory,
		sinkFactory:   sinkFactory,
		readyCh:       make(chan error),
		index:         index,
		parentLog:     logger,
		log:           logger.With(slog.String("component", "function-instance")),
	}
}

func (instance *FunctionInstanceImpl) Run(runtimeFactory api.FunctionRuntimeFactory) {
	runtime, err := runtimeFactory.NewFunctionRuntime(instance)
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating runtime")
		return
	}
	getTubeConfig := func(config contube.ConfigMap, tubeConfig *model.TubeConfig) contube.ConfigMap {
		if tubeConfig != nil && tubeConfig.Config != nil {
			return contube.Merge(config, tubeConfig.Config)
		}
		return config
	}
	sourceConfig := (&contube.SourceQueueConfig{Topics: instance.definition.Inputs, SubName: fmt.Sprintf("function-stream-%s", instance.definition.Name)}).ToConfigMap()
	sourceChan, err := instance.sourceFactory.NewSourceTube(instance.ctx, getTubeConfig(sourceConfig, instance.definition.Source))
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating source event queue")
		return
	}
	sinkConfig := (&contube.SinkQueueConfig{Topic: instance.definition.Output}).ToConfigMap()
	sinkChan, err := instance.sinkFactory.NewSinkTube(instance.ctx, getTubeConfig(sinkConfig, instance.definition.Sink))
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating sink event queue")
		return
	}
	defer close(sinkChan)
	err = <-runtime.WaitForReady()
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error waiting for runtime to be ready")
		return
	}

	close(instance.readyCh)
	defer instance.log.InfoContext(instance.ctx, "function instance has been stopped")
	logCounter := common.LogCounter()
	for e := range sourceChan {
		instance.log.DebugContext(instance.ctx, "calling process function", slog.Any("count", logCounter))
		output, err := runtime.Call(e)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			instance.log.ErrorContext(instance.ctx, "Error calling process function", slog.Any("error", err))
			return
		}
		select {
		case sinkChan <- output:
		case <-instance.ctx.Done():
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

func (instance *FunctionInstanceImpl) Definition() *model.Function {
	return instance.definition
}

func (instance *FunctionInstanceImpl) Index() int32 {
	return instance.index
}

func (instance *FunctionInstanceImpl) Logger() *slog.Logger {
	return instance.parentLog
}
