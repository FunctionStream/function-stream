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
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs/contube"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"log/slog"
)

type FunctionInstance interface {
}

type FunctionInstanceImpl struct {
	ctx         context.Context
	cancelFunc  context.CancelFunc
	definition  *model.Function
	tubeFactory contube.TubeFactory
	readyCh     chan error
	index       int32
}

func NewFunctionInstance(definition *model.Function, queueFactory contube.TubeFactory, index int32) *FunctionInstanceImpl {
	ctx, cancelFunc := context.WithCancel(context.Background())
	ctx.Value(logrus.Fields{
		"function-name":  definition.Name,
		"function-index": index,
	})
	return &FunctionInstanceImpl{
		ctx:         ctx,
		cancelFunc:  cancelFunc,
		definition:  definition,
		tubeFactory: queueFactory,
		readyCh:     make(chan error),
		index:       index,
	}
}

func handleErr(ctx context.Context, err error, message string, args ...interface{}) {
	if errors.Is(err, context.Canceled) {
		slog.InfoContext(ctx, "function instance has been stopped")
		return
	}
	extraArgs := append(args, slog.Error)
	slog.ErrorContext(ctx, message, extraArgs...)
}

func (instance *FunctionInstanceImpl) Run(runtimeFactory FunctionRuntimeFactory) {
	runtime, err := runtimeFactory.NewFunctionRuntime(instance)
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating runtime")
		return
	}

	sourceChan, err := instance.tubeFactory.NewSourceTube(instance.ctx, (&contube.SourceQueueConfig{Topics: instance.definition.Inputs, SubName: fmt.Sprintf("function-stream-%s", instance.definition.Name)}).ToConfigMap())
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating source event queue")
		return
	}
	sinkChan, err := instance.tubeFactory.NewSinkTube(instance.ctx, (&contube.SinkQueueConfig{Topic: instance.definition.Output}).ToConfigMap())
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
	for e := range sourceChan {
		output, err := runtime.Call(e)
		if err != nil {
			handleErr(instance.ctx, err, "Error calling process function")
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
	instance.cancelFunc()
}
