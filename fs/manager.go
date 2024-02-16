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
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs/api"
	"github.com/functionstream/functionstream/fs/contube"
	"github.com/functionstream/functionstream/fs/runtime/wazero"
	"github.com/pkg/errors"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
)

type FunctionManager struct {
	options       *managerOptions
	functions     map[string][]api.FunctionInstance //TODO: Use sync.map
	functionsLock sync.Mutex
}

type managerOptions struct {
	tubeFactoryMap    map[string]contube.TubeFactory
	runtimeFactoryMap map[string]api.FunctionRuntimeFactory
	instanceFactory   api.FunctionInstanceFactory
}

type ManagerOption interface {
	apply(option *managerOptions) (*managerOptions, error)
}

type managerOptionFunc func(*managerOptions) (*managerOptions, error)

func (f managerOptionFunc) apply(c *managerOptions) (*managerOptions, error) {
	return f(c)
}

func WithTubeFactory(name string, factory contube.TubeFactory) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.tubeFactoryMap[name] = factory
		return c, nil
	})
}

func WithDefaultTubeFactory(factory contube.TubeFactory) ManagerOption {
	return WithTubeFactory("default", factory)
}

func WithRuntimeFactory(name string, factory api.FunctionRuntimeFactory) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.runtimeFactoryMap[name] = factory
		return c, nil
	})
}

func WithDefaultRuntimeFactory(factory api.FunctionRuntimeFactory) ManagerOption {
	return WithRuntimeFactory("default", factory)
}

func WithInstanceFactory(factory api.FunctionInstanceFactory) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.instanceFactory = factory
		return c, nil
	})
}

func NewFunctionManager(opts ...ManagerOption) (*FunctionManager, error) {
	options := &managerOptions{
		tubeFactoryMap:    make(map[string]contube.TubeFactory),
		runtimeFactoryMap: make(map[string]api.FunctionRuntimeFactory),
	}
	options.runtimeFactoryMap["default"] = wazero.NewWazeroFunctionRuntimeFactory()
	options.tubeFactoryMap["default"] = contube.NewMemoryQueueFactory(context.Background())
	options.instanceFactory = NewDefaultInstanceFactory()
	for _, o := range opts {
		_, err := o.apply(options)
		if err != nil {
			return nil, err
		}
	}
	return &FunctionManager{
		options:   options,
		functions: make(map[string][]api.FunctionInstance),
	}, nil
}

func (fm *FunctionManager) getTubeFactory(tubeConfig *model.TubeConfig) (contube.TubeFactory, error) {
	get := func(t string) (contube.TubeFactory, error) {
		factory, exist := fm.options.tubeFactoryMap[t]
		if !exist {
			slog.ErrorContext(context.Background(), "tube factory not found", "type", t)
			return nil, common.ErrorTubeFactoryNotFound
		}
		return factory, nil

	}
	if tubeConfig == nil || tubeConfig.Type == nil {
		return get("default")
	}
	return get(*tubeConfig.Type)
}

func (fm *FunctionManager) getRuntimeFactory(runtimeConfig *model.RuntimeConfig) (api.FunctionRuntimeFactory, error) {
	get := func(t string) (api.FunctionRuntimeFactory, error) {
		factory, exist := fm.options.runtimeFactoryMap[t]
		if !exist {
			slog.ErrorContext(context.Background(), "runtime factory not found", "type", t)
			return nil, common.ErrorRuntimeFactoryNotFound
		}
		return factory, nil

	}
	if runtimeConfig == nil || runtimeConfig.Type == nil {
		return get("default")
	}
	return get(*runtimeConfig.Type)
}

func (fm *FunctionManager) StartFunction(f *model.Function) error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock() // TODO: narrow the lock scope
	if _, exist := fm.functions[f.Name]; exist {
		return common.ErrorFunctionExists
	}
	fm.functions[f.Name] = make([]api.FunctionInstance, f.Replicas)
	if f.Replicas <= 0 {
		return errors.New("replicas should be greater than 0")
	}
	for i := int32(0); i < f.Replicas; i++ {
		sourceFactory, err := fm.getTubeFactory(f.Source)
		if err != nil {
			return err
		}
		sinkFactory, err := fm.getTubeFactory(f.Sink)
		if err != nil {
			return err
		}

		instance := fm.options.instanceFactory.NewFunctionInstance(f, sourceFactory, sinkFactory, i)
		fm.functions[f.Name][i] = instance
		runtimeFactory, err := fm.getRuntimeFactory(f.Runtime)
		if err != nil {
			return err
		}
		go instance.Run(runtimeFactory)
		if err := <-instance.WaitForReady(); err != nil {
			if err != nil {
				slog.ErrorContext(instance.Context(), "Error starting function instance", slog.Any("error", err.Error()))
			}
			instance.Stop()
			return err
		}
	}
	return nil
}

func (fm *FunctionManager) DeleteFunction(name string) error {
	fm.functionsLock.Lock()
	instances, exist := fm.functions[name]
	if !exist {
		return common.ErrorFunctionNotFound
	}
	delete(fm.functions, name)
	fm.functionsLock.Unlock()
	for _, instance := range instances {
		instance.Stop()
	}
	return nil
}

func (fm *FunctionManager) ListFunctions() (result []string) {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	result = make([]string, len(fm.functions))
	i := 0
	for k := range fm.functions {
		result[i] = k
		i++
	}
	return
}

func (fm *FunctionManager) ProduceEvent(name string, event contube.Record) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := fm.options.tubeFactoryMap["default"].NewSinkTube(ctx, (&contube.SinkQueueConfig{Topic: name}).ToConfigMap())
	if err != nil {
		return err
	}
	c <- event
	return nil
}

func (fm *FunctionManager) ConsumeEvent(name string) (contube.Record, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := fm.options.tubeFactoryMap["default"].NewSourceTube(ctx, (&contube.SourceQueueConfig{Topics: []string{name}, SubName: "consume-" + strconv.Itoa(rand.Int())}).ToConfigMap())
	if err != nil {
		return nil, err
	}
	return <-c, nil
}
