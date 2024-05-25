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
	"log/slog"
	"math/rand"
	"strconv"
	"sync"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/fs/runtime/wazero"
	"github.com/functionstream/function-stream/fs/statestore"
	"github.com/pkg/errors"
)

type NamespacedName struct {
	namespace string
	name      string
}

func (n NamespacedName) String() string {
	if n.namespace == "" {
		return n.name
	}
	return n.namespace + "/" + n.name
}

func GetNamespacedName(namespace, name string) NamespacedName {
	return NamespacedName{
		namespace: namespace,
		name:      name,
	}
}

type FunctionManager interface {
	StartFunction(f *model.Function) error
	DeleteFunction(namespace, name string) error
	ListFunctions() []string
	ProduceEvent(name string, event contube.Record) error
	ConsumeEvent(name string) (contube.Record, error)
	GetStateStore() api.StateStore
	Close() error
}

type functionManagerImpl struct {
	options       *managerOptions
	functions     map[NamespacedName][]api.FunctionInstance //TODO: Use sync.map
	functionsLock sync.Mutex
	log           *slog.Logger
}

type managerOptions struct {
	tubeFactoryMap           map[string]contube.TubeFactory
	runtimeFactoryMap        map[string]api.FunctionRuntimeFactory
	instanceFactory          api.FunctionInstanceFactory
	stateStore               api.StateStore
	dontUseDefaultStateStore bool
	queueFactory             contube.TubeFactory
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

func WithQueueFactory(factory contube.TubeFactory) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.queueFactory = factory
		return c, nil
	})
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

func WithStateStore(store api.StateStore) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.dontUseDefaultStateStore = true
		c.stateStore = store
		return c, nil
	})
}

func NewFunctionManager(opts ...ManagerOption) (FunctionManager, error) {
	options := &managerOptions{
		tubeFactoryMap:    make(map[string]contube.TubeFactory),
		runtimeFactoryMap: make(map[string]api.FunctionRuntimeFactory),
	}
	options.runtimeFactoryMap["default"] = wazero.NewWazeroFunctionRuntimeFactory()
	options.instanceFactory = NewDefaultInstanceFactory()
	for _, o := range opts {
		_, err := o.apply(options)
		if err != nil {
			return nil, err
		}
	}
	if !options.dontUseDefaultStateStore {
		var err error
		options.stateStore, err = statestore.NewTmpPebbleStateStore()
		if err != nil {
			return nil, errors.Wrap(err, "failed to create default state store")
		}
	}
	log := slog.With()
	loadedRuntimeFact := make([]string, 0, len(options.runtimeFactoryMap))
	for k := range options.runtimeFactoryMap {
		loadedRuntimeFact = append(loadedRuntimeFact, k)
	}
	loadedTubeFact := make([]string, 0, len(options.tubeFactoryMap))
	for k := range options.tubeFactoryMap {
		loadedTubeFact = append(loadedTubeFact, k)
	}
	log.Info("Function manager created", slog.Any("runtime-factories", loadedRuntimeFact),
		slog.Any("tube-factories", loadedTubeFact))
	return &functionManagerImpl{
		options:   options,
		functions: make(map[NamespacedName][]api.FunctionInstance),
		log:       log,
	}, nil
}

func (fm *functionManagerImpl) getTubeFactory(tubeConfig *model.TubeConfig) (contube.TubeFactory, error) {
	factory, exist := fm.options.tubeFactoryMap[tubeConfig.Type]
	if !exist {
		fm.log.ErrorContext(context.Background(), "tube factory not found", "type", tubeConfig.Type)
		return nil, common.ErrorTubeFactoryNotFound
	}
	return factory, nil
}

func (fm *functionManagerImpl) getRuntimeType(runtimeConfig *model.RuntimeConfig) string {
	if runtimeConfig == nil || runtimeConfig.Type == nil {
		return "default"
	}
	return *runtimeConfig.Type
}

func (fm *functionManagerImpl) getRuntimeFactory(t string) (api.FunctionRuntimeFactory, error) {
	factory, exist := fm.options.runtimeFactoryMap[t]
	if !exist {
		fm.log.ErrorContext(context.Background(), "runtime factory not found", "type", t)
		return nil, common.ErrorRuntimeFactoryNotFound
	}
	return factory, nil
}

func (fm *functionManagerImpl) createFuncCtx() api.FunctionContext {
	return NewFuncCtxImpl(fm.options.stateStore)
}

func (fm *functionManagerImpl) StartFunction(f *model.Function) error { // TODO: Shouldn't use pointer here
	if err := f.Validate(); err != nil {
		return err
	}
	fm.functionsLock.Lock()
	if _, exist := fm.functions[GetNamespacedName(f.Namespace, f.Name)]; exist {
		fm.functionsLock.Unlock()
		return common.ErrorFunctionExists
	}
	fm.functions[GetNamespacedName(f.Namespace, f.Name)] = make([]api.FunctionInstance, f.Replicas)
	fm.functionsLock.Unlock()
	funcCtx := fm.createFuncCtx()
	for i := int32(0); i < f.Replicas; i++ {
		runtimeType := fm.getRuntimeType(f.Runtime)

		instance := fm.options.instanceFactory.NewFunctionInstance(f, funcCtx, i, slog.With(
			slog.String("name", f.Name),
			slog.Int("index", int(i)),
			slog.String("runtime", runtimeType),
		))
		fm.functionsLock.Lock()
		fm.functions[GetNamespacedName(f.Namespace, f.Name)][i] = instance
		fm.functionsLock.Unlock()
		runtimeFactory, err := fm.getRuntimeFactory(runtimeType)
		if err != nil {
			return err
		}
		var sources []<-chan contube.Record
		for _, t := range f.Sources {
			sourceFactory, err := fm.getTubeFactory(&t)
			if err != nil {
				return nil
			}
			sourceChan, err := sourceFactory.NewSourceTube(instance.Context(), t.Config)
			if err != nil {
				return errors.Wrap(err, "Error creating source event queue")
			}
			sources = append(sources, sourceChan)
		}
		sinkFactory, err := fm.getTubeFactory(&f.Sink)
		if err != nil {
			return nil
		}
		sink, err := sinkFactory.NewSinkTube(instance.Context(), f.Sink.Config)
		if err != nil {
			return errors.Wrap(err, "Error creating sink event queue")
		}

		go instance.Run(runtimeFactory, sources, sink)
		select {
		case err := <-instance.WaitForReady():
			if err != nil {
				fm.log.ErrorContext(instance.Context(), "Error starting function instance", slog.Any("error", err.Error()))
				instance.Stop()
				return err
			}
		case <-instance.Context().Done():
			fm.log.ErrorContext(instance.Context(), "Error starting function instance", slog.Any("error", "context cancelled"))
			return errors.New("context cancelled")
		}
	}
	return nil
}

func (fm *functionManagerImpl) DeleteFunction(namespace, name string) error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	instances, exist := fm.functions[GetNamespacedName(namespace, name)]
	if !exist {
		return common.ErrorFunctionNotFound
	}
	delete(fm.functions, GetNamespacedName(namespace, name))
	for _, instance := range instances {
		instance.Stop()
	}
	return nil
}

func (fm *functionManagerImpl) ListFunctions() (result []string) {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	result = make([]string, len(fm.functions))
	i := 0
	for k := range fm.functions {
		result[i] = k.String()
		i++
	}
	return
}

func (fm *functionManagerImpl) ProduceEvent(name string, event contube.Record) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory, ok := fm.options.tubeFactoryMap[common.MemoryTubeType]
	if !ok {
		return errors.New("memory tube factory not found")
	}
	c, err := factory.NewSinkTube(ctx, (&contube.SinkQueueConfig{Topic: name}).ToConfigMap())
	if err != nil {
		return err
	}
	c <- event
	return nil
}

func (fm *functionManagerImpl) ConsumeEvent(name string) (contube.Record, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory, ok := fm.options.tubeFactoryMap[common.MemoryTubeType]
	if !ok {
		return nil, errors.New("memory tube factory not found")
	}
	c, err := factory.NewSourceTube(ctx, (&contube.SourceQueueConfig{
		Topics: []string{name}, SubName: "consume-" + strconv.Itoa(rand.Int())}).ToConfigMap())
	if err != nil {
		return nil, err
	}
	return <-c, nil
}

// GetStateStore returns the state store used by the function manager
// Return nil if no state store is configured
func (fm *functionManagerImpl) GetStateStore() api.StateStore {
	return fm.options.stateStore
}

func (fm *functionManagerImpl) Close() error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	for _, instances := range fm.functions {
		for _, instance := range instances {
			instance.Stop()
		}
	}
	err := fm.options.stateStore.Close()
	if err != nil {
		return err
	}
	return nil
}
