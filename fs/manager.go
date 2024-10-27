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
	"math/rand"
	"strconv"
	"sync"

	"github.com/functionstream/function-stream/fs/statestore"

	"github.com/functionstream/function-stream/common/config"
	_package "github.com/functionstream/function-stream/fs/package"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
)

type FunctionManager interface {
	StartFunction(f *model.Function) error
	DeleteFunction(namespace, name string) error
	ListFunctions() []string
	ProduceEvent(name string, event contube.Record) error
	ConsumeEvent(name string) (contube.Record, error)
	GetStateStore() (api.StateStore, error)
	Close() error
}

type functionManagerImpl struct {
	options       *managerOptions
	functions     map[common.NamespacedName][]api.FunctionInstance //TODO: Use sync.map
	functionsLock sync.Mutex
	log           *common.Logger
}

type managerOptions struct {
	tubeFactoryMap    map[string]contube.TubeFactory
	runtimeFactoryMap map[string]api.FunctionRuntimeFactory
	instanceFactory   api.FunctionInstanceFactory
	stateStoreFactory api.StateStoreFactory
	queueFactory      contube.TubeFactory
	packageLoader     api.PackageLoader // TODO: Need to set it
	log               *logr.Logger
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

func WithInstanceFactory(factory api.FunctionInstanceFactory) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.instanceFactory = factory
		return c, nil
	})
}

func WithStateStoreFactory(storeFactory api.StateStoreFactory) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.stateStoreFactory = storeFactory
		return c, nil
	})
}

func WithLogger(log *logr.Logger) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.log = log
		return c, nil
	})
}

func WithPackageLoader(loader api.PackageLoader) ManagerOption {
	return managerOptionFunc(func(c *managerOptions) (*managerOptions, error) {
		c.packageLoader = loader
		return c, nil
	})
}

func NewFunctionManager(opts ...ManagerOption) (FunctionManager, error) {
	options := &managerOptions{
		tubeFactoryMap:    make(map[string]contube.TubeFactory),
		runtimeFactoryMap: make(map[string]api.FunctionRuntimeFactory),
	}
	options.instanceFactory = NewDefaultInstanceFactory()
	for _, o := range opts {
		_, err := o.apply(options)
		if err != nil {
			return nil, err
		}
	}
	var log *common.Logger
	if options.log == nil {
		log = common.NewDefaultLogger()
	} else {
		log = common.NewLogger(options.log)
	}
	loadedRuntimeFact := make([]string, 0, len(options.runtimeFactoryMap))
	for k := range options.runtimeFactoryMap {
		loadedRuntimeFact = append(loadedRuntimeFact, k)
	}
	loadedTubeFact := make([]string, 0, len(options.tubeFactoryMap))
	for k := range options.tubeFactoryMap {
		loadedTubeFact = append(loadedTubeFact, k)
	}
	if options.packageLoader == nil {
		options.packageLoader = _package.NewDefaultPackageLoader()
	}
	if options.stateStoreFactory == nil {
		if fact, err := statestore.NewDefaultPebbleStateStoreFactory(); err != nil {
			return nil, err
		} else {
			options.stateStoreFactory = fact
		}
	}
	log.Info("Function manager created", "runtime-factories", loadedRuntimeFact,
		"tube-factories", loadedTubeFact)
	return &functionManagerImpl{
		options:   options,
		functions: make(map[common.NamespacedName][]api.FunctionInstance),
		log:       log,
	}, nil
}

func (fm *functionManagerImpl) getTubeFactory(tubeConfig *model.TubeConfig) (contube.TubeFactory, error) {
	factory, exist := fm.options.tubeFactoryMap[tubeConfig.Type]
	if !exist {
		return nil, fmt.Errorf("failed to get tube factory: %w, type: %s", common.ErrorTubeFactoryNotFound, tubeConfig.Type)
	}
	return factory, nil
}

func (fm *functionManagerImpl) getRuntimeFactory(t string) (api.FunctionRuntimeFactory, error) {
	factory, exist := fm.options.runtimeFactoryMap[t]
	if !exist {
		return nil, fmt.Errorf("failed to get runtime factory: %w, type: %s", common.ErrorRuntimeFactoryNotFound, t)
	}
	return factory, nil
}

func generateRuntimeConfig(ctx context.Context, p api.Package, f *model.Function) (*model.RuntimeConfig, error) {
	log := common.GetLogger(ctx)
	rc := &model.RuntimeConfig{}
	if p == _package.EmptyPackage {
		return &f.Runtime, nil
	}
	supportedRuntimeConf := p.GetSupportedRuntimeConfig()
	rcMap := map[string]*model.RuntimeConfig{}
	for k, v := range supportedRuntimeConf {
		if v.Type == "" {
			log.Warn("Package supported runtime type is empty. Ignore it.", "index", k, "package", f.Package)
			continue
		}
		vCopy := v
		rcMap[v.Type] = &vCopy
	}
	if len(rcMap) == 0 {
		return nil, common.ErrorPackageNoSupportedRuntime
	}
	defaultRC := &supportedRuntimeConf[0]
	if f.Runtime.Type == "" {
		rc.Type = defaultRC.Type
	} else {
		if r, exist := rcMap[f.Runtime.Type]; exist {
			defaultRC = r
		} else {
			return nil, fmt.Errorf("runtime type '%s' is not supported by package '%s'", f.Runtime.Type, f.Package)
		}
		rc.Type = f.Runtime.Type
	}
	rc.Config = config.MergeConfig(defaultRC.Config, f.Runtime.Config)
	return rc, nil
}

func (fm *functionManagerImpl) StartFunction(f *model.Function) error { // TODO: Shouldn't use pointer here
	if err := f.Validate(); err != nil {
		return err
	}
	fm.functionsLock.Lock()
	if _, exist := fm.functions[common.GetNamespacedName(f.Namespace, f.Name)]; exist {
		fm.functionsLock.Unlock()
		return common.ErrorFunctionExists
	}
	fm.functions[common.GetNamespacedName(f.Namespace, f.Name)] = make([]api.FunctionInstance, f.Replicas)
	fm.functionsLock.Unlock()

	for i := int32(0); i < f.Replicas; i++ {
		p, err := fm.options.packageLoader.Load(f.Package)
		if err != nil {
			return err
		}
		runtimeConfig, err := generateRuntimeConfig(context.Background(), p, f)
		if err != nil {
			return fmt.Errorf("failed to generate runtime config: %v", err)
		}

		store, err := fm.options.stateStoreFactory.NewStateStore(f)
		if err != nil {
			return fmt.Errorf("failed to create state store: %w", err)
		}

		funcCtx := newFuncCtxImpl(f, store)
		instanceLogger := fm.log.SubLogger("functionName", f.Name, "instanceIndex", int(i), "runtimeType", runtimeConfig.Type)
		instance := fm.options.instanceFactory.NewFunctionInstance(f, funcCtx, i, instanceLogger)
		fm.functionsLock.Lock()
		fm.functions[common.GetNamespacedName(f.Namespace, f.Name)][i] = instance
		fm.functionsLock.Unlock()
		runtimeFactory, err := fm.getRuntimeFactory(runtimeConfig.Type)
		if err != nil {
			return err
		}
		var sources []<-chan contube.Record
		for _, t := range f.Sources {
			sourceFactory, err := fm.getTubeFactory(&t)
			if err != nil {
				return err
			}
			sourceChan, err := sourceFactory.NewSourceTube(instance.Context(), t.Config)
			if err != nil {
				return fmt.Errorf("failed to create source event queue: %w", err)
			}
			sources = append(sources, sourceChan)
		}
		sinkFactory, err := fm.getTubeFactory(&f.Sink)
		if err != nil {
			return err
		}
		sink, err := sinkFactory.NewSinkTube(instance.Context(), f.Sink.Config)
		if err != nil {
			return fmt.Errorf("failed to create sink event queue: %w", err)
		}
		funcCtx.setSink(sink)

		runtime, err := runtimeFactory.NewFunctionRuntime(instance, runtimeConfig)
		if err != nil {
			return fmt.Errorf("failed to create runtime: %w", err)
		}
		fm.log.Info("Starting function instance", "function", f)

		go instance.Run(runtime, sources, sink)
	}
	return nil
}

func (fm *functionManagerImpl) DeleteFunction(namespace, name string) error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	instances, exist := fm.functions[common.GetNamespacedName(namespace, name)]
	if !exist {
		return common.ErrorFunctionNotFound
	}
	delete(fm.functions, common.GetNamespacedName(namespace, name))
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
func (fm *functionManagerImpl) GetStateStore() (api.StateStore, error) {
	return fm.options.stateStoreFactory.NewStateStore(nil)
}

func (fm *functionManagerImpl) Close() error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	log := common.NewDefaultLogger()
	for _, instances := range fm.functions {
		for _, instance := range instances {
			instance.Stop()
		}
	}
	if fm.options.stateStoreFactory != nil {
		if err := fm.options.stateStoreFactory.Close(); err != nil {
			log.Error(err, "failed to close state store")
		}
	}
	return nil
}
