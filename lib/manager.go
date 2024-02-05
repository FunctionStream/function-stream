/*
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

package lib

import (
	"context"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"log/slog"
	"sync"
)

type FunctionManager struct {
	functions         map[string][]*FunctionInstance
	functionsLock     sync.Mutex
	eventQueueFactory EventQueueFactory
}

func NewFunctionManager() (*FunctionManager, error) {
	eventQueueFactory, err := NewPulsarEventQueueFactory(context.Background())
	if err != nil {
		return nil, err
	}
	return &FunctionManager{
		functions:         make(map[string][]*FunctionInstance),
		eventQueueFactory: eventQueueFactory,
	}, nil
}

func (fm *FunctionManager) StartFunction(f *model.Function) error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	if _, exist := fm.functions[f.Name]; exist {
		fm.functionsLock.Unlock()
		return common.ErrorFunctionExists
	}
	fm.functions[f.Name] = make([]*FunctionInstance, f.Replicas)
	for i := int32(0); i < f.Replicas; i++ {
		instance := NewFunctionInstance(f, fm.eventQueueFactory, i)
		fm.functions[f.Name][i] = instance
		go instance.Run()
		if err := <-instance.WaitForReady(); err != nil {
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error starting function instance", err)
			}
			fm.functionsLock.Unlock()
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
	for k := range fm.functions {
		result = append(result, k)
	}
	return
}
