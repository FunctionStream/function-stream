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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"sync"
)

type FunctionManager struct {
	functions     map[string]*FunctionInstance
	functionsLock sync.Mutex
	pc            pulsar.Client
}

func NewFunctionManager() (*FunctionManager, error) {
	pc, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: common.GetConfig().PulsarURL,
	})
	if err != nil {
		return nil, err
	}
	return &FunctionManager{
		functions: make(map[string]*FunctionInstance),
		pc:        pc,
	}, nil
}

func (fm *FunctionManager) StartFunction(f model.Function) error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	instance := NewFunctionInstance(f, fm.pc)
	fm.functions[f.Name] = instance
	go instance.Run()
	return instance.WaitForReady()
}

func (fm *FunctionManager) DeleteFunction(name string) error {
	fm.functionsLock.Lock()
	instance, exist := fm.functions[name]
	if !exist {
		return common.ErrorFunctionNotFound
	}
	delete(fm.functions, name)
	fm.functionsLock.Unlock()
	if instance != nil {
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
