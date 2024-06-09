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

package server

import (
	"os"
	"testing"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type testFunctionManagerImpl struct {
	functions map[fs.NamespacedName]*model.Function
}

func (t *testFunctionManagerImpl) StartFunction(f *model.Function) error {
	t.functions[fs.GetNamespacedName(f.Namespace, f.Name)] = f
	return nil
}

func (t *testFunctionManagerImpl) DeleteFunction(namespace, name string) error {
	delete(t.functions, fs.GetNamespacedName(namespace, name))
	return nil
}

func (t *testFunctionManagerImpl) ListFunctions() []string {
	return nil
}

func (t *testFunctionManagerImpl) ProduceEvent(_ string, _ contube.Record) error {
	return nil
}

func (t *testFunctionManagerImpl) ConsumeEvent(_ string) (contube.Record, error) {
	return nil, nil
}

func (t *testFunctionManagerImpl) GetStateStore() api.StateStore {
	return nil
}

func (t *testFunctionManagerImpl) Close() error {
	return nil
}

func newTestFunctionManagerImpl() fs.FunctionManager {
	return &testFunctionManagerImpl{
		functions: make(map[fs.NamespacedName]*model.Function),
	}
}

func createTestFunction(name string) *model.Function {
	return &model.Function{
		Runtime: model.RuntimeConfig{
			Type: common.WASMRuntime,
			Config: map[string]interface{}{
				common.RuntimeArchiveConfigKey: "../bin/example_basic.wasm",
			},
		},
		Sources: []model.TubeConfig{
			{
				Type: common.MemoryTubeType,
				Config: (&contube.SourceQueueConfig{
					Topics:  []string{"input"},
					SubName: "test",
				}).ToConfigMap(),
			},
		},
		Sink: model.TubeConfig{
			Type: common.MemoryTubeType,
			Config: (&contube.SinkQueueConfig{
				Topic: "output",
			}).ToConfigMap(),
		},
		Name:     name,
		Replicas: 1,
		Config:   map[string]string{},
	}
}

const yamlSeparator string = "---\n"

func TestFunctionStoreLoading(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "*.yaml")
	assert.Nil(t, err)
	//defer os.Remove(tmpfile.Name())

	fm := newTestFunctionManagerImpl()
	functionStore, err := NewFunctionStoreImpl(fm, tmpfile.Name())
	assert.Nil(t, err)

	f1 := createTestFunction("f1")

	f1Data, err := yaml.Marshal(&f1)
	assert.Nil(t, err)

	_, err = tmpfile.Write(f1Data)
	assert.Nil(t, err)

	assert.Nil(t, functionStore.Load())

	assert.Len(t, fm.(*testFunctionManagerImpl).functions, 1)
	assert.Equal(t, f1, fm.(*testFunctionManagerImpl).functions[fs.GetNamespacedName("", "f1")])

	f2 := createTestFunction("f2")
	_, err = tmpfile.WriteString(yamlSeparator)
	assert.Nil(t, err)
	f2Data, err := yaml.Marshal(f2)
	assert.Nil(t, err)
	_, err = tmpfile.Write(f2Data)
	assert.Nil(t, err)

	assert.Nil(t, functionStore.Load())
	assert.Len(t, fm.(*testFunctionManagerImpl).functions, 2)
	assert.Equal(t, f1, fm.(*testFunctionManagerImpl).functions[fs.GetNamespacedName("", "f1")])
	assert.Equal(t, f2, fm.(*testFunctionManagerImpl).functions[fs.GetNamespacedName("", "f2")])

	assert.Nil(t, tmpfile.Close())

	tmpfile, err = os.Create(tmpfile.Name()) // Overwrite the file
	assert.Nil(t, err)

	_, err = tmpfile.Write(f2Data)
	assert.Nil(t, err)

	assert.Nil(t, functionStore.Load())
	assert.Len(t, fm.(*testFunctionManagerImpl).functions, 1)
	assert.Equal(t, f2, fm.(*testFunctionManagerImpl).functions[fs.GetNamespacedName("", "f2")])
}
