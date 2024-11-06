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

package external

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/functionstream/function-stream/fs/statestore"

	"github.com/functionstream/function-stream/clients/gofs"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/stretchr/testify/assert"
)

type Person struct {
	Name     string `json:"name"`
	Money    int    `json:"money"`
	Expected int    `json:"expected"`
}

type Counter struct {
	Count int `json:"count"`
}

type testRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var log = common.NewDefaultLogger()

type TestFunction struct {
}

func (f *TestFunction) Init(_ gofs.FunctionContext) error {
	return nil
}

func (f *TestFunction) Handle(_ gofs.FunctionContext, event gofs.Event[Person]) (gofs.Event[Person], error) {
	p := event.Data()
	p.Money += 1
	return gofs.NewEvent(p), nil
}

type TestCounterFunction struct {
}

func (f *TestCounterFunction) Init(ctx gofs.FunctionContext) error {
	return nil
}

func (f *TestCounterFunction) Handle(_ gofs.FunctionContext, event gofs.Event[Counter]) (gofs.Event[Counter], error) {
	c := event.Data()
	c.Count += 1
	return gofs.NewEvent(c), nil
}

type TestSource struct {
}

func (f *TestSource) Init(_ gofs.FunctionContext) error {
	return nil
}

func (f *TestSource) Handle(_ gofs.FunctionContext, emit func(context.Context, gofs.Event[testRecord]) error) error {
	for i := 0; i < 10; i++ {
		err := emit(context.Background(), gofs.NewEvent(&testRecord{
			ID:   i,
			Name: "test",
		}))
		if err != nil {
			log.Error(err, "failed to emit record")
		}
	}
	return nil
}

func runMockClient() {
	err := gofs.NewFSClient().
		Register(gofs.DefaultModule, gofs.WithFunction(&TestFunction{})).
		Register("counter", gofs.WithFunction(&TestCounterFunction{})).
		Register("test-source", gofs.WithSource(&TestSource{})).
		Run()
	if err != nil {
		log.Error(err, "failed to run mock client")
	}
}

//nolint:goconst
func TestExternalRuntime(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	assert.NoError(t, os.RemoveAll(testSocketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	lis, err := net.Listen("unix", testSocketPath)
	assert.NoError(t, err)
	defer func(lis net.Listener) {
		_ = lis.Close()
	}(lis)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("external", NewFactory(lis)),
		fs.WithTubeFactory("memory", contube.NewMemoryQueueFactory(context.Background())),
	)
	if err != nil {
		t.Fatal(err)
	}

	go runMockClient()

	inputTopic := "input"
	outputTopic := "output"
	f := &model.Function{
		Name: "test",
		Runtime: model.RuntimeConfig{
			Type: "external",
		},
		Sources: []model.TubeConfig{
			{
				Type: common.MemoryTubeType,
				Config: (&contube.SourceQueueConfig{
					Topics:  []string{inputTopic},
					SubName: "test",
				}).ToConfigMap(),
			},
		},
		Sink: model.TubeConfig{
			Type: common.MemoryTubeType,
			Config: (&contube.SinkQueueConfig{
				Topic: outputTopic,
			}).ToConfigMap(),
		},
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	assert.NoError(t, err)

	event, err := contube.NewStructRecord(&Person{
		Name:  "test",
		Money: 1,
	}, func() {})
	assert.NoError(t, err)
	err = fm.ProduceEvent(inputTopic, event)
	assert.NoError(t, err)
	output, err := fm.ConsumeEvent(outputTopic)
	assert.NoError(t, err)

	p := &Person{}
	err = json.Unmarshal(output.GetPayload(), &p)
	assert.NoError(t, err)
	assert.Equal(t, 2, p.Money)

	err = fm.DeleteFunction("", f.Name)
	assert.NoError(t, err)
}

func TestNonDefaultModule(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	assert.NoError(t, os.RemoveAll(testSocketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	assert.NoError(t, os.Setenv("FS_MODULE_NAME", "counter"))
	lis, err := net.Listen("unix", testSocketPath)
	assert.NoError(t, err)
	defer func(lis net.Listener) {
		_ = lis.Close()
	}(lis)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("external", NewFactory(lis)),
		fs.WithTubeFactory("memory", contube.NewMemoryQueueFactory(context.Background())),
	)
	if err != nil {
		t.Fatal(err)
	}

	inputTopic := "input"
	outputTopic := "output"
	f := &model.Function{
		Name: "test",
		Runtime: model.RuntimeConfig{
			Type: "external",
		},
		Module: "counter",
		Sources: []model.TubeConfig{
			{
				Type: common.MemoryTubeType,
				Config: (&contube.SourceQueueConfig{
					Topics:  []string{inputTopic},
					SubName: "test",
				}).ToConfigMap(),
			},
		},
		Sink: model.TubeConfig{
			Type: common.MemoryTubeType,
			Config: (&contube.SinkQueueConfig{
				Topic: outputTopic,
			}).ToConfigMap(),
		},
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	assert.NoError(t, err)

	go runMockClient()

	event, err := contube.NewStructRecord(&Counter{
		Count: 1,
	}, func() {})
	assert.NoError(t, err)
	err = fm.ProduceEvent(inputTopic, event)
	assert.NoError(t, err)
	output, err := fm.ConsumeEvent(outputTopic)
	assert.NoError(t, err)

	c := &Counter{}
	err = json.Unmarshal(output.GetPayload(), &c)
	assert.NoError(t, err)
	assert.Equal(t, 2, c.Count)

	err = fm.DeleteFunction("", f.Name)
	assert.NoError(t, err)
}

func TestExternalSourceModule(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	assert.NoError(t, os.RemoveAll(testSocketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	assert.NoError(t, os.Setenv("FS_MODULE_NAME", "test-source"))
	lis, err := net.Listen("unix", testSocketPath)
	assert.NoError(t, err)
	defer func(lis net.Listener) {
		_ = lis.Close()
	}(lis)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("external", NewFactory(lis)),
		fs.WithTubeFactory("memory", contube.NewMemoryQueueFactory(context.Background())),
		fs.WithTubeFactory("empty", contube.NewEmptyTubeFactory()),
	)
	if err != nil {
		t.Fatal(err)
	}

	outputTopic := "output"
	f := &model.Function{
		Name: "test",
		Runtime: model.RuntimeConfig{
			Type: "external",
		},
		Module: "test-source",
		Sources: []model.TubeConfig{
			{
				Type: common.EmptyTubeType,
			},
		},
		Sink: model.TubeConfig{
			Type: common.MemoryTubeType,
			Config: (&contube.SinkQueueConfig{
				Topic: outputTopic,
			}).ToConfigMap(),
		},
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	assert.NoError(t, err)

	go runMockClient()

	for i := 0; i < 10; i++ {
		output, err := fm.ConsumeEvent(outputTopic)
		assert.NoError(t, err)

		r := &testRecord{}
		err = json.Unmarshal(output.GetPayload(), &r)
		assert.NoError(t, err)
		assert.Equal(t, i, r.ID)
	}

	err = fm.DeleteFunction("", f.Name)
	assert.NoError(t, err)
}

type TestSink struct {
	sinkCh chan Counter
}

func (f *TestSink) Init(_ gofs.FunctionContext) error {
	return nil
}

func (f *TestSink) Handle(_ gofs.FunctionContext, event gofs.Event[Counter]) error {
	f.sinkCh <- *event.Data()
	return nil
}

func newTestSink() *TestSink {
	return &TestSink{
		sinkCh: make(chan Counter),
	}
}

func TestExternalSinkModule(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	assert.NoError(t, os.RemoveAll(testSocketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	assert.NoError(t, os.Setenv("FS_MODULE_NAME", "test-sink"))
	lis, err := net.Listen("unix", testSocketPath)
	assert.NoError(t, err)
	defer func(lis net.Listener) {
		_ = lis.Close()
	}(lis)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("external", NewFactory(lis)),
		fs.WithTubeFactory("memory", contube.NewMemoryQueueFactory(context.Background())),
		fs.WithTubeFactory("empty", contube.NewEmptyTubeFactory()),
	)
	if err != nil {
		t.Fatal(err)
	}

	inputTopic := "input"
	f := &model.Function{
		Name: "test",
		Runtime: model.RuntimeConfig{
			Type: "external",
		},
		Module: "test-sink",
		Sources: []model.TubeConfig{
			{
				Type: common.MemoryTubeType,
				Config: (&contube.SourceQueueConfig{
					Topics:  []string{inputTopic},
					SubName: "test",
				}).ToConfigMap(),
			},
		},
		Sink: model.TubeConfig{
			Type: common.EmptyTubeType,
		},
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	assert.NoError(t, err)

	sinkMod := newTestSink()

	go func() {
		err := gofs.NewFSClient().Register("test-sink", gofs.WithSink(sinkMod)).Run()
		if err != nil {
			log.Error(err, "failed to run mock client")
		}
	}()

	event, err := contube.NewStructRecord(&Counter{
		Count: 1,
	}, func() {})
	assert.NoError(t, err)
	err = fm.ProduceEvent(inputTopic, event)
	assert.NoError(t, err)

	r := <-sinkMod.sinkCh
	assert.Equal(t, 1, r.Count)

	err = fm.DeleteFunction("", f.Name)
	assert.NoError(t, err)
}

func TestExternalStatefulModule(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	assert.NoError(t, os.RemoveAll(testSocketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	assert.NoError(t, os.Setenv("FS_MODULE_NAME", "test-stateful"))
	lis, err := net.Listen("unix", testSocketPath)
	assert.NoError(t, err)
	defer func(lis net.Listener) {
		_ = lis.Close()
	}(lis)

	storeFactory, err := statestore.NewDefaultPebbleStateStoreFactory()
	assert.NoError(t, err)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("external", NewFactory(lis)),
		fs.WithTubeFactory("memory", contube.NewMemoryQueueFactory(context.Background())),
		fs.WithTubeFactory("empty", contube.NewEmptyTubeFactory()),
		fs.WithStateStoreFactory(storeFactory),
	)
	assert.NoError(t, err)

	f := &model.Function{
		Name: "test",
		Runtime: model.RuntimeConfig{
			Type: "external",
		},
		Module: "test-stateful",
		Sources: []model.TubeConfig{
			{
				Type: common.EmptyTubeType,
			},
		},
		Sink: model.TubeConfig{
			Type: common.EmptyTubeType,
		},
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	assert.NoError(t, err)

	readyCh := make(chan struct{})

	go func() {
		err := gofs.NewFSClient().Register("test-stateful", gofs.WithCustom(gofs.NewSimpleCustom(
			func(ctx gofs.FunctionContext) error {
				err = ctx.PutState(context.Background(), "test-key", []byte("test-value"))
				if err != nil {
					log.Error(err, "failed to put state")
				}
				close(readyCh)
				return nil
			},
		))).Run()
		if err != nil {
			log.Error(err, "failed to run mock client")
		}
	}()

	<-readyCh

	store, err := storeFactory.NewStateStore(nil)
	assert.NoError(t, err)

	value, err := store.GetState(context.Background(), "test-key")
	assert.NoError(t, err)
	assert.Equal(t, "test-value", string(value))
}

func TestFunctionConfig(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	assert.NoError(t, os.RemoveAll(testSocketPath))
	module := "test-function-config"
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	assert.NoError(t, os.Setenv("FS_MODULE_NAME", module))
	lis, err := net.Listen("unix", testSocketPath)
	assert.NoError(t, err)
	defer func(lis net.Listener) {
		_ = lis.Close()
	}(lis)

	storeFactory, err := statestore.NewDefaultPebbleStateStoreFactory()
	assert.NoError(t, err)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("external", NewFactory(lis)),
		fs.WithTubeFactory("empty", contube.NewEmptyTubeFactory()),
		fs.WithStateStoreFactory(storeFactory),
	)
	assert.NoError(t, err)

	f := &model.Function{
		Name: "test",
		Runtime: model.RuntimeConfig{
			Type: "external",
		},
		Module: module,
		Sources: []model.TubeConfig{
			{
				Type: common.EmptyTubeType,
			},
		},
		Sink: model.TubeConfig{
			Type: common.EmptyTubeType,
		},
		Replicas: 1,
		Config: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	err = fm.StartFunction(f)
	assert.NoError(t, err)

	readyCh := make(chan struct{})

	go func() {
		err := gofs.NewFSClient().Register(module, gofs.WithCustom(gofs.NewSimpleCustom(
			func(ctx gofs.FunctionContext) error {
				err = ctx.PutState(context.Background(), "test-key", []byte("test-value"))
				if err != nil {
					log.Error(err, "failed to put state")
				}
				close(readyCh)
				return nil
			},
		))).Run()
		if err != nil {
			log.Error(err, "failed to run mock client")
		}
	}()

	<-readyCh

	store, err := storeFactory.NewStateStore(nil)
	assert.NoError(t, err)

	value, err := store.GetState(context.Background(), "test-key")
	assert.NoError(t, err)
	assert.Equal(t, "test-value", string(value))
}
