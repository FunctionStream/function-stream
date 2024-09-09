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
	"net"
	"os"
	"testing"

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

var log = common.NewDefaultLogger()

func runMockClient() {
	err := gofs.Register(gofs.DefaultModule, func(i *Person) *Person {
		i.Money += 1
		return i
	})
	if err != nil {
		log.Error(err, "failed to register default function")
	}
	err = gofs.Register("counter", func(i *Counter) *Counter {
		i.Count += 1
		return i
	})
	if err != nil {
		log.Error(err, "failed to register counter function")
	}
	gofs.Run()
}

func TestExternalRuntime(t *testing.T) {
	socketPath := "/tmp/test.sock"
	assert.NoError(t, os.RemoveAll(socketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", socketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	lis, err := net.Listen("unix", socketPath)
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
	socketPath := "/tmp/test.sock"
	assert.NoError(t, os.RemoveAll(socketPath))
	assert.NoError(t, os.Setenv("FS_SOCKET_PATH", socketPath))
	assert.NoError(t, os.Setenv("FS_FUNCTION_NAME", "test"))
	assert.NoError(t, os.Setenv("FS_MODULE_NAME", "counter"))
	lis, err := net.Listen("unix", socketPath)
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
