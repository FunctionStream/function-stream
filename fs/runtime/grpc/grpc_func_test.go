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

package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/stretchr/testify/assert"
)

func TestFMWithGRPCRuntime(t *testing.T) {
	ctx, closeFSReconcile := context.WithCancel(context.Background())
	fsService := NewFSReconcile(ctx)
	addr := "localhost:17401"
	s, err := StartGRPCServer(fsService, addr)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer s.Stop()
	go StartMockGRPCFunc(t, addr)
	select {
	case <-fsService.WaitForReady():
		t.Logf("ready")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for fs service ready")
		return
	}

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("grpc", fsService),
		fs.WithTubeFactory("memory", contube.NewMemoryQueueFactory(ctx)),
	)
	if err != nil {
		t.Fatal(err)
	}

	inputTopic := "input"
	outputTopic := "output"
	f := &model.Function{
		Name: "test",
		Runtime: &model.RuntimeConfig{
			Type: common.OptionalStr("grpc"),
			Config: map[string]interface{}{
				"addr": addr,
			},
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
	if err != nil {
		t.Fatal(err)
	}

	err = fm.GetStateStore().PutState("counter", []byte("0"))
	assert.Nil(t, err)

	event := contube.NewRecordImpl([]byte("hello"), func() {})
	err = fm.ProduceEvent(inputTopic, event)
	if err != nil {
		t.Fatal(err)
	}
	output, err := fm.ConsumeEvent(outputTopic)
	if err != nil {
		t.Fatal(err)
	}
	if string(output.GetPayload()) != "hello!" {
		t.Fatalf("unexpected result: %v", output)
	}

	counter, err := fm.GetStateStore().GetState("counter")
	assert.Nil(t, err)
	assert.Equal(t, "1", string(counter))

	err = fm.DeleteFunction("", f.Name)
	if err != nil {
		t.Fatal(err)
	}

	closeFSReconcile()
}
