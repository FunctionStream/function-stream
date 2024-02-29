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
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/tests"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"testing"
	"time"
)

type mockInstance struct {
	api.FunctionInstance
	ctx        context.Context
	definition *model.Function
}

func (m *mockInstance) Context() context.Context {
	return m.ctx
}

func (m *mockInstance) Definition() *model.Function {
	return m.definition
}

func (m *mockInstance) Stop() {

}

func (m *mockInstance) Run(_ api.FunctionRuntimeFactory) {

}
func (m *mockInstance) WaitForReady() <-chan error {
	c := make(chan error)
	close(c)
	return c
}

func (m *mockInstance) Logger() *slog.Logger {
	return slog.Default()
}

func TestGRPCFunc(t *testing.T) {
	ctx, closeFSReconcile := context.WithCancel(context.Background())
	fsService := NewFSReconcile(ctx)
	addr := "localhost:17400"
	s, err := StartGRPCServer(fsService, addr) // The test may running in parallel with other tests, so we need to specify the port
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

	funcCtx, funcCancel := context.WithCancel(context.Background())
	function, err := fsService.NewFunctionRuntime(&mockInstance{
		ctx: funcCtx,
		definition: &model.Function{
			Name: "test",
		},
	})
	if err != nil {
		t.Error(err)
	}
	select {
	case <-function.WaitForReady():
		t.Logf("function ready")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for function ready")
	}

	result, err := function.Call(contube.NewRecordImpl([]byte("hello"), func() {
		t.Logf("commit")
	}))
	if err != nil {
		t.Fatalf("failed to call function: %v", err)
		return
	}
	if string(result.GetPayload()) != "hello!" {
		t.Fatalf("unexpected result: %v", result)
		return
	}

	funcCancel()
	function.Stop()

	fsService.functionsMu.Lock()
	if _, ok := fsService.functions["test"]; ok {
		t.Fatalf("function not removed")
	}
	fsService.functionsMu.Unlock()

	closeFSReconcile()

	time.Sleep(3 * time.Second) // Wait for some time to make sure the cleanup of function doesn't raise any errors
}

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

	store := tests.NewTestPebbleStateStore(t)

	fm, err := fs.NewFunctionManager(
		fs.WithRuntimeFactory("grpc", fsService),
		fs.WithDefaultTubeFactory(contube.NewMemoryQueueFactory(ctx)),
		fs.WithStateStore(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	f := &model.Function{
		Name: "test",
		Runtime: &model.RuntimeConfig{
			Type: common.OptionalStr("grpc"),
			Config: map[string]interface{}{
				"addr": addr,
			},
		},
		Inputs:   []string{"input"},
		Output:   "output",
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	if err != nil {
		t.Fatal(err)
	}

	err = fm.GetStateStore().PutState("counter", []byte("0"))
	assert.Nil(t, err)

	event := contube.NewRecordImpl([]byte("hello"), func() {})
	err = fm.ProduceEvent(f.Inputs[0], event)
	if err != nil {
		t.Fatal(err)
	}
	output, err := fm.ConsumeEvent(f.Output)
	if err != nil {
		t.Fatal(err)
	}
	if string(output.GetPayload()) != "hello!" {
		t.Fatalf("unexpected result: %v", output)
	}

	counter, err := fm.GetStateStore().GetState("counter")
	assert.Nil(t, err)
	assert.Equal(t, "1", string(counter))

	err = fm.DeleteFunction(f.Name)
	if err != nil {
		t.Fatal(err)
	}

	closeFSReconcile()
}
