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
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs"
	"github.com/functionstream/functionstream/fs/api"
	"github.com/functionstream/functionstream/fs/contube"
	"testing"
	"time"
)

type mockInstance struct {
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

func TestGRPCFunc(t *testing.T) {
	ctx, closeFSReconcile := context.WithCancel(context.Background())
	fsService := NewFSReconcile(ctx)
	s, err := StartGRPCServer(fsService, 17400) // The test may running in parallel with other tests, so we need to specify the port
	if err != nil {
		t.Fatal(err)
		return
	}
	defer s.Stop()

	go StartMockGRPCFunc(t)

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
	s, err := StartGRPCServer(fsService, 17401)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer s.Stop()
	go StartMockGRPCFunc(t)
	select {
	case <-fsService.WaitForReady():
		t.Logf("ready")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for fs service ready")
		return
	}

	fm, err := fs.NewFunctionManager(
		fs.WithDefaultRuntimeFactory(fsService),
		fs.WithDefaultTubeFactory(contube.NewMemoryQueueFactory(ctx)))
	if err != nil {
		t.Fatal(err)
	}

	f := &model.Function{
		Name:     "test",
		Inputs:   []string{"input"},
		Output:   "output",
		Replicas: 1,
	}

	err = fm.StartFunction(f)
	if err != nil {
		t.Fatal(err)
	}

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

	err = fm.DeleteFunction(f.Name)
	if err != nil {
		t.Fatal(err)
	}

	closeFSReconcile()
}
