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
	"context"
	"encoding/json"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/restclient"
	"github.com/functionstream/function-stream/tests"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net"
	"strconv"
	"testing"
)

func getListener(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	t.Logf("Listening on %s\n", ln.Addr().String())
	return ln
}

func startStandaloneSvr(t *testing.T, ctx context.Context, opts ...ServerOption) (*Server, string) {
	ln := getListener(t)
	defaultOpts := []ServerOption{
		WithHttpListener(ln),
	}
	s, err := NewServer(
		append(defaultOpts, opts...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	svrCtx, svrCancel := context.WithCancel(context.Background())
	go s.Run(svrCtx)
	go func() {
		<-ctx.Done()
		svrCancel()
	}()
	return s, ln.Addr().String()
}

func TestStandaloneBasicFunction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := startStandaloneSvr(t, ctx)

	inputTopic := "test-input-" + strconv.Itoa(rand.Int())
	outputTopic := "test-output-" + strconv.Itoa(rand.Int())

	funcConf := &model.Function{
		Runtime: &model.RuntimeConfig{
			Config: map[string]interface{}{
				common.RuntimeArchiveConfigKey: "../bin/example_basic.wasm",
			},
		},
		Inputs:   []string{inputTopic},
		Output:   outputTopic,
		Name:     "test-func",
		Replicas: 1,
	}
	err := s.Manager.StartFunction(funcConf)
	if err != nil {
		t.Fatal(err)
	}

	p := &tests.Person{
		Name:  "rbt",
		Money: 0,
	}
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Manager.ProduceEvent(inputTopic, contube.NewRecordImpl(jsonBytes, func() {
	}))
	if err != nil {
		t.Fatal(err)
	}

	event, err := s.Manager.ConsumeEvent(outputTopic)
	if err != nil {
		t.Error(err)
		return
	}
	var out tests.Person
	err = json.Unmarshal(event.GetPayload(), &out)
	if err != nil {
		t.Error(err)
		return
	}
	if out.Money != 1 {
		t.Errorf("expected 1, got %d", out.Money)
		return
	}
}

func TestHttpTube(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, httpAddr := startStandaloneSvr(t, ctx, nil, nil)

	endpoint := "test-endpoint"
	funcConf := &model.Function{
		Runtime: &model.RuntimeConfig{
			Config: map[string]interface{}{
				common.RuntimeArchiveConfigKey: "../bin/example_basic.wasm",
			},
		},
		Source: &model.TubeConfig{
			Type: common.OptionalStr(common.HttpTubeType),
			Config: map[string]interface{}{
				contube.EndpointKey: endpoint,
			},
		},
		Inputs:   []string{},
		Output:   "output",
		Name:     "test-func",
		Replicas: 1,
	}

	err := s.Manager.StartFunction(funcConf)
	assert.Nil(t, err)

	p := &tests.Person{
		Name:  "rbt",
		Money: 0,
	}
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}

	cfg := restclient.NewConfiguration()
	cfg.Host = httpAddr
	cli := restclient.NewAPIClient(cfg)
	_, err = cli.HttpTubeAPI.TriggerHttpTubeEndpoint(ctx, endpoint).Body(string(jsonBytes)).Execute()
	assert.Nil(t, err)

	event, err := s.Manager.ConsumeEvent(funcConf.Output)
	if err != nil {
		t.Error(err)
		return
	}
	var out tests.Person
	err = json.Unmarshal(event.GetPayload(), &out)
	if err != nil {
		t.Error(err)
		return
	}
	if out.Money != 1 {
		t.Errorf("expected 1, got %d", out.Money)
		return
	}
}

type MockRuntimeFactory struct {
}

func (f *MockRuntimeFactory) NewFunctionRuntime(instance api.FunctionInstance) (api.FunctionRuntime, error) {
	return &MockRuntime{
		funcCtx: instance.FunctionContext(),
	}, nil
}

type MockRuntime struct {
	funcCtx api.FunctionContext
}

func (r *MockRuntime) WaitForReady() <-chan error {
	c := make(chan error)
	close(c)
	return c
}

func (r *MockRuntime) Call(e contube.Record) (contube.Record, error) {
	v, err := r.funcCtx.GetState("key")
	if err != nil {
		return nil, err
	}
	str := string(v)
	err = r.funcCtx.PutState("key", []byte(str+"!"))
	if err != nil {
		return nil, err
	}
	return contube.NewRecordImpl(nil, func() {

	}), nil
}

func (r *MockRuntime) Stop() {
}

func TestStatefulFunction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, httpAddr := startStandaloneSvr(t, ctx, WithFunctionManager(fs.WithDefaultRuntimeFactory(&MockRuntimeFactory{})))

	funcConf := &model.Function{
		Name:     "test-func",
		Inputs:   []string{"input"},
		Output:   "output",
		Replicas: 1,
	}
	err := s.Manager.StartFunction(funcConf)
	if err != nil {
		t.Fatal(err)
	}

	cfg := restclient.NewConfiguration()
	cfg.Host = httpAddr
	cli := restclient.NewAPIClient(cfg)

	_, err = cli.StateAPI.SetState(ctx, "key").Body("hello").Execute()
	assert.Nil(t, err)

	err = s.Manager.ProduceEvent(funcConf.Inputs[0], contube.NewRecordImpl(nil, func() {
	}))
	assert.Nil(t, err)

	_, err = s.Manager.ConsumeEvent(funcConf.Output)
	assert.Nil(t, err)

	result, _, err := cli.StateAPI.GetState(ctx, "key").Execute()
	assert.Nil(t, err)
	assert.Equal(t, "hello!", result)
}
