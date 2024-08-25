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
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/functionstream/function-stream/common/config"
	funcModel "github.com/functionstream/function-stream/common/model"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/fs/runtime/external/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type functionServerImpl struct {
	model.FunctionServer
	runtimeMaps sync.Map
}

func (f *functionServerImpl) getFunctionRuntime(ctx context.Context) (*runtime, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("failed to get metadata")
	}
	if _, ok := md["name"]; !ok || len(md["name"]) == 0 {
		return nil, fmt.Errorf("the metadata doesn't contain the function name")
	}
	name := md["name"][0]
	r, ok := f.runtimeMaps.Load(name)
	if !ok {
		msg := fmt.Sprintf("function runtime %s not found", name)
		return nil, status.Error(codes.Unavailable, msg)
	}
	return r.(*runtime), nil
}

func (f *functionServerImpl) RegisterSchema(ctx context.Context,
	request *model.RegisterSchemaRequest) (*model.RegisterSchemaResponse, error) {
	r, err := f.getFunctionRuntime(ctx)
	if err != nil {
		return nil, err
	}
	r.log.Info("Registering schema", "schema", request.Schema)
	return &model.RegisterSchemaResponse{}, nil
}

func (f *functionServerImpl) Read(ctx context.Context, request *model.ReadRequest) (*model.Event, error) {
	r, err := f.getFunctionRuntime(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case e := <-r.inputCh:
		return &model.Event{
			Payload: e.GetPayload(),
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *functionServerImpl) Write(ctx context.Context, event *model.Event) (*model.WriteResponse, error) {
	r, err := f.getFunctionRuntime(ctx)
	if err != nil {
		return nil, err
	}
	if err = r.funcCtx.Write(contube.NewRecordImpl(event.Payload, func() {})); err != nil {
		return nil, err
	}
	return &model.WriteResponse{}, nil
}

var _ model.FunctionServer = &functionServerImpl{}

type Factory struct {
	server *functionServerImpl
	sync.Mutex
	log *common.Logger
}

func (f *Factory) NewFunctionRuntime(instance api.FunctionInstance,
	_ *funcModel.RuntimeConfig) (api.FunctionRuntime, error) {
	def := instance.Definition()
	r := &runtime{
		inputCh: make(chan contube.Record),
		funcCtx: instance.FunctionContext(),
		log:     instance.Logger(),
	}
	f.server.runtimeMaps.Store(common.GetNamespacedName(def.Namespace, def.Name).String(), r)
	f.log.Info("Creating new function runtime", "function", common.GetNamespacedName(def.Namespace, def.Name))
	return r, nil
}

func NewFactory(lis net.Listener) api.FunctionRuntimeFactory {
	log := common.NewDefaultLogger().SubLogger("component", "external-runtime")
	s := grpc.NewServer()
	server := &functionServerImpl{}
	model.RegisterFunctionServer(s, server)
	// Register the health check service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, healthServer)
	healthServer.SetServingStatus("fs_external.Function", grpc_health_v1.HealthCheckResponse_SERVING)

	go func() {
		log.Info("Starting external runtime server")
		if err := s.Serve(lis); err != nil {
			log.Error(err, "Failed to start external runtime server")
		}
	}()
	return &Factory{
		server: server,
		log:    common.NewDefaultLogger().SubLogger("component", "external-runtime-factory"),
	}
}

const (
	DefaultSocketPath = "/tmp/fs.sock"
)

func NewFactoryWithConfig(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error) {
	socketPath := ""
	if v, ok := configMap["socket-path"].(string); ok {
		socketPath = v
	}
	if socketPath == "" {
		common.NewDefaultLogger().Info("socketPath is not set, use the default value: " + DefaultSocketPath)
		socketPath = DefaultSocketPath
	}
	_ = os.Remove(socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	return NewFactory(lis), nil
}

type runtime struct {
	inputCh chan contube.Record
	funcCtx api.FunctionContext
	log     *common.Logger
}

func (r *runtime) Call(e contube.Record) (contube.Record, error) {
	r.inputCh <- e
	return nil, nil
}

func (r *runtime) Stop() {
}
