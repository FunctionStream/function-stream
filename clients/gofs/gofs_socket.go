//go:build !wasi

package gofs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/functionstream/function-stream/fs/runtime/external/model"
	"github.com/wirelessr/avroschema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var client model.FunctionClient
var ctx = context.Background()

var modules = make(map[string]func([]byte) []byte)
var modulesMu sync.RWMutex

const (
	FSSocketPath   = "FS_SOCKET_PATH"
	FSFunctionName = "FS_FUNCTION_NAME"
	FSModuleName   = "FS_MODULE_NAME"
	DefaultModule  = "default"
)

func check() error {
	if client == nil {
		socketPath := os.Getenv(FSSocketPath)
		if socketPath == "" {
			return fmt.Errorf("%s is not set", FSSocketPath)
		}
		funcName := os.Getenv(FSFunctionName)
		if funcName == "" {
			return fmt.Errorf("%s is not set", FSFunctionName)
		}

		serviceConfig := `{
		   "methodConfig": [{
		       "name": [{"service": "*"}],
		       "retryPolicy": {
		           "maxAttempts": 30,
		           "initialBackoff": "0.1s",
		           "maxBackoff": "30s",
		           "backoffMultiplier": 2,
		           "retryableStatusCodes": ["UNAVAILABLE"]
		       }
		   }]
		}`
		conn, err := grpc.NewClient(
			"unix:"+socketPath,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(serviceConfig),
		)
		if err != nil {
			panic(err)
		}
		client = model.NewFunctionClient(conn)
		md := metadata.New(map[string]string{
			"name": funcName,
		})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return nil
}

func Register[I any, O any](module string, process func(*I) *O) error {
	if err := check(); err != nil {
		return err
	}
	outputSchema, err := avroschema.Reflect(new(O))
	if err != nil {
		return err
	}
	processFunc := func(payload []byte) []byte {
		input := new(I)
		err = json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		output := process(input)
		outputPayload, _ := json.Marshal(output)
		return outputPayload
	}
	modulesMu.Lock()
	modules[module] = processFunc
	modulesMu.Unlock()
	_, err = client.RegisterSchema(ctx, &model.RegisterSchemaRequest{Schema: outputSchema})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to register schema: %s\n", err)
		panic(err)
	}
	return nil
}

func Run() {
	if err := check(); err != nil {
		panic(err)
	}
	module := os.Getenv(FSModuleName)
	if module == "" {
		module = DefaultModule
	}
	modulesMu.RLock()
	processFunc, ok := modules[module]
	if !ok {
		panic(fmt.Sprintf("module %s not found", module))
	}
	modulesMu.RUnlock()
	for {
		res, err := client.Read(ctx, &model.ReadRequest{})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
			time.Sleep(3 * time.Second)
			continue
		}
		outputPayload := processFunc(res.Payload)
		_, err = client.Write(ctx, &model.Event{Payload: outputPayload})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to write: %s\n", err)
		}
	}
}
