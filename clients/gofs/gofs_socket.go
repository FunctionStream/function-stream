//go:build !wasi

package gofs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/functionstream/function-stream/fs/runtime/external/model"
	"github.com/wirelessr/avroschema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var client model.FunctionClient
var clientInitMu sync.Mutex
var ctx = context.Background()

const (
	StateInit int32 = iota
	StateRunning
)

var state = StateInit

const (
	FSSocketPath   = "FS_SOCKET_PATH"
	FSFunctionName = "FS_FUNCTION_NAME"
	FSModuleName   = "FS_MODULE_NAME"
	DefaultModule  = "default"
)

func check() error {
	if client == nil {
		clientInitMu.Lock()
		defer clientInitMu.Unlock()
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

var registerLock sync.Mutex
var oldmodules = make(map[string]func([]byte) []byte)
var sources = make(map[string]func())
var sinks = make(map[string]func([]byte))

func Register[I any, O any](module string, process func(*I) *O) error { // Should panic directly.
	if atomic.LoadInt32(&state) != StateInit {
		panic("cannot register module during running")
	}
	if err := check(); err != nil {
		return err // TODO: Should panic here
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
	_, err = client.RegisterSchema(ctx, &model.RegisterSchemaRequest{Schema: outputSchema})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to register schema: %s\n", err)
		panic(err)
	}
	registerLock.Lock()
	defer registerLock.Unlock()
	oldmodules[module] = processFunc
	return nil
}

func RegisterSource[O any](module string, process func(emit func(*O) error)) error {
	if atomic.LoadInt32(&state) != StateInit {
		panic("cannot register module during running")
	}
	if err := check(); err != nil {
		return err
	}
	outputSchema, err := avroschema.Reflect(new(O))
	if err != nil {
		return err
	}
	emit := func(event *O) error {
		outputPayload, _ := json.Marshal(event)
		_, err := client.Write(ctx, &model.Event{Payload: outputPayload})
		return err
	}
	_, err = client.RegisterSchema(ctx, &model.RegisterSchemaRequest{Schema: outputSchema})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to register schema: %s\n", err)
		panic(err)
	}
	sources[module] = func() {
		process(emit)
	}
	registerLock.Lock()
	defer registerLock.Unlock()
	return nil
}

func RegisterSink[I any](module string, process func(*I)) error {
	if atomic.LoadInt32(&state) != StateInit {
		panic("cannot register module during running")
	}
	if err := check(); err != nil {
		return err
	}
	inputSchema, err := avroschema.Reflect(new(I))
	if err != nil {
		return err
	}
	processFunc := func(payload []byte) {
		input := new(I)
		err = json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		process(input)
	}
	_, err = client.RegisterSchema(ctx, &model.RegisterSchemaRequest{Schema: inputSchema})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to register schema: %s\n", err)
		panic(err)
	}
	registerLock.Lock()
	defer registerLock.Unlock()
	sinks[module] = processFunc
	return nil
}

func runFunction(module string) bool {
	if processFunc, ok := oldmodules[module]; ok {
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
	return false
}

func runSources(module string) bool {
	if processFunc, ok := sources[module]; ok {
		processFunc()
		return true
	}
	return false
}

func runSinks(module string) bool {
	if processFunc, ok := sinks[module]; ok {
		for {
			res, err := client.Read(ctx, &model.ReadRequest{})
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			processFunc(res.Payload)
		}
	}
	return false
}

func Run() {
	if !atomic.CompareAndSwapInt32(&state, StateInit, StateRunning) {
		panic("the instance is already running")
	}
	if err := check(); err != nil {
		panic(err)
	}
	module := os.Getenv(FSModuleName)
	if module == "" {
		module = DefaultModule
	}
	if !(runFunction(module) || runSources(module) || runSinks(module)) {
		panic(fmt.Errorf("module %s not found", module))
	}
}
