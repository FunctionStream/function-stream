//go:build !wasi

package gofs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/fs/runtime/external/model"
	"github.com/wirelessr/avroschema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"os"
)

var client model.FunctionClient
var ctx = context.Background()

var processFunc func([]byte) []byte

func check() error {
	if client == nil {
		socketPath := os.Getenv("FS_SOCKET_PATH")
		if socketPath == "" {
			return fmt.Errorf("FS_SOCKET_PATH is not set")
		}
		funcName := os.Getenv("FS_FUNCTION_NAME")
		if funcName == "" {
			return fmt.Errorf("FS_FUNCTION_NAME is not set")
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

func Register[I any, O any](process func(*I) *O) error {
	if err := check(); err != nil {
		return err
	}
	outputSchema, err := avroschema.Reflect(new(O))
	if err != nil {
		return err
	}
	processFunc = func(payload []byte) []byte {
		input := new(I)
		err = json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s", err, payload)
		}
		output := process(input)
		outputPayload, _ := json.Marshal(output)
		return outputPayload
	}
	_, err = client.RegisterSchema(ctx, &model.RegisterSchemaRequest{Schema: outputSchema})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to register schema: %s", err)
		panic(err)
	}
	return nil
}

func Run() {
	if err := check(); err != nil {
		panic(err)
	}
	for {
		res, err := client.Read(ctx, &model.ReadRequest{})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s", err)
			continue
		}
		outputPayload := processFunc(res.Payload)
		_, err = client.Write(ctx, &model.Event{Payload: outputPayload})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to write: %s", err)
		}
	}
}
