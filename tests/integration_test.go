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

package tests

import (
	"context"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/restclient"
	"github.com/functionstream/function-stream/server"
	"io"
	"log/slog"
	"math/rand"
	"strconv"
	"testing"
)

func startServer() {
	common.RunProcess(func() (io.Closer, error) {
		s, err := server.NewDefaultServer()
		if err != nil {
			return nil, err
		}
		go s.Run(context.Background())
		return s, nil
	})
}

func init() {
	go startServer()
}

func TestBasicFunction(t *testing.T) {

	cfg := restclient.NewConfiguration()
	cli := restclient.NewAPIClient(cfg)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	name := "func-" + strconv.Itoa(rand.Int())
	f := restclient.ModelFunction{
		Runtime: restclient.ModelRuntimeConfig{
			Config: map[string]interface{}{
				common.RuntimeArchiveConfigKey: "../bin/example_basic.wasm",
			},
		},
		Inputs:   []string{"test-input-" + strconv.Itoa(rand.Int())},
		Output:   "test-output-" + strconv.Itoa(rand.Int()),
		Replicas: 1,
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: f.Inputs[0],
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            f.Output,
		SubscriptionName: "test-sub",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	res, err := cli.FunctionAPI.CreateFunction(context.Background()).Body(f).Execute()
	if err != nil {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf(err.Error())
			return
		}
		slog.Error(string(body))
		t.Fatal("failed to create function")
		return
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
		return
	}

	for i := 0; i < 10; i++ {
		p := Person{Name: "rbt", Money: 0}
		jsonBytes, err := json.Marshal(p)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: jsonBytes,
		})
		if err != nil {
			return
		}

		msg, err := consumer.Receive(context.Background())
		if err != nil {
			t.Fatalf(err.Error())
		}
		payload := msg.Payload()
		var out Person
		err = json.Unmarshal(payload, &out)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if out.Money != 1 {
			t.Fatalf("expected 1, got %d", out.Money)
		}
	}

	res, err = cli.FunctionAPI.DeleteFunction(context.Background(), name).Execute()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
}
