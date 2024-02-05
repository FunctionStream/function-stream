/*
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
	"github.com/functionstream/functionstream/restclient"
	"math/rand"
	"strconv"
	"testing"
)

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
	f := restclient.Function{
		Archive: "./bin/example_basic.wasm",
		Inputs:  []string{"test-input-" + strconv.Itoa(rand.Int())},
		Output:  "test-output-" + strconv.Itoa(rand.Int()),
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

	res, err := cli.DefaultAPI.ApiV1FunctionFunctionNamePost(context.Background(), name).Function(f).Execute()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
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

	res, err = cli.DefaultAPI.ApiV1FunctionFunctionNameDelete(context.Background(), name).Execute()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
}