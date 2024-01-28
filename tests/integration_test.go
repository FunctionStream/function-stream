package tests

import (
	"context"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/lib"
	"math/rand"
	"strconv"
	"testing"
)

type Person struct {
	Name  string `json:"name"`
	Money int    `json:"money"`
}

func TestBasicFunction(t *testing.T) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	f := model.Function{
		Name:    "test",
		Archive: "../bin/example_basic.wasm",
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

	fm, err := lib.NewFunctionManager()
	if err != nil {
		t.Fatalf(err.Error())
	}

	fm.StartFunction(f)

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
