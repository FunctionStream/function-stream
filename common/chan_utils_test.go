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

package common

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSendToChannel(t *testing.T) {

	t.Run("send_buffered_chan_success", func(t *testing.T) {

		c := make(chan string, 1)
		ctx := context.Background()
		if !SendToChannel(ctx, c, "data") {
			t.Fatal("SendToChannel should return true when sending succeeds")
		}
		value := <-c
		if value != "data" {
			t.Errorf("expected to receive \"data\" from channel, but received %s", value)
		}

	})

	t.Run("send_unbuffered_chan_success", func(t *testing.T) {
		c := make(chan string)
		ctx := context.Background()

		go func() {
			SendToChannel(ctx, c, "data")
		}()

		value := <-c
		if value != "data" {
			t.Errorf("expected to receive \"data\" from channel, but received %s", value)
		}

	})

	t.Run("context_timeout", func(t *testing.T) {
		// Using time.Sleep to simulating context timeout

		c := make(chan string)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		time.Sleep(1 * time.Second) // Set timeout
		if k := SendToChannel(ctx, c, "hello"); k {
			t.Fatal("context timeout but data sent successfully")
		} else {
			t.Log("failed to send data due to context timeout")
		}

	})

	t.Run("incorrect_type", func(t *testing.T) {

		defer func() {
			if r := recover(); r != nil {
				t.Log("test-ok")
			} else {
				t.Log("test-fail")
			}
		}()
		c := make(chan int)
		ctx := context.Background()
		SendToChannel(ctx, c, "incorrect type")

	})
}

func TestZeroValue(t *testing.T) {

	testZeroValue := func(name string, got, want interface{}) {
		t.Run(name, func(t *testing.T) {
			if !reflect.DeepEqual(got, want) {
				t.Errorf("zeroValue() = %v, want %v", got, want)
			}
		})
	}

	testZeroValue("int", zeroValue[int](), 0)
	testZeroValue("float64", zeroValue[float64](), float64(0))
	testZeroValue("string", zeroValue[string](), "")
	testZeroValue("bool", zeroValue[bool](), false)

}

func TestReceiveFromChannel(t *testing.T) {
	// Since SendToChannel has already been tested, only buffered chan will be considered here

	t.Run("Success", func(t *testing.T) {
		ctx := context.Background()
		ch := make(chan string, 1)
		SendToChannel(ctx, ch, "test-data")
		value, ok := ReceiveFromChannel(ctx, ch)
		if ok {
			t.Log("successfully received data")
		}
		if value != "test-data" {
			t.Errorf("receive failed,expected value to be \"test-data\", but it's %s", value)
		}

	})

	t.Run("Timeout", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		ch := make(chan string, 1)
		time.Sleep(1 * time.Second)
		// No need to send data to SendToChannel as the context has been set to expire
		value, ok := ReceiveFromChannel(ctx, ch)
		if ok {
			t.Fatal("due to timeout setting, it is expected that no value will be received from the channel")
		}
		if value != "" {
			t.Errorf("expected zero value for string, but it's %s", value)
		}

	})

	t.Run("Canceled", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel context
		ch := make(chan string, 1)
		value, ok := ReceiveFromChannel(ctx, ch)
		if ok {
			t.Fatal("expected no value to be received from channel due to context cancellation")
		}
		if value != "" {
			t.Errorf("expected zero value for string, but it's %s", value)
		}

	})
}
