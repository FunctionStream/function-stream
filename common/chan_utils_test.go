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
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSendToChannel(t *testing.T) {

	//send data
	//The first t.Run is a buffered chan, and the second t.Run is an unbuffered chan
	t.Run("send success,Buffered chan", func(t *testing.T) {
		c := make(chan string, 1)
		ctx := context.Background()
		if !SendToChannel(ctx, c, "data") {
			t.Fatal("SendToChannel should return true when sending succeeds")
		}
		value := <-c
		// Verify if the received data is correct
		if value != "data" {
			t.Errorf("expected to receive \"data\" from channel, but received %s", value)
		}
	})

	t.Run("send success,Unbuffered chan", func(t *testing.T) {
		c := make(chan string)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			SendToChannel(ctx, c, "data")
		}()
		go func() {
			defer wg.Done()
			value := <-c
			// Verify if the received data is correct
			if value != "data" {
				t.Errorf("expected to receive \"data\" from channel, but received %s", value)
			}
		}()
		wg.Wait()
	})

	//context timeout
	//The first t.Run is a test with a timeout setting, but it was successfully sent without timeout
	//The second t.Run passes through time.Sleep setting timeout, simulating context timeout
	t.Run("context not timeout", func(t *testing.T) {
		c := make(chan string, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if SendToChannel(ctx, c, "hello") {
			t.Log("Data sent successfully")
		} else {
			fmt.Println(SendToChannel(ctx, c, "hello"))
			t.Fatal("Failed to send data due to context timeout")
		}
		select {
		case <-c:
			t.Log("Successfully received data")
		case <-ctx.Done():
			t.Fatal("Channel closed after context timeout")
		}
	})

	t.Run("context timeout", func(t *testing.T) {
		c := make(chan string, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if SendToChannel(ctx, c, "data") {
			t.Log("Data sent successfully")
		} else {
			t.Fatal("Failed to send data due to context timeout")
		}

		go func() {
			time.Sleep(time.Second) // Set timeout
			select {
			case <-c:
				t.Error("Timed out but able to retrieve data")
				return
			case <-ctx.Done():
				t.Log("No data received due to context timeout")
			}
		}()
	})

	//incorrect type
	//It is not necessary to distinguish between buffered and unbuffered, as it is necessary to test the incorrect type
	t.Run("incorrect type", func(t *testing.T) {
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

	//Since SendToChannel has already been tested, only buffered chan will be considered here
	//Successfully received chan data
	t.Run("Success", func(t *testing.T) {
		ctx := context.Background()
		ch := make(chan string, 1)
		SendToChannel(ctx, ch, "test-data")
		value, _ := ReceiveFromChannel(ctx, ch)
		if value != "test-data" {
			t.Errorf("Receive failed,Expected value to be \"test-data\", but %s", value)
		}
	})

	//context timeout
	t.Run("Timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		time.Sleep(10 * time.Millisecond)
		ch := make(chan string, 1)
		//No need to send data to SendToChannel as the context has been set to expire
		value, ok := ReceiveFromChannel(ctx, ch)
		if ok {
			t.Fatal("Due to timeout setting, it is expected that no value will be received from the channel")
		}
		if value != "" {
			t.Errorf("Expected zero value for string, but %s", value)
		}
	})

	//context canceled
	t.Run("Canceled", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		//Cancel context
		cancel()
		ch := make(chan string, 1)
		value, ok := ReceiveFromChannel(ctx, ch)
		if ok {
			t.Fatal("Expected no value to be received from channel due to context cancellation")
		}
		if value != "" {
			t.Errorf("Expected zero value for string, but %s", value)
		}
	})
}
