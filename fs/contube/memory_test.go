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

package contube

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestMemoryNewSourceTube(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	memoryQueueFactory := MemoryQueueFactory{
		ctx:    ctx,
		mu:     sync.Mutex{},
		queues: make(map[string]*queue),
	}

	// Create queues, corresponding to multiple topics
	topics := []string{"topic1", "topic2", "topic3"}
	for i, v := range topics {
		memoryQueueFactory.queues[v] = &queue{
			c:      make(chan Record, 1),
			refCnt: 0,
		}
		memoryQueueFactory.queues[v].c <- &RecordImpl{
			payload:    []byte{byte(i)},
			commitFunc: nil,
		}
	}

	ch, err := memoryQueueFactory.NewSourceTube(ctx, (&SourceQueueConfig{Topics: []string{"topic1", "topic2", "topic3"}, SubName: "consume-" + strconv.Itoa(rand.Int())}).ToConfigMap())
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var events []Record

	go func() {
		defer wg.Done()
		for record := range ch {
			log.Printf("Received record: %+v", record)
			events = append(events, record)
		}
	}()

	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()

	if len(events) == len(topics) {
		t.Log("Successful")
	} else {
		t.Fatal("failed")
	}
}
