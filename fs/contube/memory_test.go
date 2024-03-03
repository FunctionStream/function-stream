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
	queues1 := queue{
		c:      make(chan Record, 1),
		refCnt: 0,
	}
	record1 := &RecordImpl{
		payload:    []byte{1},
		commitFunc: nil,
	}
	queues1.c <- record1
	queues2 := queue{
		c:      make(chan Record, 1),
		refCnt: 0,
	}
	record2 := &RecordImpl{
		payload:    []byte{2},
		commitFunc: nil,
	}
	queues2.c <- record2
	queues3 := queue{
		c:      make(chan Record, 1),
		refCnt: 0,
	}
	record3 := &RecordImpl{
		payload:    []byte{3},
		commitFunc: nil,
	}
	queues3.c <- record3

	memoryQueueFactory.queues["topic1"] = &queues1
	memoryQueueFactory.queues["topic2"] = &queues2
	memoryQueueFactory.queues["topic3"] = &queues3

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
			if record == record1 || record == record2 || record == record3 {
				events = append(events, record)
			}
		}
	}()

	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()

	// Create three Records data, determine whether the length of the received data is equal to 3
	// There is no need to determine whether the data in the "event" corresponds to the sent data, because the data has already been judged when it is written into the "event".
	if len(events) == 3 {
		t.Log("Successful")
	} else {
		t.Fatal("failed")
	}
}
