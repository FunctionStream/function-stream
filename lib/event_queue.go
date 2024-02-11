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

package lib

import (
	"context"
)

type Event interface {
	GetPayload() []byte
	Ack()
}

type SourceQueueConfig struct {
	Topics  []string
	SubName string
}

type SinkQueueConfig struct {
	Topic string
}

//type EventQueueFactory func(ctx context.Context, config *QueueConfig, function *model.Function) (EventQueue, error)

type EventQueueFactory interface {
	NewSourceChan(ctx context.Context, config *SourceQueueConfig) (<-chan Event, error)
	// NewSinkChan returns a news channel
	// The event.Ack() would be invoked after the event is sunk successfully
	// The caller should close the channel when it is done
	NewSinkChan(ctx context.Context, config *SinkQueueConfig) (chan<- Event, error)
}

type EventQueue interface {
	GetSendChan() (chan<- Event, error)
	GetRecvChan() (<-chan Event, error)
}

type AckableEvent struct {
	payload []byte
	ackFunc func()
}

func NewAckableEvent(payload []byte, ackFunc func()) *AckableEvent {
	return &AckableEvent{
		payload: payload,
		ackFunc: ackFunc,
	}
}

func (e *AckableEvent) GetPayload() []byte {
	return e.payload
}

func (e *AckableEvent) Ack() {
	e.ackFunc()
}
