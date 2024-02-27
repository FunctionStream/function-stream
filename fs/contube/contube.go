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
	"github.com/pkg/errors"
)

var (
	ErrSinkTubeNotImplemented = errors.New("sink tube not implemented")
)

type Record interface {
	GetPayload() []byte
	Commit()
}

type SourceQueueConfig struct {
	Topics  []string
	SubName string
}

type SinkQueueConfig struct {
	Topic string
}

const (
	TopicKey     = "topic"
	TopicListKey = "topicList"
	SubNameKey   = "subName"
)

func NewSourceQueueConfig(config ConfigMap) *SourceQueueConfig {
	var result SourceQueueConfig
	if topics, ok := config[TopicListKey].([]string); ok {
		result.Topics = topics
	}
	if subName, ok := config[SubNameKey].(string); ok {
		result.SubName = subName
	}
	return &result
}

func (c *SourceQueueConfig) ToConfigMap() ConfigMap {
	return ConfigMap{
		TopicListKey: c.Topics,
		SubNameKey:   c.SubName,
	}
}

func NewSinkQueueConfig(config ConfigMap) *SinkQueueConfig {
	var result SinkQueueConfig
	if topic, ok := config[TopicKey].(string); ok {
		result.Topic = topic
	}
	return &result
}

func (c *SinkQueueConfig) ToConfigMap() ConfigMap {
	return ConfigMap{
		TopicKey: c.Topic,
	}
}

type ConfigMap map[string]interface{}

// MergeConfig merges multiple ConfigMap into one
func MergeConfig(configs ...ConfigMap) ConfigMap {
	result := ConfigMap{}
	for _, config := range configs {
		for k, v := range config {
			result[k] = v
		}
	}
	return result
}

type SourceTubeFactory interface {
	// NewSourceTube returns a new channel that can be used to receive events
	// The channel would be closed when the context is done
	NewSourceTube(ctx context.Context, config ConfigMap) (<-chan Record, error)
}

type SinkTubeFactory interface {
	// NewSinkTube returns a new channel that can be used to sink events
	// The event.Commit() would be invoked after the event is sunk successfully
	// The caller should close the channel when it is done
	NewSinkTube(ctx context.Context, config ConfigMap) (chan<- Record, error)
}

type TubeFactory interface {
	SourceTubeFactory
	SinkTubeFactory
}

type RecordImpl struct {
	payload    []byte
	commitFunc func()
}

func NewRecordImpl(payload []byte, ackFunc func()) *RecordImpl {
	return &RecordImpl{
		payload:    payload,
		commitFunc: ackFunc,
	}
}

func (e *RecordImpl) GetPayload() []byte {
	return e.payload
}

func (e *RecordImpl) Commit() {
	e.commitFunc()
}
