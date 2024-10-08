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
	"encoding/json"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
)

var (
	ErrTubeNotImplemented       = errors.New("tube not implemented")
	ErrSinkTubeNotImplemented   = errors.Wrap(ErrTubeNotImplemented, "sink tube not implemented")
	ErrSourceTubeNotImplemented = errors.Wrap(ErrTubeNotImplemented, "source tube not implemented")
)

type Record interface {
	GetPayload() []byte
	GetSchema() string
	Commit()
}

type SourceQueueConfig struct {
	Topics  []string `json:"inputs" validate:"required"`
	SubName string   `json:"subscription-name" validate:"required"`
}

type SinkQueueConfig struct {
	Topic string `json:"output" validate:"required"`
}

func (c *SourceQueueConfig) ToConfigMap() ConfigMap {
	configMap, _ := ToConfigMap(c)
	return configMap
}

func (c *SinkQueueConfig) ToConfigMap() ConfigMap {
	configMap, _ := ToConfigMap(c)
	return configMap
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

func (c ConfigMap) ToConfigStruct(v any) error {
	jsonData, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(jsonData, v); err != nil {
		return err
	}
	validate := validator.New()
	return validate.Struct(v)
}

func ToConfigMap(v any) (ConfigMap, error) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var result ConfigMap
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return nil, err
	}
	return result, nil
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
	schema     string
	commitFunc func()
}

func NewRecordImpl(payload []byte, ackFunc func()) *RecordImpl {
	return &RecordImpl{
		payload:    payload,
		commitFunc: ackFunc,
	}
}

func NewStructRecord(payload any, ackFunc func()) (*RecordImpl, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &RecordImpl{
		payload:    data,
		commitFunc: ackFunc,
	}, nil
}

func NewSchemaRecordImpl(payload []byte, schema string, ackFunc func()) *RecordImpl {
	return &RecordImpl{
		payload:    payload,
		schema:     schema,
		commitFunc: ackFunc,
	}
}

func (e *RecordImpl) GetPayload() []byte {
	return e.payload
}

func (e *RecordImpl) GetSchema() string {
	return e.schema
}

func (e *RecordImpl) Commit() {
	if e.commitFunc != nil {
		e.commitFunc()
	}
}

type emptyTubeFactory struct {
}

func NewEmptyTubeFactory() TubeFactory {
	return &emptyTubeFactory{}
}

func (f *emptyTubeFactory) NewSourceTube(ctx context.Context, config ConfigMap) (<-chan Record, error) {
	ch := make(chan Record)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *emptyTubeFactory) NewSinkTube(ctx context.Context, config ConfigMap) (chan<- Record, error) {
	ch := make(chan Record)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				continue
			}
		}
	}()
	return ch, nil
}
