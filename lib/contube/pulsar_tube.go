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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"log/slog"
	"sync/atomic"
)

const (
	PulsarURLKey = "pulsarURL"
)

type PulsarTubeFactorConfig struct {
	PulsarURL string
}

func NewPulsarTubeFactorConfig(configMap ConfigMap) *PulsarTubeFactorConfig {
	var result PulsarTubeFactorConfig
	if pulsarURL, ok := configMap[PulsarURLKey].(string); ok {
		result.PulsarURL = pulsarURL
	}
	return &result
}

func (c *PulsarTubeFactorConfig) ToConfigMap() ConfigMap {
	return ConfigMap{
		PulsarURLKey: c.PulsarURL,
	}
}

type PulsarEventQueueFactory struct {
	newSourceChan func(ctx context.Context, config *SourceQueueConfig) (<-chan Record, error)
	newSinkChan   func(ctx context.Context, config *SinkQueueConfig) (chan<- Record, error)
}

func (f *PulsarEventQueueFactory) NewSourceTube(ctx context.Context, configMap ConfigMap) (<-chan Record, error) {
	config := NewSourceQueueConfig(configMap)
	return f.newSourceChan(ctx, config)
}

func (f *PulsarEventQueueFactory) NewSinkTube(ctx context.Context, configMap ConfigMap) (chan<- Record, error) {
	config := NewSinkQueueConfig(configMap)
	return f.newSinkChan(ctx, config)
}

func NewPulsarEventQueueFactory(ctx context.Context, configMap ConfigMap) (TubeFactory, error) {
	config := NewPulsarTubeFactorConfig(configMap)
	pc, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: config.PulsarURL,
	})
	if err != nil {
		return nil, err
	}
	var closed atomic.Bool // TODO: Remove this after the bug of Producer.Flush is fixed
	go func() {
		<-ctx.Done()
		slog.InfoContext(ctx, "Closing Pulsar event queue factory", slog.Any("config", config))
		closed.Store(true)
		pc.Close()
	}()
	handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
		if errors.Is(err, context.Canceled) {
			slog.InfoContext(ctx, "Pulsar queue cancelled", slog.Any("config", config))
			return
		}
		extraArgs := append(args, slog.Any("config", config), slog.Any("error", err))
		slog.ErrorContext(ctx, message, extraArgs...)
	}
	log := func(message string, config interface{}, args ...interface{}) {
		slog.InfoContext(ctx, message, append(args, slog.Any("config", config))...)
	}
	return &PulsarEventQueueFactory{
		newSourceChan: func(ctx context.Context, config *SourceQueueConfig) (<-chan Record, error) {
			c := make(chan Record)
			consumer, err := pc.Subscribe(pulsar.ConsumerOptions{
				Topics:           config.Topics,
				SubscriptionName: config.SubName,
				Type:             pulsar.Failover,
			})
			if err != nil {
				return nil, errors.Wrap(err, "Error creating consumer")
			}
			log("Pulsar source queue created", config)
			go func() {
				defer log("Pulsar source queue closed", config)
				defer consumer.Close()
				defer close(c)
				for msg := range consumer.Chan() {
					c <- NewRecordImpl(msg.Payload(), func() {
						err := consumer.Ack(msg)
						if err != nil {
							handleErr(ctx, err, "Error acknowledging message", "error", err)
							return
						}
					})
				}
			}()
			return c, nil
		},
		newSinkChan: func(ctx context.Context, config *SinkQueueConfig) (chan<- Record, error) {
			c := make(chan Record)
			producer, err := pc.CreateProducer(pulsar.ProducerOptions{
				Topic: config.Topic,
			})
			if err != nil {
				return nil, errors.Wrap(err, "Error creating producer")
			}
			log("Pulsar sink queue created", config)
			go func() {
				defer log("Pulsar sink queue closed", config)
				defer producer.Close()
				flush := func() {
					if closed.Load() {
						return
					}
					err := producer.Flush()
					if err != nil {
						handleErr(ctx, err, "Error flushing producer", "error", err)
					}
				}
				for {
					select {
					case e, ok := <-c:
						if !ok {
							flush()
							return
						}
						producer.SendAsync(ctx, &pulsar.ProducerMessage{
							Payload: e.GetPayload(),
						}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
							if err != nil {
								handleErr(ctx, err, "Error sending message", "error", err, "messageId", id)
								return
							}
							e.Commit()
						})
					case <-ctx.Done():
						flush()
						return
					}
				}
			}()
			return c, nil
		},
	}, nil
}
