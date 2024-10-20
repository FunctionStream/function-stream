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
	"time"

	"github.com/functionstream/function-stream/common"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type NatsTubeFactoryConfig struct {
	NatsURL string `json:"nats_url"`
}

type NatsEventQueueFactory struct {
	nc *nats.Conn
}

type NatsSourceTubeConfig struct {
	Subject string `json:"subject" validate:"required"`
}

func (n NatsEventQueueFactory) NewSourceTube(ctx context.Context, configMap ConfigMap) (<-chan Record, error) {
	config := &NatsSourceTubeConfig{}
	if err := configMap.ToConfigStruct(config); err != nil {
		return nil, err
	}
	c := make(chan Record)
	sub, err := n.nc.SubscribeSync(config.Subject)
	if err != nil {
		return nil, err
	}
	log := common.NewDefaultLogger()
	go func() {
		for {
			msg, err := sub.NextMsg(10 * time.Millisecond)
			if err != nil {
				if !errors.Is(err, nats.ErrTimeout) {
					log.Error(err, "Failed to get next message", "subject", config.Subject)
				}
				continue
			}
			select {
			case c <- NewRecordImpl(msg.Data, func() {
				_ = msg.Ack()
			}): // do nothing
			case <-ctx.Done():
				return
			}
		}
	}()
	return c, nil
}

type NatsSinkTubeConfig struct {
	Subject string `json:"subject" validate:"required"`
}

func (n NatsEventQueueFactory) NewSinkTube(ctx context.Context, configMap ConfigMap) (chan<- Record, error) {
	config := &NatsSinkTubeConfig{}
	if err := configMap.ToConfigStruct(config); err != nil {
		return nil, err
	}
	c := make(chan Record)
	log := common.NewDefaultLogger()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-c:
				if !ok {
					return
				}
				err := n.nc.Publish(config.Subject, event.GetPayload())
				log.Info("Published message", "subject", config.Subject, "err", err)
				if err != nil {
					log.Error(err, "Failed to publish message", "subject", config.Subject)
					continue
				}
				event.Commit()
			}
		}
	}()
	return c, nil
}

func NewNatsEventQueueFactory(ctx context.Context, configMap ConfigMap) (TubeFactory, error) {
	config := &NatsTubeFactoryConfig{}
	if err := configMap.ToConfigStruct(config); err != nil {
		return nil, err
	}
	if config.NatsURL == "" {
		config.NatsURL = "nats://localhost:4222"
	}
	nc, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, err
	}
	log := common.NewDefaultLogger()
	go func() {
		<-ctx.Done()
		// Close the nats queue factory
		log.Info("Closing nats queue factory", "url", config.NatsURL)
		err := nc.Drain()
		if err != nil {
			log.Error(err, "Failed to drain nats connection", "url", config.NatsURL)
		}
	}()
	return &NatsEventQueueFactory{
		nc: nc,
	}, nil
}
