package lib

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common/model"
	"github.com/pkg/errors"
	"log/slog"
	"sync"
)

type PulsarMessageQueueFactory struct {
	pc pulsar.Client
}

type PulsarEvent struct {
	msg pulsar.Message
}

func (e *PulsarEvent) GetPayload() []byte {
	return e.msg.Payload()
}

func NewPulsarEventQueueFactory(ctx context.Context, config *Config) (func(ctx context.Context, config *QueueConfig, f *model.Function) (EventQueue, error), error) {
	pc, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: config.PulsarURL,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			pc.Close()
		}
	}()
	return func(ctx context.Context, config *QueueConfig, f *model.Function) (EventQueue, error) {
		handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
			if errors.Is(err, context.Canceled) {
				slog.InfoContext(ctx, "function instance has been stopped")
				return
			}
			slog.ErrorContext(ctx, message, args...)
		}
		initRecvChan := &sync.Once{}
		initSendChan := &sync.Once{}
		return &PulsarEventQueue{
			getRecvChan: func() (c <-chan Event, e error) {
				var recvChan chan Event
				initRecvChan.Do(func() {
					recvChan = make(chan Event)
					consumer, err := pc.Subscribe(pulsar.ConsumerOptions{
						Topics:           config.Inputs,
						SubscriptionName: fmt.Sprintf("function-stream-%s", f.Name),
						Type:             pulsar.Failover,
					})
					if err != nil {
						e = errors.Wrap(err, "Error creating consumer")
						return
					}
					go func() {
						defer consumer.Close()
						for msg := range consumer.Chan() {
							recvChan <- NewAckableEvent(msg.Payload(), func() {
								err := consumer.Ack(msg)
								if err != nil {
									handleErr(ctx, err, "Error acknowledging message", "error", err)
									return
								}
							})
						}
					}()
				})
				return recvChan, err
			},
			getSendChan: func() (c chan<- Event, e error) {
				var sendChan chan Event
				initSendChan.Do(func() {
					sendChan = make(chan Event)
					producer, err := pc.CreateProducer(pulsar.ProducerOptions{
						Topic: config.Output,
					})
					if err != nil {
						e = errors.Wrap(err, "Error creating producer")
					}
					go func() {
						for e := range sendChan {
							producer.SendAsync(ctx, &pulsar.ProducerMessage{
								Payload: e.GetPayload(),
							}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
								if err != nil {
									handleErr(ctx, err, "Error sending message", "error", err, "messageId", id)
									return
								}
								e.Ack()
							})
						}
					}()
				})
				return sendChan, nil
			},
		}, nil
	}, nil
}

type PulsarEventQueue struct {
	getRecvChan func() (<-chan Event, error)
	getSendChan func() (chan<- Event, error)
}

func (q *PulsarEventQueue) GetSendChan() (c chan<- Event, e error) {
	return q.getSendChan()
}

func (q *PulsarEventQueue) GetRecvChan() (c <-chan Event, err error) {
	return q.getRecvChan()
}
