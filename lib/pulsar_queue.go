package lib

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/pkg/errors"
	"log/slog"
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

func NewPulsarEventQueueFactory() (func(ctx context.Context, f *model.Function) (EventQueue, error), error) {
	pc, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: common.GetConfig().PulsarURL,
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, f *model.Function) (EventQueue, error) {
		consumer, err := pc.Subscribe(pulsar.ConsumerOptions{
			Topics:           f.Inputs,
			SubscriptionName: fmt.Sprintf("function-stream-%s", f.Name),
			Type:             pulsar.Failover,
		})
		if err != nil {
			return nil, errors.Wrap(err, "Error creating consumer")
		}
		producer, err := pc.CreateProducer(pulsar.ProducerOptions{
			Topic: f.Output,
		})
		if err != nil {
			return nil, errors.Wrap(err, "Error creating producer")
		}
		handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
			if errors.Is(err, context.Canceled) {
				slog.InfoContext(ctx, "function instance has been stopped")
				return
			}
			slog.ErrorContext(ctx, message, args...)
		}
		recvChan := make(chan Event)
		go func() {
			for msg := range consumer.Chan() {
				recvChan <- func() ([]byte, func()) {
					return msg.Payload(), func() {
						err := consumer.Ack(msg)
						if err != nil {
							handleErr(ctx, err, "Error acknowledging message", "error", err)
							return
						}
					}
				}
			}
		}()
		sendChan := make(chan Event)
		go func() {
			for e := range sendChan {
				payload, ackFunc := e()
				producer.SendAsync(ctx, &pulsar.ProducerMessage{
					Payload: payload,
				}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
					if err != nil {
						handleErr(ctx, err, "Error sending message", "error", err, "messageId", id)
						return
					}
					ackFunc()
				})
			}
		}()
		return &PulsarEventQueue{
			sendChan: sendChan,
			recvChan: recvChan,
			consumer: consumer,
			producer: producer,
		}, nil
	}, nil
}

type PulsarEventQueue struct {
	sendChan chan<- Event
	recvChan <-chan Event
	consumer pulsar.Consumer
	producer pulsar.Producer
}

func (q *PulsarEventQueue) GetSendChan() chan<- Event {
	return q.sendChan
}

func (q *PulsarEventQueue) GetRecvChan() <-chan Event {
	return q.recvChan
}

func (q *PulsarEventQueue) Close() {
	q.consumer.Close()
}
