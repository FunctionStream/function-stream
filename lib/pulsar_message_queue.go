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

func NewPulsarEventQueueFactory() (*PulsarMessageQueueFactory, error) {
	pc, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: common.GetConfig().PulsarURL,
	})
	if err != nil {
		return nil, err
	}
	return &PulsarMessageQueueFactory{
		pc: pc,
	}, nil
}

type PulsarMessageQueue struct {
	sendChan chan<- SinkEvent
	recvChan <-chan SourceEvent
	consumer pulsar.Consumer
	producer pulsar.Producer
}

func (factor *PulsarMessageQueueFactory) NewEventQueue(ctx context.Context, f *model.Function) (EventQueue, error) {
	consumer, err := factor.pc.Subscribe(pulsar.ConsumerOptions{
		Topics:           f.Inputs,
		SubscriptionName: fmt.Sprintf("function-stream-%s", f.Name),
		Type:             pulsar.Failover,
	})
	if err != nil {
		return nil, errors.Wrap(err, "Error creating consumer")
	}
	producer, err := factor.pc.CreateProducer(pulsar.ProducerOptions{
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
	recvChan := make(chan SourceEvent)
	go func() {
		for msg := range consumer.Chan() {
			recvChan <- &PulsarEvent{msg: msg}
		}
	}()
	sendChan := make(chan SinkEvent)
	go func() {
		for e := range sendChan {
			producer.SendAsync(ctx, &pulsar.ProducerMessage{
				Payload: e.GetPayload(),
			}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if err != nil {
					handleErr(ctx, err, "Error sending message", "error", err, "messageId", id)
					return
				}
				err = consumer.Ack(e.GetSourceEvent().(*PulsarEvent).msg)
				if err != nil {
					handleErr(ctx, err, "Error acknowledging message", "error", err, "messageId", id)
					return
				}
			})
		}
	}()
	return &PulsarMessageQueue{
		sendChan: sendChan,
		recvChan: recvChan,
		consumer: consumer,
		producer: producer,
	}, nil
}

func (q *PulsarMessageQueue) GetSendChan() chan<- SinkEvent {
	return q.sendChan
}

func (q *PulsarMessageQueue) GetRecvChan() <-chan SourceEvent {
	return q.recvChan
}

func (q *PulsarMessageQueue) Close() {
	q.consumer.Close()
}
