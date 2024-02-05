package lib

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"log/slog"
)

type PulsarEventQueueFactory struct {
	newSourceChan func(ctx context.Context, config *SourceQueueConfig) (<-chan Event, error)
	newSinkChan   func(ctx context.Context, config *SinkQueueConfig) (chan<- Event, error)
}

func (f *PulsarEventQueueFactory) NewSourceChan(ctx context.Context, config *SourceQueueConfig) (<-chan Event, error) {
	return f.newSourceChan(ctx, config)
}

func (f *PulsarEventQueueFactory) NewSinkChan(ctx context.Context, config *SinkQueueConfig) (chan<- Event, error) {
	return f.newSinkChan(ctx, config)
}

func NewPulsarEventQueueFactory(ctx context.Context, config *Config) (EventQueueFactory, error) {
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
	handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
		if errors.Is(err, context.Canceled) {
			slog.InfoContext(ctx, "function instance has been stopped")
			return
		}
		slog.ErrorContext(ctx, message, args...)
	}
	return &PulsarEventQueueFactory{
		newSourceChan: func(ctx context.Context, config *SourceQueueConfig) (<-chan Event, error) {
			c := make(chan Event)
			consumer, err := pc.Subscribe(pulsar.ConsumerOptions{
				Topics:           config.Topics,
				SubscriptionName: config.SubName,
				Type:             pulsar.Failover,
			})
			if err != nil {
				return nil, errors.Wrap(err, "Error creating consumer")
			}
			go func() {
				defer consumer.Close()
				for msg := range consumer.Chan() {
					c <- NewAckableEvent(msg.Payload(), func() {
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
		newSinkChan: func(ctx context.Context, config *SinkQueueConfig) (chan<- Event, error) {
			c := make(chan Event)
			producer, err := pc.CreateProducer(pulsar.ProducerOptions{
				Topic: config.Topic,
			})
			if err != nil {
				return nil, errors.Wrap(err, "Error creating producer")
			}
			go func() {
				defer producer.Close()
				for e := range c {
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
			return c, nil
		},
	}, nil
}
