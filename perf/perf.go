package perf

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/bmizerany/perks/quantile"
	"github.com/functionstream/functionstream/restclient"
	"golang.org/x/time/rate"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type Config struct {
	PulsarURL   string
	RequestRate float64
}

type Perf interface {
	Run(context.Context)
}

type perf struct {
	config   Config
	producer pulsar.Producer
	consumer pulsar.Consumer
}

func New(config Config) Perf {
	return &perf{
		config: config,
	}
}

type Person struct {
	Name  string `json:"name"`
	Money int    `json:"money"`
}

func (p *perf) Run(ctx context.Context) {
	slog.Info(
		"Starting Function stream perf client",
		slog.Any("config", p.config),
	)

	name := "perf-" + strconv.Itoa(rand.Int())
	f := restclient.Function{
		Archive: "./bin/example_basic.wasm",
		Inputs:  []string{"test-input-" + strconv.Itoa(rand.Int())},
		Output:  "test-output-" + strconv.Itoa(rand.Int()),
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: p.config.PulsarURL,
	})
	if err != nil {
		slog.Error(
			"Failed to create Pulsar client",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	p.consumer, err = client.Subscribe(pulsar.ConsumerOptions{
		Topic:            f.Output,
		SubscriptionName: "perf",
	})
	if err != nil {
		slog.Error(
			"Failed to create Pulsar Consumer",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	p.producer, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: f.Inputs[0],
	})
	if err != nil {
		slog.Error(
			"Failed to create Pulsar Producer",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	cfg := restclient.NewConfiguration()
	cli := restclient.NewAPIClient(cfg)

	res, err := cli.DefaultAPI.ApiV1FunctionFunctionNamePost(context.Background(), name).Function(f).Execute()
	if err != nil {
		slog.Error(
			"Failed to create Create Function",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	if res.StatusCode != 200 {
		slog.Error(
			"Failed to create Create Function",
			slog.Any("statusCode", res.StatusCode),
		)
		os.Exit(1)
	}

	latencyCh := make(chan int64)
	var failureCount int64
	go p.generateTraffic(ctx, latencyCh, &failureCount)

	reportInterval := time.Second
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	q := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	ops := 0
	for {
		select {
		case <-ticker.C:
			r := float64(ops) / float64(reportInterval/time.Second)
			slog.Info(fmt.Sprintf(`Stats - Total ops: %6.1f ops/s - Failed ops: %6.1f ops/s
			Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				r,
				float64(failureCount)/float64(reportInterval/time.Second),
				q.Query(0.5),
				q.Query(0.95),
				q.Query(0.99),
				q.Query(0.999),
				q.Query(1.0),
			))
			q.Reset()
			ops = 0
			atomic.StoreInt64(&failureCount, 0)
		case l := <-latencyCh:
			ops++
			q.Insert(float64(l) / 1000.0) // Convert to millis
		case <-ctx.Done():
			return
		}
	}

}

func (p *perf) generateTraffic(ctx context.Context, latencyCh chan int64, failureCount *int64) {
	limiter := rate.NewLimiter(rate.Limit(p.config.RequestRate), int(p.config.RequestRate))

	msgCh := p.consumer.Chan()
	count := 0
	next := make(chan bool, int(p.config.RequestRate))
	for {
		if err := limiter.Wait(ctx); err != nil {
			return
		}
		c := count
		count++

		go func() {
			person := Person{Name: "rbt", Money: c}
			jsonBytes, err := json.Marshal(person)
			if err != nil {
				slog.Error(
					"Failed to marshal Person",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
			start := time.Now()
			p.producer.SendAsync(ctx, &pulsar.ProducerMessage{
				Payload: jsonBytes,
			}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if err != nil {
					slog.Warn(
						"Failed to produce message",
						slog.Any("error", err),
					)
				}
			})
			next <- true
			m := <-msgCh
			latencyCh <- time.Since(start).Microseconds()
			payload := m.Payload()
			var out Person
			err = json.Unmarshal(payload, &out)
			if err != nil {
				slog.Error(
					"Failed to unmarshal Person",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
			if out.Money != c+1 {
				slog.Error(
					"Unexpected value for money",
					slog.Any("money", out.Money),
				)
				atomic.AddInt64(failureCount, 1)
			}
		}()
		<-next
	}
}
