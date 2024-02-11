/*
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

package perf

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"github.com/functionstream/functionstream/lib"
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
	PulsarURL    string
	RequestRate  float64
	Func         *restclient.Function
	QueueBuilder lib.QueueBuilder
}

type Perf interface {
	Run(context.Context)
}

type perf struct {
	config       *Config
	input        chan<- lib.Event
	output       <-chan lib.Event
	queueBuilder lib.QueueBuilder
}

func New(config *Config) Perf {
	p := &perf{
		config: config,
	}
	if config.QueueBuilder == nil {
		p.queueBuilder = func(ctx context.Context, c *lib.Config) (lib.EventQueueFactory, error) {
			return lib.NewPulsarEventQueueFactory(ctx, c)
		}
	} else {
		p.queueBuilder = config.QueueBuilder
	}
	return p
}

type Person struct {
	Name     string `json:"name"`
	Money    int    `json:"money"`
	Expected int    `json:"expected"`
}

func (p *perf) Run(ctx context.Context) {
	slog.Info(
		"Starting Function stream perf client",
		slog.Any("config", p.config),
	)

	name := "perf-" + strconv.Itoa(rand.Int())
	var f restclient.Function
	if p.config.Func != nil {
		f = *p.config.Func
	} else {
		f = restclient.Function{
			Archive: "./bin/example_basic.wasm",
			Inputs:  []string{"test-input-" + strconv.Itoa(rand.Int())},
			Output:  "test-output-" + strconv.Itoa(rand.Int()),
		}
	}

	config := &lib.Config{
		PulsarURL: p.config.PulsarURL,
	}

	queueFactory, err := p.queueBuilder(ctx, config)
	if err != nil {
		slog.Error(
			"Failed to create Event Queue Factory",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	p.input, err = queueFactory.NewSinkChan(ctx, &lib.SinkQueueConfig{
		Topic: f.Inputs[0],
	})
	if err != nil {
		slog.Error(
			"Failed to create Sink Perf Channel",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	p.output, err = queueFactory.NewSourceChan(ctx, &lib.SourceQueueConfig{
		Topics:  []string{f.Output},
		SubName: "perf",
	})
	if err != nil {
		slog.Error(
			"Failed to create Source Perf Channel",
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

	defer func() {
		res, err := cli.DefaultAPI.ApiV1FunctionFunctionNameDelete(context.Background(), name).Execute()
		if err != nil {
			slog.Error(
				"Failed to delete Function",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
		if res.StatusCode != 200 {
			slog.Error(
				"Failed to delete Function",
				slog.Any("statusCode", res.StatusCode),
			)
			os.Exit(1)
		}
	}()

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
			slog.Info(fmt.Sprintf(`Stats - Total ops: %6.1f ops/s - Failed ops: %6.1f ops/s
			Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				float64(ops)/float64(reportInterval/time.Second),
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
			slog.InfoContext(ctx, "Shutting down perf client")
			return
		}
	}

}

func SendToChannel[T any](ctx context.Context, c chan<- T, e interface{}) bool {
	select {
	case c <- e.(T): // It will panic if `e` is not of type `T` or a type that can be converted to `T`.
		return true
	case <-ctx.Done():
		close(c)
		return false
	}
}

func zeroValue[T any]() T {
	var v T
	return v
}

func ReceiveFromChannel[T any](ctx context.Context, c <-chan T) (T, bool) {
	select {
	case e := <-c:
		return e, true
	case <-ctx.Done():
		return zeroValue[T](), false
	}
}

func (p *perf) generateTraffic(ctx context.Context, latencyCh chan int64, failureCount *int64) {
	limiter := rate.NewLimiter(rate.Limit(p.config.RequestRate), int(p.config.RequestRate))

	count := 0
	for {
		if err := limiter.Wait(ctx); err != nil {
			return
		}
		c := count
		count++
		person := Person{Name: "rbt", Money: c, Expected: c + 1}
		jsonBytes, err := json.Marshal(person)
		if err != nil {
			slog.Error(
				"Failed to marshal Person",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
		start := time.Now()
		if !SendToChannel(ctx, p.input, lib.NewAckableEvent(jsonBytes, func() {})) {
			return
		}
		go func() {
			e, ok := ReceiveFromChannel(ctx, p.output)
			if !ok {
				return
			}
			latencyCh <- time.Since(start).Microseconds()
			payload := e.GetPayload()
			e.Ack()
			var out Person
			err = json.Unmarshal(payload, &out)
			if err != nil {
				slog.Error(
					"Failed to unmarshal Person",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
			if out.Money != out.Expected {
				slog.Error(
					"Unexpected value for money",
					slog.Any("money", out.Money),
				)
				atomic.AddInt64(failureCount, 1)
			}
		}()
	}
}
