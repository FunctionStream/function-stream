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

package perf

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/restclient"
	"golang.org/x/time/rate"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type TubeBuilder func(ctx context.Context, config *common.Config) (contube.TubeFactory, error)

type Config struct {
	PulsarURL    string
	RequestRate  float64
	Func         *restclient.Function
	QueueBuilder TubeBuilder
}

type Perf interface {
	Run(context.Context)
}

type perf struct {
	config      *Config
	input       chan<- contube.Record
	output      <-chan contube.Record
	tubeBuilder TubeBuilder
}

func New(config *Config) Perf {
	p := &perf{
		config: config,
	}
	if config.QueueBuilder == nil {
		p.tubeBuilder = func(ctx context.Context, c *common.Config) (contube.TubeFactory, error) {
			return contube.NewPulsarEventQueueFactory(ctx, (&contube.PulsarTubeFactoryConfig{
				PulsarURL: config.PulsarURL,
			}).ToConfigMap())
		}
	} else {
		p.tubeBuilder = config.QueueBuilder
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
			Runtime: &restclient.FunctionRuntime{
				Config: map[string]interface{}{
					common.RuntimeArchiveConfigKey: "./bin/example_basic.wasm",
				},
			},
			Inputs: []string{"test-input-" + strconv.Itoa(rand.Int())},
			Output: "test-output-" + strconv.Itoa(rand.Int()),
		}
	}

	config := &common.Config{
		PulsarURL: p.config.PulsarURL,
	}

	queueFactory, err := p.tubeBuilder(ctx, config)
	if err != nil {
		slog.Error(
			"Failed to create Record Queue Factory",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	p.input, err = queueFactory.NewSinkTube(ctx, (&contube.SinkQueueConfig{
		Topic: f.Inputs[0],
	}).ToConfigMap())
	if err != nil {
		slog.Error(
			"Failed to create Sink Perf Channel",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	p.output, err = queueFactory.NewSourceTube(ctx, (&contube.SourceQueueConfig{
		Topics:  []string{f.Output},
		SubName: "perf",
	}).ToConfigMap())
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
		if !common.SendToChannel(ctx, p.input, contube.NewRecordImpl(jsonBytes, func() {})) {
			return
		}
		go func() {
			e, ok := common.ReceiveFromChannel(ctx, p.output)
			if !ok {
				return
			}
			latencyCh <- time.Since(start).Microseconds()
			payload := e.GetPayload()
			e.Commit()
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
