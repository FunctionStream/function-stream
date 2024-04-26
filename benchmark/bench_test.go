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

package benchmark

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/functionstream/function-stream/admin/client"
	adminutils "github.com/functionstream/function-stream/admin/utils"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/perf"
	"github.com/functionstream/function-stream/server"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

func BenchmarkStressForBasicFunc(b *testing.B) {
	s, err := server.NewDefaultServer()
	if err != nil {
		b.Fatal(err)
	}
	svrCtx, svrCancel := context.WithCancel(context.Background())
	go s.Run(svrCtx)
	defer func() {
		svrCancel()
	}()

	inputTopic := "test-input-" + strconv.Itoa(rand.Int())
	outputTopic := "test-output-" + strconv.Itoa(rand.Int())
	cfg := &pulsaradmin.Config{}
	admin, err := pulsaradmin.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	replicas := int32(5)
	createTopic := func(t string) {
		tn, err := utils.GetTopicName(t)
		if err != nil {
			panic(err)
		}
		err = admin.Topics().Create(*tn, int(replicas))
		if err != nil {
			panic(err)
		}

	}
	createTopic(inputTopic)
	createTopic(outputTopic)

	pConfig := &perf.Config{
		PulsarURL:   "pulsar://localhost:6650",
		RequestRate: 200000.0,
		Func: &adminclient.ModelFunction{
			Runtime: adminclient.ModelRuntimeConfig{
				Config: map[string]interface{}{
					common.RuntimeArchiveConfigKey: "../bin/example_basic.wasm",
				},
			},
			Source:   adminutils.MakeQueueSourceTubeConfig("fs", inputTopic),
			Sink:     adminutils.MakeQueueSinkTubeConfig(outputTopic),
			Replicas: replicas,
		},
	}

	b.ReportAllocs()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
	defer cancel()

	profile := b.Name() + ".pprof"
	file, err := os.Create(profile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()

	err = pprof.StartCPUProfile(file)
	if err != nil {
		b.Fatal(err)
	}

	<-s.WaitForReady(context.Background())
	perf.New(pConfig).Run(ctx)

	pprof.StopCPUProfile()
}

func BenchmarkStressForBasicFuncWithMemoryQueue(b *testing.B) {
	memoryQueueFactory := contube.NewMemoryQueueFactory(context.Background())

	s, err := server.NewServer(server.WithFunctionManager(fs.WithDefaultTubeFactory(memoryQueueFactory)))
	if err != nil {
		b.Fatal(err)
	}
	svrCtx, svrCancel := context.WithCancel(context.Background())
	go s.Run(svrCtx)
	defer func() {
		svrCancel()
	}()

	inputTopic := "test-input-" + strconv.Itoa(rand.Int())
	outputTopic := "test-output-" + strconv.Itoa(rand.Int())

	replicas := int32(1)

	pConfig := &perf.Config{
		RequestRate: 200000.0,
		Func: &adminclient.ModelFunction{
			Runtime: adminclient.ModelRuntimeConfig{
				Config: map[string]interface{}{
					common.RuntimeArchiveConfigKey: "../bin/example_basic.wasm",
				},
			},
			Source:   adminutils.MakeQueueSourceTubeConfig("fs", inputTopic),
			Sink:     adminutils.MakeQueueSinkTubeConfig(outputTopic),
			Replicas: replicas,
		},
		QueueBuilder: func(ctx context.Context) (contube.TubeFactory, error) {
			return memoryQueueFactory, nil
		},
	}

	b.ReportAllocs()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
	defer cancel()

	profile := b.Name() + ".pprof"
	file, err := os.Create(profile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()

	err = pprof.StartCPUProfile(file)
	if err != nil {
		b.Fatal(err)
	}

	<-s.WaitForReady(context.Background())
	perf.New(pConfig).Run(ctx)

	pprof.StopCPUProfile()
}
