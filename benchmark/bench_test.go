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

package benchmark

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/lib"
	"github.com/functionstream/functionstream/perf"
	"github.com/functionstream/functionstream/restclient"
	"github.com/functionstream/functionstream/server"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

func prepareEnv() {
	workingDirectory := os.Getenv("FS_TEST_WORK_DIR")
	if workingDirectory != "" {
		err := os.Chdir(workingDirectory)
		slog.Info("Changing working directory", "working-dir", workingDirectory)
		if err != nil {
			panic(err)
		}
	}

}

func BenchmarkStressForBasicFunc(b *testing.B) {
	prepareEnv()

	s := server.New()
	go s.Run()
	defer func(s *server.Server) {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}(s)

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
		Func: &restclient.Function{
			Archive:  "./bin/example_basic.wasm",
			Inputs:   []string{inputTopic},
			Output:   outputTopic,
			Replicas: &replicas,
		},
	}

	b.ReportAllocs()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(3*time.Second))
	defer cancel()

	profile := "BenchmarkStressForBasicFunc.pprof"
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

	perf.New(pConfig).Run(ctx)

	pprof.StopCPUProfile()
}

func BenchmarkStressForBasicFuncWithMemoryQueue(b *testing.B) {
	prepareEnv()

	memoryQueueFactory := lib.NewMemoryQueueFactory()

	svrConf := &lib.Config{
		QueueBuilder: func(ctx context.Context, config *lib.Config) (lib.EventQueueFactory, error) {
			return memoryQueueFactory, nil
		},
	}

	fm, err := lib.NewFunctionManager(svrConf)
	if err != nil {
		b.Fatal(err)
	}
	s := server.NewWithFM(fm)
	go func() {
		common.RunProcess(func() (io.Closer, error) {
			go s.Run()
			return s, nil
		})
	}()

	inputTopic := "test-input-" + strconv.Itoa(rand.Int())
	outputTopic := "test-output-" + strconv.Itoa(rand.Int())

	replicas := int32(15)

	pConfig := &perf.Config{
		RequestRate: 200000.0,
		Func: &restclient.Function{
			Archive:  "./bin/example_basic.wasm",
			Inputs:   []string{inputTopic},
			Output:   outputTopic,
			Replicas: &replicas,
		},
		QueueBuilder: func(ctx context.Context, c *lib.Config) (lib.EventQueueFactory, error) {
			return memoryQueueFactory, nil
		},
	}

	b.ReportAllocs()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(3*time.Second))
	defer cancel()

	profile := "BenchmarkStressForBasicFunc.pprof"
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

	perf.New(pConfig).Run(ctx)

	pprof.StopCPUProfile()

	err = s.Close()
	if err != nil {
		b.Fatal(err)
	}
}
