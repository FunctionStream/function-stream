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
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/perf"
	"github.com/spf13/cobra"
	"io"
)

var (
	Cmd = &cobra.Command{
		Use:   "perf",
		Short: "Function Stream perf client",
		Long:  `Tool for basic performance tests`,
		Run:   exec,
	}

	config = &perf.Config{}
)

func init() {
	Cmd.Flags().StringVarP(&config.PulsarURL, "pulsar-url", "p", "pulsar://localhost:6650", "Pulsar URL")
	Cmd.Flags().Float64VarP(&config.RequestRate, "rate", "r", 100.0, "Request rate, ops/s")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(runPerf)
}

type closer struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newCloser(ctx context.Context) *closer {
	c := &closer{}
	c.ctx, c.cancel = context.WithCancel(ctx)
	return c
}

func (c *closer) Close() error {
	c.cancel()
	return nil
}

func runPerf() (io.Closer, error) {
	closer := newCloser(context.Background())
	go perf.New(config).Run(closer.ctx)
	return closer, nil
}
