package perf

import (
	"context"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/perf"
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

	config = perf.Config{}
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
