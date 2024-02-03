package benchmark

import (
	"context"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/perf"
	"github.com/functionstream/functionstream/server"
	"io"
	"log/slog"
	"os"
	"runtime/pprof"
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
	go func() {
		common.RunProcess(func() (io.Closer, error) {
			go s.Run()
			return s, nil
		})
	}()

	pConfig := perf.Config{
		PulsarURL:   "pulsar://localhost:6650",
		RequestRate: 500.0,
	}

	b.ReportAllocs()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
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
