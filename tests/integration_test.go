package tests

import (
	"context"
	"encoding/json"
	"github.com/functionstream/function-stream/apiclient"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/functionstream/function-stream/pkg/testfunctions"
	"github.com/functionstream/function-stream/pkg/testutil"
	"github.com/functionstream/function-stream/server"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestIntegration(t *testing.T) {
	l := testutil.GetTestLogger(t)
	cfgContent := `
listen-address: ":17300"
package-loader:
  type: memory
event-storage:
  type: memory
state-store:
  type: memory
runtimes:
  - type: external
    config:
      address: ":17400"
`
	cfgPath, cleanup := testutil.CreateTempFile(t, cfgContent, "*.yaml")
	defer cleanup()
	cfg, err := server.LoadConfigFromFile(cfgPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svr, err := server.NewServer(ctx, cfg, &server.ServerOption{
		Logger: l,
	})
	require.NoError(t, err)
	go func() {
		err := svr.Run(ctx)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		require.NoError(t, err)
	}()
	<-svr.WaitForReady(ctx)

	apiCliCfg := apiclient.NewConfiguration()
	apiCliCfg.Debug = true
	apiCliCfg.Host = "localhost:17300"
	apiCli := apiclient.NewAPIClient(apiCliCfg)

	pkg := &model.Package{
		Name: "test-pkg",
		Type: "external",
		Modules: map[string]model.ModuleConfig{
			"default":     {},
			"counter":     {},
			"test-source": {},
			"test-sink":   {},
		},
	}

	err = apiCli.PackageService().Create(ctx, pkg)
	require.NoError(t, err)
	readPkg, err := apiCli.PackageService().Read(ctx, pkg.Name)
	require.NoError(t, err)
	require.Equal(t, pkg, readPkg)
	testfunctions.SetupFSTarget(t, "dns:///localhost:17400")
	go testfunctions.NewTestModules().Run(context.Background())
	time.Sleep(1 * time.Second)

	consume := func(topic string) (<-chan string, context.CancelFunc) {
		ctx, cancel := context.WithCancel(ctx)
		recvCh, _ := apiCli.EventsService().Consume(ctx, "output")
		return recvCh, cancel
	}

	t.Run("Default Module", func(t *testing.T) {
		f := model.Function{
			Name:    "test-default",
			Package: pkg.Name,
			Module:  "default",
			Sources: []model.TopicConfig{
				{
					Name: "input",
				},
			},
			Sink: model.TopicConfig{
				Name: "output",
			},
		}
		err = apiCli.FunctionService().Deploy(ctx, &f)
		require.NoError(t, err)

		recvCh, cancel := consume("output")
		defer cancel()
		time.Sleep(1 * time.Second)
		p := &testfunctions.Person{
			Name:  "test",
			Money: 1,
		}
		require.NoError(t, apiCli.EventsService().Produce(ctx, "input", p))
		output := <-recvCh
		require.NoError(t, json.Unmarshal([]byte(output), p))
		require.Equal(t, 2, p.Money)

		require.NoError(t, apiCli.FunctionService().Delete(ctx, f.Name))
	})

	t.Run("Counter Module", func(t *testing.T) {
		f := model.Function{
			Name:    "test-default",
			Package: pkg.Name,
			Module:  "counter",
			Sources: []model.TopicConfig{
				{
					Name: "input",
				},
			},
			Sink: model.TopicConfig{
				Name: "output",
			},
		}
		err = apiCli.FunctionService().Deploy(ctx, &f)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)

		recvCh, cancel := consume("output")
		defer cancel()
		e := &testfunctions.Counter{
			Count: 1,
		}
		require.NoError(t, apiCli.EventsService().Produce(ctx, "input", e))
		output := <-recvCh
		require.NoError(t, json.Unmarshal([]byte(output), e))
		require.Equal(t, 2, e.Count)

		require.NoError(t, apiCli.FunctionService().Delete(ctx, f.Name))
	})

	t.Run("Source Module", func(t *testing.T) {
		f := model.Function{
			Name:    "test-default",
			Package: pkg.Name,
			Module:  "test-source",
			Sources: []model.TopicConfig{
				{
					Name: "input",
				},
			},
			Sink: model.TopicConfig{
				Name: "output",
			},
		}
		err = apiCli.FunctionService().Deploy(ctx, &f)
		require.NoError(t, err)
		recvCh, cancel := consume("output")
		defer cancel()
		time.Sleep(1 * time.Second)

		for i := 0; i < 10; i++ {
			output := <-recvCh
			r := &testfunctions.TestRecord{}
			require.NoError(t, json.Unmarshal([]byte(output), r))
			require.Equal(t, i, r.ID)
			require.Equal(t, "test", r.Name)
		}
		require.NoError(t, apiCli.FunctionService().Delete(ctx, f.Name))
	})

	t.Run("Sink Module", func(t *testing.T) {
		f := model.Function{
			Name:    "test-default",
			Package: pkg.Name,
			Module:  "test-sink",
			Sources: []model.TopicConfig{
				{
					Name: "input",
				},
			},
			Sink: model.TopicConfig{
				Name: "output",
			},
		}
		err = apiCli.FunctionService().Deploy(ctx, &f)
		require.NoError(t, err)
		sinkCh := make(chan testfunctions.Counter)
		go func() {
			for i := 0; i < 10; i++ {
				e := <-sinkCh
				require.Equal(t, i, e.Count)
			}
		}()
		time.Sleep(1 * time.Second)
		for i := 0; i < 10; i++ {
			e := &testfunctions.Counter{
				Count: i,
			}
			require.NoError(t, apiCli.EventsService().Produce(ctx, "input", e))
			sinkCh <- *e
			require.Equal(t, i, e.Count)
		}
		require.NoError(t, apiCli.FunctionService().Delete(ctx, f.Name))
	})
}
