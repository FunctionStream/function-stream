package external

import (
	"context"
	"encoding/json"
	"fmt"
	api "github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/event"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/functionstream/function-stream/pkg/testfunctions"
	"github.com/functionstream/function-stream/pkg/testutil"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"testing"
	"time"
)

type MockInstance struct {
	ctx context.Context
	f   *model.Function
	es  api.EventStorage
	p   *model.Package
}

func (m *MockInstance) Context() context.Context {
	return m.ctx
}

func (m *MockInstance) Function() *model.Function {
	return m.f
}

func (m *MockInstance) Package() *model.Package {
	return m.p
}

func (m *MockInstance) EventStorage() api.EventStorage {
	return m.es
}

func (m *MockInstance) StateStore() api.StateStore {
	return nil
}

func NewMockInstance(ctx context.Context, f *model.Function, es api.EventStorage, p *model.Package) api.Instance {
	return &MockInstance{
		ctx: ctx,
		f:   f,
		es:  es,
		p:   p,
	}
}

type MockEventStorage struct {
	ReadCh   chan api.Event
	WriteCh  chan api.Event
	CommitCh chan string
}

func (m *MockEventStorage) Read(ctx context.Context, topics []model.TopicConfig) (<-chan api.Event, error) {
	return m.ReadCh, nil
}

func (m *MockEventStorage) Write(ctx context.Context, event api.Event, topic model.TopicConfig) error {
	m.WriteCh <- event
	return nil
}

func (m *MockEventStorage) Commit(ctx context.Context, eventId string) error {
	m.CommitCh <- eventId
	return nil
}

func NewMockEventStorage() *MockEventStorage {
	return &MockEventStorage{
		ReadCh:   make(chan api.Event),
		WriteCh:  make(chan api.Event),
		CommitCh: make(chan string, 1000),
	}
}

func TestExternalRuntime(t *testing.T) {
	testSocketPath := fmt.Sprintf("/tmp/%s.sock", t.Name())
	require.NoError(t, os.RemoveAll(testSocketPath))
	testfunctions.SetupFSTarget(t, "unix:"+testSocketPath)
	lis, err := net.Listen("unix", testSocketPath)
	require.NoError(t, err)
	log := testutil.GetTestLogger(t)
	adapter, err := NewExternalAdapter(context.Background(), "test", &Config{
		Listener: lis,
		Logger:   log,
	})
	require.NoError(t, err)
	go testfunctions.NewTestModules().Run(context.Background())
	time.Sleep(1 * time.Second)

	t.Run("Default Module", func(t *testing.T) {
		es := NewMockEventStorage()
		instance := NewMockInstance(context.Background(), &model.Function{
			Name:    "test",
			Package: "test",
			Module:  "default",
		}, es, nil)
		require.NoError(t, adapter.DeployFunction(context.Background(), instance))
		in, err := event.NewStructEvent("1", &testfunctions.Person{
			Name:  "test",
			Money: 1,
		})
		require.NoError(t, err)
		es.ReadCh <- in
		out := <-es.WriteCh
		require.Equal(t, "", out.ID())
		p := &testfunctions.Person{}
		require.NoError(t, json.NewDecoder(out.Payload()).Decode(p))
		require.Equal(t, 2, p.Money)
	})

	t.Run("Counter Module", func(t *testing.T) {
		es := NewMockEventStorage()
		instance := NewMockInstance(context.Background(), &model.Function{
			Name:    "test",
			Package: "test",
			Module:  "counter",
		}, es, nil)
		require.NoError(t, adapter.DeployFunction(context.Background(), instance))
		in, err := event.NewStructEvent("1", &testfunctions.Counter{
			Count: 1,
		})
		require.NoError(t, err)
		es.ReadCh <- in
		out := <-es.WriteCh
		require.Equal(t, "", out.ID())
		c := &testfunctions.Counter{}
		require.NoError(t, json.NewDecoder(out.Payload()).Decode(c))
		require.Equal(t, 2, c.Count)
	})

	t.Run("Source Module", func(t *testing.T) {
		es := NewMockEventStorage()
		instance := NewMockInstance(context.Background(), &model.Function{
			Name:    "test",
			Package: "test",
			Module:  "test-source",
		}, es, nil)
		require.NoError(t, adapter.DeployFunction(context.Background(), instance))
		for i := 0; i < 10; i++ {
			out := <-es.WriteCh
			require.Equal(t, "", out.ID())
			r := &testfunctions.TestRecord{}
			require.NoError(t, json.NewDecoder(out.Payload()).Decode(r))
			require.NoError(t, out.Commit(context.Background()))
			require.Equal(t, i, r.ID)
		}
	})
}
