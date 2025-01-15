package external

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/clients/gofs"
	gofsapi "github.com/functionstream/function-stream/clients/gofs/api"
	"github.com/functionstream/function-stream/common"
	api "github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/event"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"net"
	"os"
	"testing"
	"time"
)

type Person struct {
	Name     string `json:"name"`
	Money    int    `json:"money"`
	Expected int    `json:"expected"`
}

type Counter struct {
	Count int `json:"count"`
}

type testRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var log = common.NewDefaultLogger()

type TestFunction struct {
}

func (f *TestFunction) Init(_ gofsapi.FunctionContext) error {
	return nil
}

func (f *TestFunction) Handle(_ gofsapi.FunctionContext, event gofsapi.Event[Person]) (gofsapi.Event[Person], error) {
	p := event.Data()
	p.Money += 1
	return gofsapi.NewEvent(p), nil
}

type TestCounterFunction struct {
}

func (f *TestCounterFunction) Init(ctx gofsapi.FunctionContext) error {
	return nil
}

func (f *TestCounterFunction) Handle(_ gofsapi.FunctionContext, event gofsapi.Event[Counter]) (gofsapi.Event[Counter], error) {
	c := event.Data()
	c.Count += 1
	return gofsapi.NewEvent(c), nil
}

type TestSource struct {
}

func (f *TestSource) Init(_ gofsapi.FunctionContext) error {
	return nil
}

func (f *TestSource) Handle(_ gofsapi.FunctionContext, emit func(context.Context, gofsapi.Event[testRecord]) error) error {
	for i := 0; i < 10; i++ {
		err := emit(context.Background(), gofsapi.NewEvent(&testRecord{
			ID:   i,
			Name: "test",
		}))
		if err != nil {
			log.Error(err, "failed to emit record")
		}
	}
	return nil
}

type TestSink struct {
	sinkCh chan Counter
}

func (f *TestSink) Init(_ gofsapi.FunctionContext) error {
	return nil
}

func (f *TestSink) Handle(ctx gofsapi.FunctionContext, event gofsapi.Event[Counter]) error {
	f.sinkCh <- *event.Data()
	return event.Commit(ctx)
}

type TestModules struct {
	testFunction *TestFunction
	testCounter  *TestCounterFunction
	testSource   *TestSource
	testSink     *TestSink
}

func NewTestModules() *TestModules {
	return &TestModules{
		testFunction: &TestFunction{},
		testCounter:  &TestCounterFunction{},
		testSource:   &TestSource{},
		testSink: &TestSink{
			sinkCh: make(chan Counter),
		},
	}
}

func (t *TestModules) Run(ctx context.Context) {
	err := gofs.NewFSClient().
		Register(gofsapi.DefaultModule, gofs.WithFunction(func() gofsapi.Function[Person, Person] {
			return t.testFunction
		})).
		Register("counter", gofs.WithFunction(func() gofsapi.Function[Counter, Counter] {
			return t.testCounter
		})).
		Register("test-source", gofs.WithSource(func() gofsapi.Source[testRecord] {
			return t.testSource
		})).
		Register("test-sink", gofs.WithSink(func() gofsapi.Sink[Counter] {
			return t.testSink
		})).
		Run(ctx)
	if err != nil {
		log.Error(err, "failed to run mock client")
	}
}

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
	require.NoError(t, os.Setenv("FS_SOCKET_PATH", testSocketPath))
	lis, err := net.Listen("unix", testSocketPath)
	require.NoError(t, err)
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	log, err := config.Build()
	require.NoError(t, err)
	adapter, err := NewExternalAdapter(context.Background(), "test", &Config{
		Listener: lis,
		Logger:   log,
	})
	require.NoError(t, err)
	go NewTestModules().Run(context.Background())
	time.Sleep(1 * time.Second)

	t.Run("Default Module", func(t *testing.T) {
		es := NewMockEventStorage()
		instance := NewMockInstance(context.Background(), &model.Function{
			Name:    "test",
			Package: "test",
			Module:  "default",
		}, es, nil)
		require.NoError(t, adapter.DeployFunction(context.Background(), instance))
		in, err := event.NewStructEvent("1", &Person{
			Name:  "test",
			Money: 1,
		})
		require.NoError(t, err)
		es.ReadCh <- in
		out := <-es.WriteCh
		require.Equal(t, "", out.ID())
		p := &Person{}
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
		in, err := event.NewStructEvent("1", &Counter{
			Count: 1,
		})
		require.NoError(t, err)
		es.ReadCh <- in
		out := <-es.WriteCh
		require.Equal(t, "", out.ID())
		c := &Counter{}
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
			r := &testRecord{}
			require.NoError(t, json.NewDecoder(out.Payload()).Decode(r))
			require.NoError(t, out.Commit(context.Background()))
			require.Equal(t, i, r.ID)
		}
	})
}
