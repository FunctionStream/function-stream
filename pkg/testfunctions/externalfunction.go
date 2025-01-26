package testfunctions

import (
	"context"
	"github.com/functionstream/function-stream/clients/gofs"
	gofsapi "github.com/functionstream/function-stream/clients/gofs/api"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type Person struct {
	Name     string `json:"name"`
	Money    int    `json:"money"`
	Expected int    `json:"expected"`
}

type Counter struct {
	Count int `json:"count"`
}

type TestRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

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

func (f *TestSource) Handle(_ gofsapi.FunctionContext, emit func(context.Context, gofsapi.Event[TestRecord]) error) error {
	for i := 0; i < 10; i++ {
		err := emit(context.Background(), gofsapi.NewEvent(&TestRecord{
			ID:   i,
			Name: "test",
		}))
		if err != nil {
			return err
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
		Register("test-source", gofs.WithSource(func() gofsapi.Source[TestRecord] {
			return t.testSource
		})).
		Register("test-sink", gofs.WithSink(func() gofsapi.Sink[Counter] {
			return t.testSink
		})).
		Run(ctx)
	if err != nil {
		panic(err)
	}
}

func SetupFSTarget(t *testing.T, target string) {
	require.NoError(t, os.Setenv("FS_TARGET", target))
}
