package memory

import (
	"bytes"
	"context"
	"fmt"
	"github.com/functionstream/function-stream/fs/event"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestMemoryEventStorage(t *testing.T) {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	log, err := config.Build()

	ctx, cancel := context.WithCancel(context.Background())
	es := NewMemEs(ctx, log)

	topics := []model.TopicConfig{
		{
			Name: "topic1",
		},
		{
			Name: "topic2",
		},
		{
			Name: "topic3",
		},
	}
	source, err := es.Read(ctx, topics)
	require.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}

	numMsgsPerTopic := 10
	totalMsgs := numMsgsPerTopic * len(topics)

	go func() {
		for _, v := range topics {
			for i := 0; i < numMsgsPerTopic; i++ {
				require.NoError(t, es.Write(ctx, event.NewRawEvent(strconv.Itoa(i), bytes.NewReader([]byte(fmt.Sprintf("%s-%d", v.Name, i)))), v))
			}
		}
	}()
	events := make(map[string]int)

	recvMsgs := 0

	for e := range source {
		data, err := io.ReadAll(e.Payload())
		require.NoError(t, err)
		str := string(data)
		parts := strings.Split(str, "-")
		l := events[parts[0]]
		i, err := strconv.Atoi(parts[1])
		require.NoError(t, err)
		require.Equal(t, l, i)
		events[parts[0]]++
		recvMsgs++
		if recvMsgs == totalMsgs {
			break
		}
	}

	require.ElementsMatch(t, maps.Keys(events), []string{"topic1", "topic2", "topic3"})
	cancel()
}
