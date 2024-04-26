package utils

import (
	"fmt"
	"github.com/functionstream/function-stream/admin/client"
)

func MakeQueueSourceTubeConfig(subName string, topics ...string) []adminclient.ModelTubeConfig {
	return []adminclient.ModelTubeConfig{
		{
			Config: map[string]interface{}{
				"topicList": append([]string{}, topics...),
				"subName":   subName,
			},
		},
	}
}

func MakeQueueSinkTubeConfig(topic string) *adminclient.ModelTubeConfig {
	return &adminclient.ModelTubeConfig{
		Config: map[string]interface{}{
			"topic": topic,
		},
	}
}

func GetInputTopics(f *adminclient.ModelFunction) ([]string, error) {
	if len(f.Source) < 1 {
		return nil, fmt.Errorf("function %s has no sources", f.Name)
	}
	config := f.Source[0].Config
	if len(config) < 1 {
		return nil, fmt.Errorf("source config for function %s is empty", f.Name)
	}
	if topicList, ok := config["topicList"].([]string); ok {
		return topicList, nil
	}
	return nil, fmt.Errorf("source config for function %s has no topicList", f.Name)
}

func GetOutputTopic(f *adminclient.ModelFunction) (string, error) {
	if f.Sink == nil {
		return "", fmt.Errorf("function %s has no sink", f.Name)
	}
	config := f.Sink.Config
	if len(config) < 1 {
		return "", fmt.Errorf("sink config for function %s is empty", f.Name)
	}
	if topic, ok := config["topic"].(string); ok {
		return topic, nil
	}
	return "", fmt.Errorf("sink config for function %s has no topic", f.Name)
}
