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

package utils

import (
	"fmt"

	adminclient "github.com/functionstream/function-stream/admin/client"
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
