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

package config

// ConfigMap is a custom type that represents a map where keys are strings and values are of any type.
// Since Viper is not case-sensitive, we use '-' to separate words in all field names in the config map.
// This convention helps in maintaining consistency across different configurations and makes them easier to read.
//
// For example:
//   - `socket-path` refers to the path of the socket.
//   - `pulsar-url` refers to the URL of the Pulsar service.
type ConfigMap map[string]interface{}

// MergeConfig merges multiple ConfigMap into one
func MergeConfig(configs ...ConfigMap) ConfigMap {
	result := ConfigMap{}
	for _, config := range configs {
		for k, v := range config {
			result[k] = v
		}
	}
	return result
}
