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

package model

import "github.com/functionstream/function-stream/fs/contube"

type TubeConfig struct {
	Type   *string           `json:"type,omitempty"` // Default to `default`
	Config contube.ConfigMap `json:"config,omitempty"`
}

type ConfigMap map[string]interface{}

type RuntimeConfig struct {
	Config ConfigMap `json:"config,omitempty" yaml:"config,omitempty"`
	Type   *string   `json:"type,omitempty" yaml:"type,omitempty"`
}

type Function struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Runtime   *RuntimeConfig    `json:"runtime"`
	Sources   []*TubeConfig     `json:"source,omitempty"`
	Sink      *TubeConfig       `json:"sink,omitempty"`
	Config    map[string]string `json:"config,omitempty"`
	Replicas  int32             `json:"replicas"`
}
