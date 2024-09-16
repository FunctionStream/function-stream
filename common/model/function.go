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

import (
	"strings"

	"github.com/functionstream/function-stream/common/config"

	"github.com/functionstream/function-stream/fs/contube"
	"github.com/pkg/errors"
)

type TubeConfig struct {
	Type   string            `json:"type"`
	Config contube.ConfigMap `json:"config,omitempty"`
}

type RuntimeConfig struct {
	Config config.ConfigMap `json:"config,omitempty"`
	Type   string           `json:"type"`
}

type Function struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Package   string            `json:"package"`
	Module    string            `json:"module"`
	Runtime   RuntimeConfig     `json:"runtime"`
	Sources   []TubeConfig      `json:"source"`
	Sink      TubeConfig        `json:"sink"`
	Config    map[string]string `json:"config,omitempty"`
	Replicas  int32             `json:"replicas"`
}

func (f *Function) Validate() error {
	if f.Name == "" {
		return errors.New("function name shouldn't be empty")
	}
	if strings.Contains(f.Name, "/") {
		return errors.New("name should not contain '/'")
	}
	if strings.Contains(f.Namespace, "/") {
		return errors.New("namespace should not contain '/'")
	}
	if len(f.Sources) == 0 {
		return errors.New("sources should be configured")
	}
	if f.Replicas <= 0 {
		return errors.New("replicas should be greater than 0")
	}
	return nil
}
