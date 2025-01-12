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

package fsold

import (
	"context"
	"testing"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/stretchr/testify/assert"
)

// Mock implementations of the interfaces and structs
type MockPackage struct {
	runtimeConfigs []model.RuntimeConfig
}

func (m *MockPackage) GetSupportedRuntimeConfig() []model.RuntimeConfig {
	return m.runtimeConfigs
}

func TestGenerateRuntimeConfig_EmptySupportedRuntimeConfig(t *testing.T) {
	ctx := context.Background()
	p := &MockPackage{runtimeConfigs: []model.RuntimeConfig{}}
	f := &model.Function{}

	_, err := generateRuntimeConfig(ctx, p, f)
	assert.NotNil(t, err)
	assert.Equal(t, common.ErrorPackageNoSupportedRuntime, err)
}

func TestGenerateRuntimeConfig_EmptyFunctionRuntimeType(t *testing.T) {
	ctx := context.Background()
	p := &MockPackage{
		runtimeConfigs: []model.RuntimeConfig{
			{Type: "runtime1", Config: map[string]interface{}{"key1": "value1"}},
		},
	}
	f := &model.Function{
		Runtime: model.RuntimeConfig{},
	}

	rc, err := generateRuntimeConfig(ctx, p, f)
	assert.Nil(t, err)
	assert.Equal(t, "runtime1", rc.Type)
	assert.Equal(t, "value1", rc.Config["key1"])
}

func TestGenerateRuntimeConfig_UnsupportedFunctionRuntimeType(t *testing.T) {
	ctx := context.Background()
	p := &MockPackage{
		runtimeConfigs: []model.RuntimeConfig{
			{Type: "runtime1", Config: map[string]interface{}{"key1": "value1"}},
		},
	}
	f := &model.Function{
		Runtime: model.RuntimeConfig{Type: "unsupported_runtime"},
	}

	_, err := generateRuntimeConfig(ctx, p, f)
	assert.NotNil(t, err)
	assert.Equal(t, "runtime type 'unsupported_runtime' is not supported by package ''", err.Error())
}

func TestGenerateRuntimeConfig_SupportedFunctionRuntimeType(t *testing.T) {
	ctx := context.Background()
	p := &MockPackage{
		runtimeConfigs: []model.RuntimeConfig{
			{Type: "runtime1", Config: map[string]interface{}{"key1": "value1"}},
			{Type: "runtime2", Config: map[string]interface{}{"key2": "value2"}},
		},
	}
	f := &model.Function{
		Runtime: model.RuntimeConfig{Type: "runtime2", Config: map[string]interface{}{"key3": "value3"}},
	}

	rc, err := generateRuntimeConfig(ctx, p, f)
	assert.Nil(t, err)
	assert.Equal(t, "runtime2", rc.Type)
	assert.Equal(t, "value2", rc.Config["key2"])
	assert.Equal(t, "value3", rc.Config["key3"])
}
