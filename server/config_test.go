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

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromYaml(t *testing.T) {
	c, err := LoadConfigFromFile("../tests/test_config.yaml")
	require.Nil(t, err)
	assertConfig(t, c)
}

func TestLoadConfigFromJson(t *testing.T) {
	c, err := LoadConfigFromFile("../tests/test_config.json")
	require.Nil(t, err)
	assertConfig(t, c)
}

func assertConfig(t *testing.T, c *Config) {
	assert.Equal(t, ":17300", c.ListenAddr)
	require.Contains(t, c.tubeTypesMap, "my_pulsar")

	if config := c.tubeTypesMap["my_pulsar"].Config; config != nil {
		assert.Equal(t, "pulsar://localhost:6651", (*config)["pulsar_url"])
	} else {
		t.Fatal("pulsar config is nil")
	}

	require.Contains(t, c.tubeTypesMap, "my_memory")

	require.Contains(t, c.tubeTypesMap, "default")
	assert.Equal(t, "my_pulsar", *c.tubeTypesMap["default"].Ref)
	if config := c.tubeTypesMap["default"].Config; config != nil {
		assert.Equal(t, "pulsar://localhost:6651", (*config)["pulsar_url"])
	} else {
		t.Fatal("pulsar config is nil")
	}
}
