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
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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

func TestLoadConfigFromEnv(t *testing.T) {
	assert.Nil(t, os.Setenv("FS_LISTEN_ADDR", ":17300"))
	assert.Nil(t, os.Setenv("FS_TUBE_CONFIG__MY_TUBE__KEY", "value"))
	assert.Nil(t, os.Setenv("FS_RUNTIME_CONFIG__CUSTOM_RUNTIME__NAME", "test"))

	viper.AutomaticEnv()

	c, err := LoadConfigFromEnv()
	require.Nil(t, err)
	assertConfig(t, c)
}

func assertConfig(t *testing.T, c *Config) {
	assert.Equal(t, ":17300", c.ListenAddr)
	require.Contains(t, c.TubeConfig, "my-tube")
	assert.Equal(t, "value", c.TubeConfig["my-tube"]["key"])

	require.Contains(t, c.RuntimeConfig, "custom-runtime")
	assert.Equal(t, "test", c.RuntimeConfig["custom-runtime"]["name"])
}
