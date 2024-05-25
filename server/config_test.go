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

//func TestLoadConfigFromYaml(t *testing.T) {
//	c, err := LoadConfigFromFile("../tests/test_config.yaml")
//	require.Nil(t, err)
//	assertConfig(t, c)
//}
//
//func TestLoadConfigFromJson(t *testing.T) {
//	c, err := LoadConfigFromFile("../tests/test_config.json")
//	require.Nil(t, err)
//	assertConfig(t, c)
//}
//
//func TestLoadConfigFromEnv(t *testing.T) {
//	assert.Nil(t, os.Setenv("FS_LISTEN_ADDR", ":17300"))
//	assert.Nil(t, os.Setenv("FS_TUBE_FACTORY__MY_PULSAR__TYPE", "pulsar"))
//	assert.Nil(t, os.Setenv("FS_TUBE_FACTORY__MY_PULSAR__CONFIG__PULSAR_URL", "pulsar://localhost:6651"))
//	assert.Nil(t, os.Setenv("FS_TUBE_FACTORY__MY_MEMORY__TYPE", "memory"))
//	assert.Nil(t, os.Setenv("FS_TUBE_FACTORY__DEFAULT__REF", "my_pulsar"))
//
//	viper.AutomaticEnv()
//
//	c, err := LoadConfigFromEnv()
//	require.Nil(t, err)
//	assertConfig(t, c)
//}
//
//func assertConfig(t *testing.T, c *Config) {
//	assert.Equal(t, ":17300", c.ListenAddr)
//	require.Contains(t, c.TubeConfig, "my_pulsar")
//	assert.Equal(t, "pulsar", *c.TubeConfig["my_pulsar"].Type)
//
//	if config := c.TubeConfig["my_pulsar"].Config; config != nil {
//		assert.Equal(t, "pulsar://localhost:6651", (*config)["pulsar_url"])
//	} else {
//		t.Fatal("pulsar config is nil")
//	}
//
//	require.Contains(t, c.TubeConfig, "my_memory")
//	assert.Equal(t, "memory", *c.TubeConfig["my_memory"].Type)
//
//	require.Contains(t, c.TubeConfig, "default")
//	assert.Equal(t, "my_pulsar", *c.TubeConfig["default"].Ref)
//	if config := c.TubeConfig["default"].Config; config != nil {
//		assert.Equal(t, "pulsar://localhost:6651", (*config)["pulsar_url"])
//	} else {
//		t.Fatal("pulsar config is nil")
//	}
//}
