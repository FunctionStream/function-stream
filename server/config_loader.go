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
	"github.com/functionstream/function-stream/common"
	"log/slog"
	"os"
	"sync"
)

var loadedConfig *common.Config
var initConfig = sync.Once{}

func LoadConfigFromEnv() *common.Config {
	initConfig.Do(func() {
		loadedConfig = &common.Config{
			ListenAddr: getEnvWithDefault("LISTEN_ADDR", common.DefaultAddr),
			PulsarURL:  getEnvWithDefault("PULSAR_URL", common.DefaultPulsarURL),
			TubeType:   getEnvWithDefault("TUBE_TYPE", common.DefaultTubeType),
		}
	})
	return loadedConfig
}

func LoadStandaloneConfigFromEnv() *common.Config {
	initConfig.Do(func() {
		loadedConfig = &common.Config{
			ListenAddr: getEnvWithDefault("LISTEN_ADDR", common.DefaultAddr),
			TubeType:   getEnvWithDefault("TUBE_TYPE", common.MemoryTubeType),
		}
	})
	return loadedConfig
}

func getEnvWithDefault(key string, defaultVal string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	slog.Info("Environment variable not found, using the default value:", key, defaultVal)
	return defaultVal
}
