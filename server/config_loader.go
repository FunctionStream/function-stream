/*
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
	"context"
	"github.com/functionstream/functionstream/common"
	"log/slog"
	"os"
	"sync"

	"github.com/functionstream/functionstream/lib"
)

var loadedConfig *lib.Config
var initConfig = sync.Once{}

func LoadConfigFromEnv() *lib.Config {
	initConfig.Do(func() {
		loadedConfig = &lib.Config{
			ListenAddr: getEnvWithDefault("LISTEN_ADDR", common.DefaultAddr),
			PulsarURL:  getEnvWithDefault("PULSAR_URL", common.DefaultPulsarURL),
		}
		queueType := getEnvWithDefault("QUEUE_TYPE", common.DefaultQueueType)
		switch queueType {
		case common.PulsarQueueType:
			loadedConfig.QueueBuilder = func(ctx context.Context, c *lib.Config) (lib.EventQueueFactory, error) {
				return lib.NewPulsarEventQueueFactory(ctx, c)
			}
		}
	})
	return loadedConfig
}

func LoadStandaloneConfigFromEnv() *lib.Config {
	initConfig.Do(func() {
		loadedConfig = &lib.Config{
			ListenAddr: getEnvWithDefault("LISTEN_ADDR", common.DefaultAddr),
		}
		loadedConfig.QueueBuilder = func(ctx context.Context, c *lib.Config) (lib.EventQueueFactory, error) {
			return lib.NewMemoryQueueFactory(), nil
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
