package server

import (
	"context"
	"log/slog"
	"os"

	"github.com/functionstream/functionstream/lib"
)

var loadedConfig *lib.Config

const (
	PulsarQueueType = "pulsar"
)

func init() {
	loadedConfig = &lib.Config{
		ListenAddr: getEnvWithDefault("PORT", ":7300"),
		PulsarURL:  getEnvWithDefault("PULSAR_URL", "pulsar://localhost:6650"),
	}
	queueType := getEnvWithDefault("QUEUE_TYPE", PulsarQueueType)
	switch queueType {
	case PulsarQueueType:
		loadedConfig.QueueBuilder = func(ctx context.Context, c *lib.Config) (lib.EventQueueFactory, error) {
			return lib.NewPulsarEventQueueFactory(ctx, c)
		}
	}
}

func GetConfig() *lib.Config {
	return loadedConfig
}

func getEnvWithDefault(key string, defaultVal string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	slog.Info("Environment variable not found, using the default value:", key, defaultVal)
	return defaultVal
}
