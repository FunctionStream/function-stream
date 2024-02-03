package common

import (
	"log/slog"
	"os"
)

type Config struct {
	ListenAddr string
	PulsarURL  string
}

var loadedConfig *Config

func GetConfig() *Config {
	if loadedConfig == nil {
		loadedConfig = &Config{
			ListenAddr: getEnvWithDefault("PORT", ":7300"),
			PulsarURL:  getEnvWithDefault("PULSAR_URL", "pulsar://localhost:6650"),
		}
	}
	return loadedConfig
}

func getEnvWithDefault(key string, defaultVal string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	slog.Info("Environment variable not found, using the default value:", key, defaultVal)
	return defaultVal
}
