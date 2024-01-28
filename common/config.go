package common

import (
	"log/slog"
	"os"
)

type Config struct {
	ListenAddr string
	PulsarURL  string
}

func GetConfig() *Config {
	return &Config{
		ListenAddr: getEnvWithDefault("PORT", ":7000"),
		PulsarURL:  getEnvWithDefault("PULSAR_URL", "pulsar://localhost:6650"),
	}
}

func getEnvWithDefault(key string, defaultVal string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	slog.Info("Environment variable not found, using the default value:", key, defaultVal)
	return defaultVal
}
