package common

import (
	"fmt"
	"log"
	"os"
)

type Config struct {
	ListenAddr string
}

func GetConfig() *Config {
	return &Config{
		ListenAddr: getEnvWithDefault("PORT", ":8080"),
	}
}

func getEnvWithDefault(key string, defaultVal string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	log.Printf("Environment variable %s not found, using the default value: %s", key, defaultVal)
	return defaultVal
}

func getEnv(key string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	panic(fmt.Sprintf("Environment variable %s not found.", key))
}
