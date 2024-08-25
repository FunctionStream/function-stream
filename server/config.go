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
	"fmt"
	"os"
	"strings"

	"github.com/go-playground/validator/v10"

	"github.com/functionstream/function-stream/common"
	"github.com/spf13/viper"
)

type FactoryConfig struct {
	// Deprecate
	Ref    *string           `mapstructure:"ref"`
	Type   *string           `mapstructure:"type"`
	Config *common.ConfigMap `mapstructure:"config"`
}

type StateStoreConfig struct {
	Type   *string           `mapstructure:"type"`
	Config *common.ConfigMap `mapstructure:"config"`
}

type QueueConfig struct {
	Type   string           `mapstructure:"type"`
	Config common.ConfigMap `mapstructure:"config"`
}

type Config struct {
	// ListenAddr is the address that the function stream REST service will listen on.
	ListenAddr string `mapstructure:"listen-addr"`

	Queue QueueConfig `mapstructure:"queue"`

	TubeConfig map[string]common.ConfigMap `mapstructure:"tube-config"`

	RuntimeConfig map[string]common.ConfigMap `mapstructure:"runtime-config"`

	// StateStore is the configuration for the state store that the function stream server will use.
	// Optional
	StateStore *StateStoreConfig `mapstructure:"state-store"`

	// FunctionStore is the path to the function store
	FunctionStore string `mapstructure:"function-store"`

	EnableTLS   bool   `mapstructure:"enable-tls"`
	TLSCertFile string `mapstructure:"tls-cert-file"`
	TLSKeyFile  string `mapstructure:"tls-key-file"`
}

func init() {
	viper.SetDefault("listen-addr", ":7300")
	viper.SetDefault("function-store", "./functions")
}

func (c *Config) PreprocessConfig() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("ListenAddr shouldn't be empty")
	}
	validate := validator.New()
	if err := validate.Struct(c); err != nil {
		return err
	}
	return nil
}

func loadConfig() (*Config, error) {
	var c Config
	if err := viper.Unmarshal(&c); err != nil {
		return nil, err
	}
	if err := c.PreprocessConfig(); err != nil {
		return nil, err
	}
	return &c, nil
}

const envPrefix = "FS_"

func LoadConfigFromFile(filePath string) (*Config, error) {
	viper.SetConfigFile(filePath)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}
	return loadConfig()
}

func LoadConfigFromEnv() (*Config, error) {
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, envPrefix) {
			parts := strings.SplitN(strings.TrimPrefix(env, envPrefix), "=", 2)
			key := parts[0]
			value := parts[1]

			key = strings.Replace(key, "__", ".", -1)
			key = strings.Replace(key, "_", "-", -1)
			viper.Set(key, value)
		}
	}

	return loadConfig()
}
