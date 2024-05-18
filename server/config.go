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
	"log/slog"
	"os"
	"strings"

	"github.com/functionstream/function-stream/common"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type TypeConfig struct {
	Name   string            `mapstructure:"name"`
	Ref    *string           `mapstructure:"ref"`
	Config *common.ConfigMap `mapstructure:"config"`
}

type StateStoreConfig struct {
	Type   *string           `mapstructure:"type"`
	Config *common.ConfigMap `mapstructure:"config"`
}

type Config struct {
	// ListenAddr is the address that the function stream REST service will listen on.
	ListenAddr string `mapstructure:"listen_addr"`

	TubeTypes []*TypeConfig `mapstructure:"tube_types"`

	tubeTypesMap map[string]*TypeConfig

	RuntimeTypes []*TypeConfig `mapstructure:"runtime_types"`

	runtimeTypesMap map[string]*TypeConfig

	// StateStore is the configuration for the state store that the function stream server will use.
	// Optional
	StateStore *StateStoreConfig `mapstructure:"state_store"`

	// FunctionStore is the path to the function store
	FunctionStore string `mapstructure:"function_store"`

	EnableTLS   bool   `mapstructure:"enable_tls"`
	TLSCertFile string `mapstructure:"tls_cert_file"`
	TLSKeyFile  string `mapstructure:"tls_key_file"`
}

func init() {
	viper.SetDefault("listen_addr", ":7300")
	viper.SetDefault("function_store", "./functions")
}

func preprocessFactoriesConfig(n string, m map[string]*TypeConfig) error {
	for name, factory := range m {
		if ref := factory.Ref; ref != nil && *ref != "" {
			referred, ok := m[strings.ToLower(*ref)]
			if !ok {
				return errors.Errorf("%s factory %s refers to non-existent factory %s", n, name, *ref)
			}
			factory.Config = common.MergeConfig(referred.Config, factory.Config)
		}
	}
	return nil
}

func (c *Config) preprocessConfig() error {
	if c.ListenAddr == "" {
		return errors.New("ListenAddr shouldn't be empty")
	}
	err := preprocessFactoriesConfig("Tube", c.tubeTypesMap)
	if err != nil {
		return err
	}
	return preprocessFactoriesConfig("Runtime", c.runtimeTypesMap)
}

func loadConfig() (*Config, error) {
	var c Config
	if err := viper.Unmarshal(&c); err != nil {
		return nil, err
	}
	c.tubeTypesMap = make(map[string]*TypeConfig)
	for _, t := range c.TubeTypes {
		if t.Config == nil {
			t.Config = &common.ConfigMap{}
		}
		c.tubeTypesMap[strings.ToLower(t.Name)] = t
	}
	c.runtimeTypesMap = make(map[string]*TypeConfig)
	for _, t := range c.RuntimeTypes {
		if t.Config == nil {
			t.Config = &common.ConfigMap{}
		}
		c.runtimeTypesMap[strings.ToLower(t.Name)] = t
	}
	if err := c.preprocessConfig(); err != nil {
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

// Deprecate
func LoadConfigFromEnv() (*Config, error) {
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "FS_") {
			parts := strings.SplitN(strings.TrimPrefix(env, envPrefix), "=", 2)
			key := parts[0]
			value := parts[1]

			slog.Info("Loading environment variable", "key", key, "value", value)
			viper.Set(strings.Replace(key, "__", ".", -1), value)
		}
	}

	return loadConfig()
}
