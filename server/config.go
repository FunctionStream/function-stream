package server

import (
	"fmt"
	"github.com/functionstream/function-stream/common/config"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type ComponentConfig struct {
	Type   string           `mapstructure:"type"`
	Config config.ConfigMap `mapstructure:"config"`
}

type Config struct {
	// ListenAddress is the address that the function stream REST service will listen on.
	ListenAddress string `mapstructure:"listen-address" validate:"required"`
	EnableTLS     bool   `mapstructure:"enable-tls"`
	TLSCertFile   string `mapstructure:"tls-cert-file"`
	TLSKeyFile    string `mapstructure:"tls-key-file"`

	PackageLoader ComponentConfig   `mapstructure:"package-loader"`
	EventStorage  ComponentConfig   `mapstructure:"event-storage"`
	StateStore    ComponentConfig   `mapstructure:"state-store"`
	Runtimes      []ComponentConfig `mapstructure:"runtimes"`
}

func init() {
	viper.SetDefault("listen-address", ":7300")
}

func (c *Config) PreprocessConfig() error {
	if c.ListenAddress == "" {
		return fmt.Errorf("listen-address shouldn't be empty")
	}
	validate := validator.New()
	if err := validate.Struct(c); err != nil {
		return err
	}
	return nil
}

func LoadConfigFromFile(filePath string) (*Config, error) {
	viper.SetConfigFile(filePath)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}
	return loadConfig()
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
