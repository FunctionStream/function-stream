package model

type ConfigMap map[string]string
type GenericConfigMap map[string]interface{}

type TopicConfig struct {
	Name   string    `json:"name"`
	Config ConfigMap `json:"config"`
}

type Function struct {
	Name    string        `json:"name" validate:"required,alphanumdash"`
	Package string        `json:"package" validate:"required,alphanumdash"`
	Module  string        `json:"module" validate:"required,alphanumdash"`
	Sources []TopicConfig `json:"sources"`
	Sink    TopicConfig   `json:"sink"`
	Config  ConfigMap     `json:"config"`
}

type ModuleConfigItem struct {
	Type     string `json:"type"`
	Required string `json:"required"`
}

type ModuleConfig map[string]ModuleConfigItem

type Package struct {
	Name    string                  `json:"name"`
	Type    string                  `json:"type"`
	Modules map[string]ModuleConfig `json:"modules"`
}
