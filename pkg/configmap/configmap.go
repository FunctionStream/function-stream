package configmap

import (
	"encoding/json"

	"github.com/go-playground/validator/v10"
)

// ConfigMap is a custom type that represents a map where keys are strings and values are of any type.
// Since Viper is not case-sensitive, we use '-' to separate words in all field names in the config map.
// This convention helps in maintaining consistency across different configurations and makes them easier to read.
type ConfigMap map[string]interface{}

// MergeConfig merges multiple ConfigMap into one
func MergeConfig(configs ...ConfigMap) ConfigMap {
	result := ConfigMap{}
	for _, config := range configs {
		for k, v := range config {
			result[k] = v
		}
	}
	return result
}

func (c ConfigMap) ToConfigStruct(v any) error {
	jsonData, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(jsonData, v); err != nil {
		return err
	}
	validate := validator.New()
	return validate.Struct(v)
}

func ToConfigMap(v any) (ConfigMap, error) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var result ConfigMap
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return nil, err
	}
	return result, nil
}
