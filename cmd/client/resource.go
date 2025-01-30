package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/fs/model"
	"gopkg.in/yaml.v3"
	"strings"
)

type Metadata struct {
	Name string `yaml:"name"`
}

type Resource struct {
	Kind     string         `yaml:"kind"`
	Metadata Metadata       `yaml:"metadata"`
	Spec     map[string]any `yaml:"spec"`
}

func decodeSpec(spec any, out any) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func DecodeResource(data []byte) ([]*Resource, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	resources := make([]*Resource, 0)

	for {
		resource := new(Resource)
		if err := decoder.Decode(&resource); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("Invalid yaml format: %w", err)
		}

		resource.Kind = strings.ToLower(resource.Kind)
		resource.Spec["name"] = resource.Metadata.Name

		// TODO: Validate the resources
		switch resource.Kind {
		case "package":
			pkg := &model.Package{}
			if err := decodeSpec(resource.Spec, &pkg); err != nil {
				return nil, fmt.Errorf("Invalid PackageMetadata: %w", err)
			}

			pkg.Name = resource.Metadata.Name
			resources = append(resources, resource)
		case "function":
			function := &model.Function{}
			if err := decodeSpec(resource.Spec, &function); err != nil {
				return nil, fmt.Errorf("Invalid Function: %w", err)
			}
			function.Name = resource.Metadata.Name
			resources = append(resources, resource)
		default:
			return nil, fmt.Errorf("Invalid resource kind: %s", resource.Kind)
		}
	}
	return resources, nil
}
