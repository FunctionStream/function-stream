package model

type Stream struct {
	Tubes     map[string]*TubeConfig `json:"tubes,omitempty" yaml:"tubes,omitempty"`
	Functions map[string]*Function   `json:"functions,omitempty" yaml:"functions,omitempty"`
}
