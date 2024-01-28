package model

type Function struct {
	Name    string            `json:"name"`
	Archive string            `json:"archive"`
	Inputs  []string          `json:"inputs"`
	Output  string            `json:"output"`
	Config  map[string]string `json:"config"`
}
