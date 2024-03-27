/*
Function Stream Service

Manage Function Stream Resources

API version: 1.0.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package restclient

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// checks if the ModelFunction type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &ModelFunction{}

// ModelFunction struct for ModelFunction
type ModelFunction struct {
	Config   *map[string]string `json:"config,omitempty"`
	Inputs   []string           `json:"inputs"`
	Name     *string            `json:"name,omitempty"`
	Output   string             `json:"output"`
	Replicas int32              `json:"replicas"`
	Runtime  ModelRuntimeConfig `json:"runtime"`
	Sink     *ModelTubeConfig   `json:"sink,omitempty"`
	Source   *ModelTubeConfig   `json:"source,omitempty"`
}

type _ModelFunction ModelFunction

// NewModelFunction instantiates a new ModelFunction object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewModelFunction(inputs []string, output string, replicas int32, runtime ModelRuntimeConfig) *ModelFunction {
	this := ModelFunction{}
	this.Inputs = inputs
	this.Output = output
	this.Replicas = replicas
	this.Runtime = runtime
	return &this
}

// NewModelFunctionWithDefaults instantiates a new ModelFunction object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewModelFunctionWithDefaults() *ModelFunction {
	this := ModelFunction{}
	return &this
}

// GetConfig returns the Config field value if set, zero value otherwise.
func (o *ModelFunction) GetConfig() map[string]string {
	if o == nil || IsNil(o.Config) {
		var ret map[string]string
		return ret
	}
	return *o.Config
}

// GetConfigOk returns a tuple with the Config field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetConfigOk() (*map[string]string, bool) {
	if o == nil || IsNil(o.Config) {
		return nil, false
	}
	return o.Config, true
}

// HasConfig returns a boolean if a field has been set.
func (o *ModelFunction) HasConfig() bool {
	if o != nil && !IsNil(o.Config) {
		return true
	}

	return false
}

// SetConfig gets a reference to the given map[string]string and assigns it to the Config field.
func (o *ModelFunction) SetConfig(v map[string]string) {
	o.Config = &v
}

// GetInputs returns the Inputs field value
func (o *ModelFunction) GetInputs() []string {
	if o == nil {
		var ret []string
		return ret
	}

	return o.Inputs
}

// GetInputsOk returns a tuple with the Inputs field value
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetInputsOk() ([]string, bool) {
	if o == nil {
		return nil, false
	}
	return o.Inputs, true
}

// SetInputs sets field value
func (o *ModelFunction) SetInputs(v []string) {
	o.Inputs = v
}

// GetName returns the Name field value if set, zero value otherwise.
func (o *ModelFunction) GetName() string {
	if o == nil || IsNil(o.Name) {
		var ret string
		return ret
	}
	return *o.Name
}

// GetNameOk returns a tuple with the Name field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetNameOk() (*string, bool) {
	if o == nil || IsNil(o.Name) {
		return nil, false
	}
	return o.Name, true
}

// HasName returns a boolean if a field has been set.
func (o *ModelFunction) HasName() bool {
	if o != nil && !IsNil(o.Name) {
		return true
	}

	return false
}

// SetName gets a reference to the given string and assigns it to the Name field.
func (o *ModelFunction) SetName(v string) {
	o.Name = &v
}

// GetOutput returns the Output field value
func (o *ModelFunction) GetOutput() string {
	if o == nil {
		var ret string
		return ret
	}

	return o.Output
}

// GetOutputOk returns a tuple with the Output field value
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetOutputOk() (*string, bool) {
	if o == nil {
		return nil, false
	}
	return &o.Output, true
}

// SetOutput sets field value
func (o *ModelFunction) SetOutput(v string) {
	o.Output = v
}

// GetReplicas returns the Replicas field value
func (o *ModelFunction) GetReplicas() int32 {
	if o == nil {
		var ret int32
		return ret
	}

	return o.Replicas
}

// GetReplicasOk returns a tuple with the Replicas field value
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetReplicasOk() (*int32, bool) {
	if o == nil {
		return nil, false
	}
	return &o.Replicas, true
}

// SetReplicas sets field value
func (o *ModelFunction) SetReplicas(v int32) {
	o.Replicas = v
}

// GetRuntime returns the Runtime field value
func (o *ModelFunction) GetRuntime() ModelRuntimeConfig {
	if o == nil {
		var ret ModelRuntimeConfig
		return ret
	}

	return o.Runtime
}

// GetRuntimeOk returns a tuple with the Runtime field value
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetRuntimeOk() (*ModelRuntimeConfig, bool) {
	if o == nil {
		return nil, false
	}
	return &o.Runtime, true
}

// SetRuntime sets field value
func (o *ModelFunction) SetRuntime(v ModelRuntimeConfig) {
	o.Runtime = v
}

// GetSink returns the Sink field value if set, zero value otherwise.
func (o *ModelFunction) GetSink() ModelTubeConfig {
	if o == nil || IsNil(o.Sink) {
		var ret ModelTubeConfig
		return ret
	}
	return *o.Sink
}

// GetSinkOk returns a tuple with the Sink field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetSinkOk() (*ModelTubeConfig, bool) {
	if o == nil || IsNil(o.Sink) {
		return nil, false
	}
	return o.Sink, true
}

// HasSink returns a boolean if a field has been set.
func (o *ModelFunction) HasSink() bool {
	if o != nil && !IsNil(o.Sink) {
		return true
	}

	return false
}

// SetSink gets a reference to the given ModelTubeConfig and assigns it to the Sink field.
func (o *ModelFunction) SetSink(v ModelTubeConfig) {
	o.Sink = &v
}

// GetSource returns the Source field value if set, zero value otherwise.
func (o *ModelFunction) GetSource() ModelTubeConfig {
	if o == nil || IsNil(o.Source) {
		var ret ModelTubeConfig
		return ret
	}
	return *o.Source
}

// GetSourceOk returns a tuple with the Source field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ModelFunction) GetSourceOk() (*ModelTubeConfig, bool) {
	if o == nil || IsNil(o.Source) {
		return nil, false
	}
	return o.Source, true
}

// HasSource returns a boolean if a field has been set.
func (o *ModelFunction) HasSource() bool {
	if o != nil && !IsNil(o.Source) {
		return true
	}

	return false
}

// SetSource gets a reference to the given ModelTubeConfig and assigns it to the Source field.
func (o *ModelFunction) SetSource(v ModelTubeConfig) {
	o.Source = &v
}

func (o ModelFunction) MarshalJSON() ([]byte, error) {
	toSerialize, err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o ModelFunction) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.Config) {
		toSerialize["config"] = o.Config
	}
	toSerialize["inputs"] = o.Inputs
	if !IsNil(o.Name) {
		toSerialize["name"] = o.Name
	}
	toSerialize["output"] = o.Output
	toSerialize["replicas"] = o.Replicas
	toSerialize["runtime"] = o.Runtime
	if !IsNil(o.Sink) {
		toSerialize["sink"] = o.Sink
	}
	if !IsNil(o.Source) {
		toSerialize["source"] = o.Source
	}
	return toSerialize, nil
}

func (o *ModelFunction) UnmarshalJSON(data []byte) (err error) {
	// This validates that all required properties are included in the JSON object
	// by unmarshalling the object into a generic map with string keys and checking
	// that every required field exists as a key in the generic map.
	requiredProperties := []string{
		"inputs",
		"output",
		"replicas",
		"runtime",
	}

	allProperties := make(map[string]interface{})

	err = json.Unmarshal(data, &allProperties)

	if err != nil {
		return err
	}

	for _, requiredProperty := range requiredProperties {
		if _, exists := allProperties[requiredProperty]; !exists {
			return fmt.Errorf("no value given for required property %v", requiredProperty)
		}
	}

	varModelFunction := _ModelFunction{}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&varModelFunction)

	if err != nil {
		return err
	}

	*o = ModelFunction(varModelFunction)

	return err
}

type NullableModelFunction struct {
	value *ModelFunction
	isSet bool
}

func (v NullableModelFunction) Get() *ModelFunction {
	return v.value
}

func (v *NullableModelFunction) Set(val *ModelFunction) {
	v.value = val
	v.isSet = true
}

func (v NullableModelFunction) IsSet() bool {
	return v.isSet
}

func (v *NullableModelFunction) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableModelFunction(val *ModelFunction) *NullableModelFunction {
	return &NullableModelFunction{value: val, isSet: true}
}

func (v NullableModelFunction) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableModelFunction) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
