/*
Function Stream API

No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

API version: 0.1.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package restclient

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// checks if the Function type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &Function{}

// Function struct for Function
type Function struct {
	Name     *string            `json:"name,omitempty"`
	Runtime  *FunctionRuntime   `json:"runtime,omitempty"`
	Source   *FunctionSource    `json:"source,omitempty"`
	Sink     *FunctionSource    `json:"sink,omitempty"`
	Inputs   []string           `json:"inputs"`
	Output   string             `json:"output"`
	Config   *map[string]string `json:"config,omitempty"`
	Replicas int32              `json:"replicas"`
}

type _Function Function

// NewFunction instantiates a new Function object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewFunction(inputs []string, output string, replicas int32) *Function {
	this := Function{}
	this.Inputs = inputs
	this.Output = output
	this.Replicas = replicas
	return &this
}

// NewFunctionWithDefaults instantiates a new Function object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewFunctionWithDefaults() *Function {
	this := Function{}
	return &this
}

// GetName returns the Name field value if set, zero value otherwise.
func (o *Function) GetName() string {
	if o == nil || IsNil(o.Name) {
		var ret string
		return ret
	}
	return *o.Name
}

// GetNameOk returns a tuple with the Name field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *Function) GetNameOk() (*string, bool) {
	if o == nil || IsNil(o.Name) {
		return nil, false
	}
	return o.Name, true
}

// HasName returns a boolean if a field has been set.
func (o *Function) HasName() bool {
	if o != nil && !IsNil(o.Name) {
		return true
	}

	return false
}

// SetName gets a reference to the given string and assigns it to the Name field.
func (o *Function) SetName(v string) {
	o.Name = &v
}

// GetRuntime returns the Runtime field value if set, zero value otherwise.
func (o *Function) GetRuntime() FunctionRuntime {
	if o == nil || IsNil(o.Runtime) {
		var ret FunctionRuntime
		return ret
	}
	return *o.Runtime
}

// GetRuntimeOk returns a tuple with the Runtime field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *Function) GetRuntimeOk() (*FunctionRuntime, bool) {
	if o == nil || IsNil(o.Runtime) {
		return nil, false
	}
	return o.Runtime, true
}

// HasRuntime returns a boolean if a field has been set.
func (o *Function) HasRuntime() bool {
	if o != nil && !IsNil(o.Runtime) {
		return true
	}

	return false
}

// SetRuntime gets a reference to the given FunctionRuntime and assigns it to the Runtime field.
func (o *Function) SetRuntime(v FunctionRuntime) {
	o.Runtime = &v
}

// GetSource returns the Source field value if set, zero value otherwise.
func (o *Function) GetSource() FunctionSource {
	if o == nil || IsNil(o.Source) {
		var ret FunctionSource
		return ret
	}
	return *o.Source
}

// GetSourceOk returns a tuple with the Source field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *Function) GetSourceOk() (*FunctionSource, bool) {
	if o == nil || IsNil(o.Source) {
		return nil, false
	}
	return o.Source, true
}

// HasSource returns a boolean if a field has been set.
func (o *Function) HasSource() bool {
	if o != nil && !IsNil(o.Source) {
		return true
	}

	return false
}

// SetSource gets a reference to the given FunctionSource and assigns it to the Source field.
func (o *Function) SetSource(v FunctionSource) {
	o.Source = &v
}

// GetSink returns the Sink field value if set, zero value otherwise.
func (o *Function) GetSink() FunctionSource {
	if o == nil || IsNil(o.Sink) {
		var ret FunctionSource
		return ret
	}
	return *o.Sink
}

// GetSinkOk returns a tuple with the Sink field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *Function) GetSinkOk() (*FunctionSource, bool) {
	if o == nil || IsNil(o.Sink) {
		return nil, false
	}
	return o.Sink, true
}

// HasSink returns a boolean if a field has been set.
func (o *Function) HasSink() bool {
	if o != nil && !IsNil(o.Sink) {
		return true
	}

	return false
}

// SetSink gets a reference to the given FunctionSource and assigns it to the Sink field.
func (o *Function) SetSink(v FunctionSource) {
	o.Sink = &v
}

// GetInputs returns the Inputs field value
func (o *Function) GetInputs() []string {
	if o == nil {
		var ret []string
		return ret
	}

	return o.Inputs
}

// GetInputsOk returns a tuple with the Inputs field value
// and a boolean to check if the value has been set.
func (o *Function) GetInputsOk() ([]string, bool) {
	if o == nil {
		return nil, false
	}
	return o.Inputs, true
}

// SetInputs sets field value
func (o *Function) SetInputs(v []string) {
	o.Inputs = v
}

// GetOutput returns the Output field value
func (o *Function) GetOutput() string {
	if o == nil {
		var ret string
		return ret
	}

	return o.Output
}

// GetOutputOk returns a tuple with the Output field value
// and a boolean to check if the value has been set.
func (o *Function) GetOutputOk() (*string, bool) {
	if o == nil {
		return nil, false
	}
	return &o.Output, true
}

// SetOutput sets field value
func (o *Function) SetOutput(v string) {
	o.Output = v
}

// GetConfig returns the Config field value if set, zero value otherwise.
func (o *Function) GetConfig() map[string]string {
	if o == nil || IsNil(o.Config) {
		var ret map[string]string
		return ret
	}
	return *o.Config
}

// GetConfigOk returns a tuple with the Config field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *Function) GetConfigOk() (*map[string]string, bool) {
	if o == nil || IsNil(o.Config) {
		return nil, false
	}
	return o.Config, true
}

// HasConfig returns a boolean if a field has been set.
func (o *Function) HasConfig() bool {
	if o != nil && !IsNil(o.Config) {
		return true
	}

	return false
}

// SetConfig gets a reference to the given map[string]string and assigns it to the Config field.
func (o *Function) SetConfig(v map[string]string) {
	o.Config = &v
}

// GetReplicas returns the Replicas field value
func (o *Function) GetReplicas() int32 {
	if o == nil {
		var ret int32
		return ret
	}

	return o.Replicas
}

// GetReplicasOk returns a tuple with the Replicas field value
// and a boolean to check if the value has been set.
func (o *Function) GetReplicasOk() (*int32, bool) {
	if o == nil {
		return nil, false
	}
	return &o.Replicas, true
}

// SetReplicas sets field value
func (o *Function) SetReplicas(v int32) {
	o.Replicas = v
}

func (o Function) MarshalJSON() ([]byte, error) {
	toSerialize, err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o Function) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.Name) {
		toSerialize["name"] = o.Name
	}
	if !IsNil(o.Runtime) {
		toSerialize["runtime"] = o.Runtime
	}
	if !IsNil(o.Source) {
		toSerialize["source"] = o.Source
	}
	if !IsNil(o.Sink) {
		toSerialize["sink"] = o.Sink
	}
	toSerialize["inputs"] = o.Inputs
	toSerialize["output"] = o.Output
	if !IsNil(o.Config) {
		toSerialize["config"] = o.Config
	}
	toSerialize["replicas"] = o.Replicas
	return toSerialize, nil
}

func (o *Function) UnmarshalJSON(data []byte) (err error) {
	// This validates that all required properties are included in the JSON object
	// by unmarshalling the object into a generic map with string keys and checking
	// that every required field exists as a key in the generic map.
	requiredProperties := []string{
		"inputs",
		"output",
		"replicas",
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

	varFunction := _Function{}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&varFunction)

	if err != nil {
		return err
	}

	*o = Function(varFunction)

	return err
}

type NullableFunction struct {
	value *Function
	isSet bool
}

func (v NullableFunction) Get() *Function {
	return v.value
}

func (v *NullableFunction) Set(val *Function) {
	v.value = val
	v.isSet = true
}

func (v NullableFunction) IsSet() bool {
	return v.isSet
}

func (v *NullableFunction) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableFunction(val *Function) *NullableFunction {
	return &NullableFunction{value: val, isSet: true}
}

func (v NullableFunction) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableFunction) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
