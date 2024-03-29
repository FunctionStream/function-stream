/*
Function Stream API

No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

API version: 0.1.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package restclient

import (
	"encoding/json"
)

// checks if the FunctionRuntime type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &FunctionRuntime{}

// FunctionRuntime struct for FunctionRuntime
type FunctionRuntime struct {
	Config map[string]interface{} `json:"config,omitempty"`
	Type   NullableString         `json:"type,omitempty"`
}

// NewFunctionRuntime instantiates a new FunctionRuntime object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewFunctionRuntime() *FunctionRuntime {
	this := FunctionRuntime{}
	return &this
}

// NewFunctionRuntimeWithDefaults instantiates a new FunctionRuntime object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewFunctionRuntimeWithDefaults() *FunctionRuntime {
	this := FunctionRuntime{}
	return &this
}

// GetConfig returns the Config field value if set, zero value otherwise.
func (o *FunctionRuntime) GetConfig() map[string]interface{} {
	if o == nil || IsNil(o.Config) {
		var ret map[string]interface{}
		return ret
	}
	return o.Config
}

// GetConfigOk returns a tuple with the Config field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *FunctionRuntime) GetConfigOk() (map[string]interface{}, bool) {
	if o == nil || IsNil(o.Config) {
		return map[string]interface{}{}, false
	}
	return o.Config, true
}

// HasConfig returns a boolean if a field has been set.
func (o *FunctionRuntime) HasConfig() bool {
	if o != nil && !IsNil(o.Config) {
		return true
	}

	return false
}

// SetConfig gets a reference to the given map[string]interface{} and assigns it to the Config field.
func (o *FunctionRuntime) SetConfig(v map[string]interface{}) {
	o.Config = v
}

// GetType returns the Type field value if set, zero value otherwise (both if not set or set to explicit null).
func (o *FunctionRuntime) GetType() string {
	if o == nil || IsNil(o.Type.Get()) {
		var ret string
		return ret
	}
	return *o.Type.Get()
}

// GetTypeOk returns a tuple with the Type field value if set, nil otherwise
// and a boolean to check if the value has been set.
// NOTE: If the value is an explicit nil, `nil, true` will be returned
func (o *FunctionRuntime) GetTypeOk() (*string, bool) {
	if o == nil {
		return nil, false
	}
	return o.Type.Get(), o.Type.IsSet()
}

// HasType returns a boolean if a field has been set.
func (o *FunctionRuntime) HasType() bool {
	if o != nil && o.Type.IsSet() {
		return true
	}

	return false
}

// SetType gets a reference to the given NullableString and assigns it to the Type field.
func (o *FunctionRuntime) SetType(v string) {
	o.Type.Set(&v)
}

// SetTypeNil sets the value for Type to be an explicit nil
func (o *FunctionRuntime) SetTypeNil() {
	o.Type.Set(nil)
}

// UnsetType ensures that no value is present for Type, not even an explicit nil
func (o *FunctionRuntime) UnsetType() {
	o.Type.Unset()
}

func (o FunctionRuntime) MarshalJSON() ([]byte, error) {
	toSerialize, err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o FunctionRuntime) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.Config) {
		toSerialize["config"] = o.Config
	}
	if o.Type.IsSet() {
		toSerialize["type"] = o.Type.Get()
	}
	return toSerialize, nil
}

type NullableFunctionRuntime struct {
	value *FunctionRuntime
	isSet bool
}

func (v NullableFunctionRuntime) Get() *FunctionRuntime {
	return v.value
}

func (v *NullableFunctionRuntime) Set(val *FunctionRuntime) {
	v.value = val
	v.isSet = true
}

func (v NullableFunctionRuntime) IsSet() bool {
	return v.isSet
}

func (v *NullableFunctionRuntime) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableFunctionRuntime(val *FunctionRuntime) *NullableFunctionRuntime {
	return &NullableFunctionRuntime{value: val, isSet: true}
}

func (v NullableFunctionRuntime) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableFunctionRuntime) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
