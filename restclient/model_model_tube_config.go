/*
Function Stream Service

Manage Function Stream Resources

API version: 1.0.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package restclient

import (
	"encoding/json"
)

// checks if the ModelTubeConfig type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &ModelTubeConfig{}

// ModelTubeConfig struct for ModelTubeConfig
type ModelTubeConfig struct {
	Config map[string]interface{} `json:"config,omitempty"`
	Type   *string                `json:"type,omitempty"`
}

// NewModelTubeConfig instantiates a new ModelTubeConfig object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewModelTubeConfig() *ModelTubeConfig {
	this := ModelTubeConfig{}
	return &this
}

// NewModelTubeConfigWithDefaults instantiates a new ModelTubeConfig object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewModelTubeConfigWithDefaults() *ModelTubeConfig {
	this := ModelTubeConfig{}
	return &this
}

// GetConfig returns the Config field value if set, zero value otherwise.
func (o *ModelTubeConfig) GetConfig() map[string]interface{} {
	if o == nil || IsNil(o.Config) {
		var ret map[string]interface{}
		return ret
	}
	return o.Config
}

// GetConfigOk returns a tuple with the Config field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ModelTubeConfig) GetConfigOk() (map[string]interface{}, bool) {
	if o == nil || IsNil(o.Config) {
		return map[string]interface{}{}, false
	}
	return o.Config, true
}

// HasConfig returns a boolean if a field has been set.
func (o *ModelTubeConfig) HasConfig() bool {
	if o != nil && !IsNil(o.Config) {
		return true
	}

	return false
}

// SetConfig gets a reference to the given map[string]interface{} and assigns it to the Config field.
func (o *ModelTubeConfig) SetConfig(v map[string]interface{}) {
	o.Config = v
}

// GetType returns the Type field value if set, zero value otherwise.
func (o *ModelTubeConfig) GetType() string {
	if o == nil || IsNil(o.Type) {
		var ret string
		return ret
	}
	return *o.Type
}

// GetTypeOk returns a tuple with the Type field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ModelTubeConfig) GetTypeOk() (*string, bool) {
	if o == nil || IsNil(o.Type) {
		return nil, false
	}
	return o.Type, true
}

// HasType returns a boolean if a field has been set.
func (o *ModelTubeConfig) HasType() bool {
	if o != nil && !IsNil(o.Type) {
		return true
	}

	return false
}

// SetType gets a reference to the given string and assigns it to the Type field.
func (o *ModelTubeConfig) SetType(v string) {
	o.Type = &v
}

func (o ModelTubeConfig) MarshalJSON() ([]byte, error) {
	toSerialize, err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o ModelTubeConfig) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.Config) {
		toSerialize["config"] = o.Config
	}
	if !IsNil(o.Type) {
		toSerialize["type"] = o.Type
	}
	return toSerialize, nil
}

type NullableModelTubeConfig struct {
	value *ModelTubeConfig
	isSet bool
}

func (v NullableModelTubeConfig) Get() *ModelTubeConfig {
	return v.value
}

func (v *NullableModelTubeConfig) Set(val *ModelTubeConfig) {
	v.value = val
	v.isSet = true
}

func (v NullableModelTubeConfig) IsSet() bool {
	return v.isSet
}

func (v *NullableModelTubeConfig) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableModelTubeConfig(val *ModelTubeConfig) *NullableModelTubeConfig {
	return &NullableModelTubeConfig{value: val, isSet: true}
}

func (v NullableModelTubeConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableModelTubeConfig) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
