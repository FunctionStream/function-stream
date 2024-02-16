# FunctionRuntime

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Config** | Pointer to **map[string]interface{}** |  | [optional] 
**Type** | Pointer to **NullableString** |  | [optional] 

## Methods

### NewFunctionRuntime

`func NewFunctionRuntime() *FunctionRuntime`

NewFunctionRuntime instantiates a new FunctionRuntime object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewFunctionRuntimeWithDefaults

`func NewFunctionRuntimeWithDefaults() *FunctionRuntime`

NewFunctionRuntimeWithDefaults instantiates a new FunctionRuntime object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfig

`func (o *FunctionRuntime) GetConfig() map[string]interface{}`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *FunctionRuntime) GetConfigOk() (*map[string]interface{}, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *FunctionRuntime) SetConfig(v map[string]interface{})`

SetConfig sets Config field to given value.

### HasConfig

`func (o *FunctionRuntime) HasConfig() bool`

HasConfig returns a boolean if a field has been set.

### GetType

`func (o *FunctionRuntime) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *FunctionRuntime) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *FunctionRuntime) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *FunctionRuntime) HasType() bool`

HasType returns a boolean if a field has been set.

### SetTypeNil

`func (o *FunctionRuntime) SetTypeNil(b bool)`

 SetTypeNil sets the value for Type to be an explicit nil

### UnsetType
`func (o *FunctionRuntime) UnsetType()`

UnsetType ensures that no value is present for Type, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


