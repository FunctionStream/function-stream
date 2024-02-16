# FunctionSource

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Config** | Pointer to **map[string]interface{}** |  | [optional] 
**Type** | Pointer to **NullableString** |  | [optional] 

## Methods

### NewFunctionSource

`func NewFunctionSource() *FunctionSource`

NewFunctionSource instantiates a new FunctionSource object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewFunctionSourceWithDefaults

`func NewFunctionSourceWithDefaults() *FunctionSource`

NewFunctionSourceWithDefaults instantiates a new FunctionSource object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfig

`func (o *FunctionSource) GetConfig() map[string]interface{}`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *FunctionSource) GetConfigOk() (*map[string]interface{}, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *FunctionSource) SetConfig(v map[string]interface{})`

SetConfig sets Config field to given value.

### HasConfig

`func (o *FunctionSource) HasConfig() bool`

HasConfig returns a boolean if a field has been set.

### GetType

`func (o *FunctionSource) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *FunctionSource) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *FunctionSource) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *FunctionSource) HasType() bool`

HasType returns a boolean if a field has been set.

### SetTypeNil

`func (o *FunctionSource) SetTypeNil(b bool)`

 SetTypeNil sets the value for Type to be an explicit nil

### UnsetType
`func (o *FunctionSource) UnsetType()`

UnsetType ensures that no value is present for Type, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


