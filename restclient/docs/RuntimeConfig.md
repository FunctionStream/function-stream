# RuntimeConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Config** | Pointer to **map[string]interface{}** |  | [optional] 
**Type** | Pointer to **NullableString** |  | [optional] 

## Methods

### NewRuntimeConfig

`func NewRuntimeConfig() *RuntimeConfig`

NewRuntimeConfig instantiates a new RuntimeConfig object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewRuntimeConfigWithDefaults

`func NewRuntimeConfigWithDefaults() *RuntimeConfig`

NewRuntimeConfigWithDefaults instantiates a new RuntimeConfig object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfig

`func (o *RuntimeConfig) GetConfig() map[string]interface{}`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *RuntimeConfig) GetConfigOk() (*map[string]interface{}, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *RuntimeConfig) SetConfig(v map[string]interface{})`

SetConfig sets Config field to given value.

### HasConfig

`func (o *RuntimeConfig) HasConfig() bool`

HasConfig returns a boolean if a field has been set.

### GetType

`func (o *RuntimeConfig) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *RuntimeConfig) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *RuntimeConfig) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *RuntimeConfig) HasType() bool`

HasType returns a boolean if a field has been set.

### SetTypeNil

`func (o *RuntimeConfig) SetTypeNil(b bool)`

 SetTypeNil sets the value for Type to be an explicit nil

### UnsetType
`func (o *RuntimeConfig) UnsetType()`

UnsetType ensures that no value is present for Type, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


