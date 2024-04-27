# ModelFunction

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Config** | Pointer to **map[string]string** |  | [optional] 
**Name** | **string** |  | 
**Replicas** | **int32** |  | 
**Runtime** | [**ModelRuntimeConfig**](ModelRuntimeConfig.md) |  | 
**Sink** | Pointer to [**ModelTubeConfig**](ModelTubeConfig.md) |  | [optional] 
**Source** | Pointer to [**[]ModelTubeConfig**](ModelTubeConfig.md) |  | [optional] 

## Methods

### NewModelFunction

`func NewModelFunction(name string, replicas int32, runtime ModelRuntimeConfig, ) *ModelFunction`

NewModelFunction instantiates a new ModelFunction object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewModelFunctionWithDefaults

`func NewModelFunctionWithDefaults() *ModelFunction`

NewModelFunctionWithDefaults instantiates a new ModelFunction object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfig

`func (o *ModelFunction) GetConfig() map[string]string`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *ModelFunction) GetConfigOk() (*map[string]string, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *ModelFunction) SetConfig(v map[string]string)`

SetConfig sets Config field to given value.

### HasConfig

`func (o *ModelFunction) HasConfig() bool`

HasConfig returns a boolean if a field has been set.

### GetName

`func (o *ModelFunction) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ModelFunction) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ModelFunction) SetName(v string)`

SetName sets Name field to given value.


### GetReplicas

`func (o *ModelFunction) GetReplicas() int32`

GetReplicas returns the Replicas field if non-nil, zero value otherwise.

### GetReplicasOk

`func (o *ModelFunction) GetReplicasOk() (*int32, bool)`

GetReplicasOk returns a tuple with the Replicas field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReplicas

`func (o *ModelFunction) SetReplicas(v int32)`

SetReplicas sets Replicas field to given value.


### GetRuntime

`func (o *ModelFunction) GetRuntime() ModelRuntimeConfig`

GetRuntime returns the Runtime field if non-nil, zero value otherwise.

### GetRuntimeOk

`func (o *ModelFunction) GetRuntimeOk() (*ModelRuntimeConfig, bool)`

GetRuntimeOk returns a tuple with the Runtime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRuntime

`func (o *ModelFunction) SetRuntime(v ModelRuntimeConfig)`

SetRuntime sets Runtime field to given value.


### GetSink

`func (o *ModelFunction) GetSink() ModelTubeConfig`

GetSink returns the Sink field if non-nil, zero value otherwise.

### GetSinkOk

`func (o *ModelFunction) GetSinkOk() (*ModelTubeConfig, bool)`

GetSinkOk returns a tuple with the Sink field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSink

`func (o *ModelFunction) SetSink(v ModelTubeConfig)`

SetSink sets Sink field to given value.

### HasSink

`func (o *ModelFunction) HasSink() bool`

HasSink returns a boolean if a field has been set.

### GetSource

`func (o *ModelFunction) GetSource() []ModelTubeConfig`

GetSource returns the Source field if non-nil, zero value otherwise.

### GetSourceOk

`func (o *ModelFunction) GetSourceOk() (*[]ModelTubeConfig, bool)`

GetSourceOk returns a tuple with the Source field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSource

`func (o *ModelFunction) SetSource(v []ModelTubeConfig)`

SetSource sets Source field to given value.

### HasSource

`func (o *ModelFunction) HasSource() bool`

HasSource returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


