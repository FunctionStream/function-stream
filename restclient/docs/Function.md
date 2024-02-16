# Function

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | Pointer to **string** |  | [optional] 
**Runtime** | Pointer to [**FunctionRuntime**](FunctionRuntime.md) |  | [optional] 
**Source** | [**FunctionSource**](FunctionSource.md) |  | 
**Sink** | [**FunctionSource**](FunctionSource.md) |  | 
**Inputs** | **[]string** |  | 
**Output** | **string** |  | 
**Config** | Pointer to **map[string]string** |  | [optional] 
**Replicas** | **int32** |  | 

## Methods

### NewFunction

`func NewFunction(source FunctionSource, sink FunctionSource, inputs []string, output string, replicas int32, ) *Function`

NewFunction instantiates a new Function object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewFunctionWithDefaults

`func NewFunctionWithDefaults() *Function`

NewFunctionWithDefaults instantiates a new Function object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *Function) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Function) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Function) SetName(v string)`

SetName sets Name field to given value.

### HasName

`func (o *Function) HasName() bool`

HasName returns a boolean if a field has been set.

### GetRuntime

`func (o *Function) GetRuntime() FunctionRuntime`

GetRuntime returns the Runtime field if non-nil, zero value otherwise.

### GetRuntimeOk

`func (o *Function) GetRuntimeOk() (*FunctionRuntime, bool)`

GetRuntimeOk returns a tuple with the Runtime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRuntime

`func (o *Function) SetRuntime(v FunctionRuntime)`

SetRuntime sets Runtime field to given value.

### HasRuntime

`func (o *Function) HasRuntime() bool`

HasRuntime returns a boolean if a field has been set.

### GetSource

`func (o *Function) GetSource() FunctionSource`

GetSource returns the Source field if non-nil, zero value otherwise.

### GetSourceOk

`func (o *Function) GetSourceOk() (*FunctionSource, bool)`

GetSourceOk returns a tuple with the Source field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSource

`func (o *Function) SetSource(v FunctionSource)`

SetSource sets Source field to given value.


### GetSink

`func (o *Function) GetSink() FunctionSource`

GetSink returns the Sink field if non-nil, zero value otherwise.

### GetSinkOk

`func (o *Function) GetSinkOk() (*FunctionSource, bool)`

GetSinkOk returns a tuple with the Sink field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSink

`func (o *Function) SetSink(v FunctionSource)`

SetSink sets Sink field to given value.


### GetInputs

`func (o *Function) GetInputs() []string`

GetInputs returns the Inputs field if non-nil, zero value otherwise.

### GetInputsOk

`func (o *Function) GetInputsOk() (*[]string, bool)`

GetInputsOk returns a tuple with the Inputs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetInputs

`func (o *Function) SetInputs(v []string)`

SetInputs sets Inputs field to given value.


### GetOutput

`func (o *Function) GetOutput() string`

GetOutput returns the Output field if non-nil, zero value otherwise.

### GetOutputOk

`func (o *Function) GetOutputOk() (*string, bool)`

GetOutputOk returns a tuple with the Output field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOutput

`func (o *Function) SetOutput(v string)`

SetOutput sets Output field to given value.


### GetConfig

`func (o *Function) GetConfig() map[string]string`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *Function) GetConfigOk() (*map[string]string, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *Function) SetConfig(v map[string]string)`

SetConfig sets Config field to given value.

### HasConfig

`func (o *Function) HasConfig() bool`

HasConfig returns a boolean if a field has been set.

### GetReplicas

`func (o *Function) GetReplicas() int32`

GetReplicas returns the Replicas field if non-nil, zero value otherwise.

### GetReplicasOk

`func (o *Function) GetReplicasOk() (*int32, bool)`

GetReplicasOk returns a tuple with the Replicas field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReplicas

`func (o *Function) SetReplicas(v int32)`

SetReplicas sets Replicas field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


