# Function

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | Pointer to **string** |  | [optional] 
**Archive** | **string** |  | 
**Inputs** | **[]string** |  | 
**Output** | **string** |  | 
**Config** | Pointer to **map[string]string** |  | [optional] 

## Methods

### NewFunction

`func NewFunction(archive string, inputs []string, output string, ) *Function`

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

### GetArchive

`func (o *Function) GetArchive() string`

GetArchive returns the Archive field if non-nil, zero value otherwise.

### GetArchiveOk

`func (o *Function) GetArchiveOk() (*string, bool)`

GetArchiveOk returns a tuple with the Archive field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetArchive

`func (o *Function) SetArchive(v string)`

SetArchive sets Archive field to given value.


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


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


