# SpecSwaggerSchemaProps

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Discriminator** | Pointer to **string** |  | [optional] 
**Example** | Pointer to **map[string]interface{}** |  | [optional] 
**ExternalDocs** | Pointer to [**SpecExternalDocumentation**](SpecExternalDocumentation.md) |  | [optional] 
**ReadOnly** | Pointer to **bool** |  | [optional] 
**Xml** | Pointer to [**SpecXMLObject**](SpecXMLObject.md) |  | [optional] 

## Methods

### NewSpecSwaggerSchemaProps

`func NewSpecSwaggerSchemaProps() *SpecSwaggerSchemaProps`

NewSpecSwaggerSchemaProps instantiates a new SpecSwaggerSchemaProps object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSpecSwaggerSchemaPropsWithDefaults

`func NewSpecSwaggerSchemaPropsWithDefaults() *SpecSwaggerSchemaProps`

NewSpecSwaggerSchemaPropsWithDefaults instantiates a new SpecSwaggerSchemaProps object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDiscriminator

`func (o *SpecSwaggerSchemaProps) GetDiscriminator() string`

GetDiscriminator returns the Discriminator field if non-nil, zero value otherwise.

### GetDiscriminatorOk

`func (o *SpecSwaggerSchemaProps) GetDiscriminatorOk() (*string, bool)`

GetDiscriminatorOk returns a tuple with the Discriminator field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDiscriminator

`func (o *SpecSwaggerSchemaProps) SetDiscriminator(v string)`

SetDiscriminator sets Discriminator field to given value.

### HasDiscriminator

`func (o *SpecSwaggerSchemaProps) HasDiscriminator() bool`

HasDiscriminator returns a boolean if a field has been set.

### GetExample

`func (o *SpecSwaggerSchemaProps) GetExample() map[string]interface{}`

GetExample returns the Example field if non-nil, zero value otherwise.

### GetExampleOk

`func (o *SpecSwaggerSchemaProps) GetExampleOk() (*map[string]interface{}, bool)`

GetExampleOk returns a tuple with the Example field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExample

`func (o *SpecSwaggerSchemaProps) SetExample(v map[string]interface{})`

SetExample sets Example field to given value.

### HasExample

`func (o *SpecSwaggerSchemaProps) HasExample() bool`

HasExample returns a boolean if a field has been set.

### GetExternalDocs

`func (o *SpecSwaggerSchemaProps) GetExternalDocs() SpecExternalDocumentation`

GetExternalDocs returns the ExternalDocs field if non-nil, zero value otherwise.

### GetExternalDocsOk

`func (o *SpecSwaggerSchemaProps) GetExternalDocsOk() (*SpecExternalDocumentation, bool)`

GetExternalDocsOk returns a tuple with the ExternalDocs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExternalDocs

`func (o *SpecSwaggerSchemaProps) SetExternalDocs(v SpecExternalDocumentation)`

SetExternalDocs sets ExternalDocs field to given value.

### HasExternalDocs

`func (o *SpecSwaggerSchemaProps) HasExternalDocs() bool`

HasExternalDocs returns a boolean if a field has been set.

### GetReadOnly

`func (o *SpecSwaggerSchemaProps) GetReadOnly() bool`

GetReadOnly returns the ReadOnly field if non-nil, zero value otherwise.

### GetReadOnlyOk

`func (o *SpecSwaggerSchemaProps) GetReadOnlyOk() (*bool, bool)`

GetReadOnlyOk returns a tuple with the ReadOnly field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReadOnly

`func (o *SpecSwaggerSchemaProps) SetReadOnly(v bool)`

SetReadOnly sets ReadOnly field to given value.

### HasReadOnly

`func (o *SpecSwaggerSchemaProps) HasReadOnly() bool`

HasReadOnly returns a boolean if a field has been set.

### GetXml

`func (o *SpecSwaggerSchemaProps) GetXml() SpecXMLObject`

GetXml returns the Xml field if non-nil, zero value otherwise.

### GetXmlOk

`func (o *SpecSwaggerSchemaProps) GetXmlOk() (*SpecXMLObject, bool)`

GetXmlOk returns a tuple with the Xml field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetXml

`func (o *SpecSwaggerSchemaProps) SetXml(v SpecXMLObject)`

SetXml sets Xml field to given value.

### HasXml

`func (o *SpecSwaggerSchemaProps) HasXml() bool`

HasXml returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


