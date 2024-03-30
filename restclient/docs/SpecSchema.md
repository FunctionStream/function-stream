# SpecSchema

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**VendorExtensible** | **string** |  | 
**AdditionalItems** | Pointer to **string** |  | [optional] 
**AdditionalPropertiesField** | Pointer to **string** |  | [optional] 
**AllOf** | Pointer to [**[]SpecSchema**](SpecSchema.md) |  | [optional] 
**AnyOf** | Pointer to [**[]SpecSchema**](SpecSchema.md) |  | [optional] 
**Default** | Pointer to **map[string]interface{}** |  | [optional] 
**Definitions** | Pointer to [**map[string]SpecSchema**](SpecSchema.md) |  | [optional] 
**Dependencies** | Pointer to [**map[string]SpecSchemaOrStringArray**](SpecSchemaOrStringArray.md) |  | [optional] 
**Description** | Pointer to **string** |  | [optional] 
**Discriminator** | Pointer to **string** |  | [optional] 
**Enum** | Pointer to **[]map[string]interface{}** |  | [optional] 
**Example** | Pointer to **map[string]interface{}** |  | [optional] 
**ExclusiveMaximum** | Pointer to **bool** |  | [optional] 
**ExclusiveMinimum** | Pointer to **bool** |  | [optional] 
**ExternalDocs** | Pointer to [**SpecExternalDocumentation**](SpecExternalDocumentation.md) |  | [optional] 
**Format** | Pointer to **string** |  | [optional] 
**Id** | Pointer to **string** |  | [optional] 
**Items** | Pointer to **string** |  | [optional] 
**MaxItems** | Pointer to **int64** |  | [optional] 
**MaxLength** | Pointer to **int64** |  | [optional] 
**MaxProperties** | Pointer to **int64** |  | [optional] 
**Maximum** | Pointer to **float64** |  | [optional] 
**MinItems** | Pointer to **int64** |  | [optional] 
**MinLength** | Pointer to **int64** |  | [optional] 
**MinProperties** | Pointer to **int64** |  | [optional] 
**Minimum** | Pointer to **float64** |  | [optional] 
**MultipleOf** | Pointer to **float64** |  | [optional] 
**Not** | Pointer to **string** |  | [optional] 
**Nullable** | Pointer to **bool** |  | [optional] 
**OneOf** | Pointer to [**[]SpecSchema**](SpecSchema.md) |  | [optional] 
**Pattern** | Pointer to **string** |  | [optional] 
**PatternProperties** | Pointer to **string** |  | [optional] 
**Properties** | Pointer to **string** |  | [optional] 
**ReadOnly** | Pointer to **bool** |  | [optional] 
**Required** | Pointer to **[]string** |  | [optional] 
**Title** | Pointer to **string** |  | [optional] 
**Type** | Pointer to **string** |  | [optional] 
**UniqueItems** | Pointer to **bool** |  | [optional] 
**Xml** | Pointer to [**SpecXMLObject**](SpecXMLObject.md) |  | [optional] 

## Methods

### NewSpecSchema

`func NewSpecSchema(vendorExtensible string, ) *SpecSchema`

NewSpecSchema instantiates a new SpecSchema object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSpecSchemaWithDefaults

`func NewSpecSchemaWithDefaults() *SpecSchema`

NewSpecSchemaWithDefaults instantiates a new SpecSchema object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetVendorExtensible

`func (o *SpecSchema) GetVendorExtensible() string`

GetVendorExtensible returns the VendorExtensible field if non-nil, zero value otherwise.

### GetVendorExtensibleOk

`func (o *SpecSchema) GetVendorExtensibleOk() (*string, bool)`

GetVendorExtensibleOk returns a tuple with the VendorExtensible field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVendorExtensible

`func (o *SpecSchema) SetVendorExtensible(v string)`

SetVendorExtensible sets VendorExtensible field to given value.


### GetAdditionalItems

`func (o *SpecSchema) GetAdditionalItems() string`

GetAdditionalItems returns the AdditionalItems field if non-nil, zero value otherwise.

### GetAdditionalItemsOk

`func (o *SpecSchema) GetAdditionalItemsOk() (*string, bool)`

GetAdditionalItemsOk returns a tuple with the AdditionalItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalItems

`func (o *SpecSchema) SetAdditionalItems(v string)`

SetAdditionalItems sets AdditionalItems field to given value.

### HasAdditionalItems

`func (o *SpecSchema) HasAdditionalItems() bool`

HasAdditionalItems returns a boolean if a field has been set.

### GetAdditionalPropertiesField

`func (o *SpecSchema) GetAdditionalPropertiesField() string`

GetAdditionalPropertiesField returns the AdditionalPropertiesField field if non-nil, zero value otherwise.

### GetAdditionalPropertiesFieldOk

`func (o *SpecSchema) GetAdditionalPropertiesFieldOk() (*string, bool)`

GetAdditionalPropertiesFieldOk returns a tuple with the AdditionalPropertiesField field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalPropertiesField

`func (o *SpecSchema) SetAdditionalPropertiesField(v string)`

SetAdditionalPropertiesField sets AdditionalPropertiesField field to given value.

### HasAdditionalPropertiesField

`func (o *SpecSchema) HasAdditionalPropertiesField() bool`

HasAdditionalPropertiesField returns a boolean if a field has been set.

### GetAllOf

`func (o *SpecSchema) GetAllOf() []SpecSchema`

GetAllOf returns the AllOf field if non-nil, zero value otherwise.

### GetAllOfOk

`func (o *SpecSchema) GetAllOfOk() (*[]SpecSchema, bool)`

GetAllOfOk returns a tuple with the AllOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAllOf

`func (o *SpecSchema) SetAllOf(v []SpecSchema)`

SetAllOf sets AllOf field to given value.

### HasAllOf

`func (o *SpecSchema) HasAllOf() bool`

HasAllOf returns a boolean if a field has been set.

### GetAnyOf

`func (o *SpecSchema) GetAnyOf() []SpecSchema`

GetAnyOf returns the AnyOf field if non-nil, zero value otherwise.

### GetAnyOfOk

`func (o *SpecSchema) GetAnyOfOk() (*[]SpecSchema, bool)`

GetAnyOfOk returns a tuple with the AnyOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAnyOf

`func (o *SpecSchema) SetAnyOf(v []SpecSchema)`

SetAnyOf sets AnyOf field to given value.

### HasAnyOf

`func (o *SpecSchema) HasAnyOf() bool`

HasAnyOf returns a boolean if a field has been set.

### GetDefault

`func (o *SpecSchema) GetDefault() map[string]interface{}`

GetDefault returns the Default field if non-nil, zero value otherwise.

### GetDefaultOk

`func (o *SpecSchema) GetDefaultOk() (*map[string]interface{}, bool)`

GetDefaultOk returns a tuple with the Default field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefault

`func (o *SpecSchema) SetDefault(v map[string]interface{})`

SetDefault sets Default field to given value.

### HasDefault

`func (o *SpecSchema) HasDefault() bool`

HasDefault returns a boolean if a field has been set.

### GetDefinitions

`func (o *SpecSchema) GetDefinitions() map[string]SpecSchema`

GetDefinitions returns the Definitions field if non-nil, zero value otherwise.

### GetDefinitionsOk

`func (o *SpecSchema) GetDefinitionsOk() (*map[string]SpecSchema, bool)`

GetDefinitionsOk returns a tuple with the Definitions field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefinitions

`func (o *SpecSchema) SetDefinitions(v map[string]SpecSchema)`

SetDefinitions sets Definitions field to given value.

### HasDefinitions

`func (o *SpecSchema) HasDefinitions() bool`

HasDefinitions returns a boolean if a field has been set.

### GetDependencies

`func (o *SpecSchema) GetDependencies() map[string]SpecSchemaOrStringArray`

GetDependencies returns the Dependencies field if non-nil, zero value otherwise.

### GetDependenciesOk

`func (o *SpecSchema) GetDependenciesOk() (*map[string]SpecSchemaOrStringArray, bool)`

GetDependenciesOk returns a tuple with the Dependencies field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDependencies

`func (o *SpecSchema) SetDependencies(v map[string]SpecSchemaOrStringArray)`

SetDependencies sets Dependencies field to given value.

### HasDependencies

`func (o *SpecSchema) HasDependencies() bool`

HasDependencies returns a boolean if a field has been set.

### GetDescription

`func (o *SpecSchema) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *SpecSchema) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *SpecSchema) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *SpecSchema) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetDiscriminator

`func (o *SpecSchema) GetDiscriminator() string`

GetDiscriminator returns the Discriminator field if non-nil, zero value otherwise.

### GetDiscriminatorOk

`func (o *SpecSchema) GetDiscriminatorOk() (*string, bool)`

GetDiscriminatorOk returns a tuple with the Discriminator field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDiscriminator

`func (o *SpecSchema) SetDiscriminator(v string)`

SetDiscriminator sets Discriminator field to given value.

### HasDiscriminator

`func (o *SpecSchema) HasDiscriminator() bool`

HasDiscriminator returns a boolean if a field has been set.

### GetEnum

`func (o *SpecSchema) GetEnum() []map[string]interface{}`

GetEnum returns the Enum field if non-nil, zero value otherwise.

### GetEnumOk

`func (o *SpecSchema) GetEnumOk() (*[]map[string]interface{}, bool)`

GetEnumOk returns a tuple with the Enum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEnum

`func (o *SpecSchema) SetEnum(v []map[string]interface{})`

SetEnum sets Enum field to given value.

### HasEnum

`func (o *SpecSchema) HasEnum() bool`

HasEnum returns a boolean if a field has been set.

### GetExample

`func (o *SpecSchema) GetExample() map[string]interface{}`

GetExample returns the Example field if non-nil, zero value otherwise.

### GetExampleOk

`func (o *SpecSchema) GetExampleOk() (*map[string]interface{}, bool)`

GetExampleOk returns a tuple with the Example field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExample

`func (o *SpecSchema) SetExample(v map[string]interface{})`

SetExample sets Example field to given value.

### HasExample

`func (o *SpecSchema) HasExample() bool`

HasExample returns a boolean if a field has been set.

### GetExclusiveMaximum

`func (o *SpecSchema) GetExclusiveMaximum() bool`

GetExclusiveMaximum returns the ExclusiveMaximum field if non-nil, zero value otherwise.

### GetExclusiveMaximumOk

`func (o *SpecSchema) GetExclusiveMaximumOk() (*bool, bool)`

GetExclusiveMaximumOk returns a tuple with the ExclusiveMaximum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExclusiveMaximum

`func (o *SpecSchema) SetExclusiveMaximum(v bool)`

SetExclusiveMaximum sets ExclusiveMaximum field to given value.

### HasExclusiveMaximum

`func (o *SpecSchema) HasExclusiveMaximum() bool`

HasExclusiveMaximum returns a boolean if a field has been set.

### GetExclusiveMinimum

`func (o *SpecSchema) GetExclusiveMinimum() bool`

GetExclusiveMinimum returns the ExclusiveMinimum field if non-nil, zero value otherwise.

### GetExclusiveMinimumOk

`func (o *SpecSchema) GetExclusiveMinimumOk() (*bool, bool)`

GetExclusiveMinimumOk returns a tuple with the ExclusiveMinimum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExclusiveMinimum

`func (o *SpecSchema) SetExclusiveMinimum(v bool)`

SetExclusiveMinimum sets ExclusiveMinimum field to given value.

### HasExclusiveMinimum

`func (o *SpecSchema) HasExclusiveMinimum() bool`

HasExclusiveMinimum returns a boolean if a field has been set.

### GetExternalDocs

`func (o *SpecSchema) GetExternalDocs() SpecExternalDocumentation`

GetExternalDocs returns the ExternalDocs field if non-nil, zero value otherwise.

### GetExternalDocsOk

`func (o *SpecSchema) GetExternalDocsOk() (*SpecExternalDocumentation, bool)`

GetExternalDocsOk returns a tuple with the ExternalDocs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExternalDocs

`func (o *SpecSchema) SetExternalDocs(v SpecExternalDocumentation)`

SetExternalDocs sets ExternalDocs field to given value.

### HasExternalDocs

`func (o *SpecSchema) HasExternalDocs() bool`

HasExternalDocs returns a boolean if a field has been set.

### GetFormat

`func (o *SpecSchema) GetFormat() string`

GetFormat returns the Format field if non-nil, zero value otherwise.

### GetFormatOk

`func (o *SpecSchema) GetFormatOk() (*string, bool)`

GetFormatOk returns a tuple with the Format field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFormat

`func (o *SpecSchema) SetFormat(v string)`

SetFormat sets Format field to given value.

### HasFormat

`func (o *SpecSchema) HasFormat() bool`

HasFormat returns a boolean if a field has been set.

### GetId

`func (o *SpecSchema) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *SpecSchema) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *SpecSchema) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *SpecSchema) HasId() bool`

HasId returns a boolean if a field has been set.

### GetItems

`func (o *SpecSchema) GetItems() string`

GetItems returns the Items field if non-nil, zero value otherwise.

### GetItemsOk

`func (o *SpecSchema) GetItemsOk() (*string, bool)`

GetItemsOk returns a tuple with the Items field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetItems

`func (o *SpecSchema) SetItems(v string)`

SetItems sets Items field to given value.

### HasItems

`func (o *SpecSchema) HasItems() bool`

HasItems returns a boolean if a field has been set.

### GetMaxItems

`func (o *SpecSchema) GetMaxItems() int64`

GetMaxItems returns the MaxItems field if non-nil, zero value otherwise.

### GetMaxItemsOk

`func (o *SpecSchema) GetMaxItemsOk() (*int64, bool)`

GetMaxItemsOk returns a tuple with the MaxItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxItems

`func (o *SpecSchema) SetMaxItems(v int64)`

SetMaxItems sets MaxItems field to given value.

### HasMaxItems

`func (o *SpecSchema) HasMaxItems() bool`

HasMaxItems returns a boolean if a field has been set.

### GetMaxLength

`func (o *SpecSchema) GetMaxLength() int64`

GetMaxLength returns the MaxLength field if non-nil, zero value otherwise.

### GetMaxLengthOk

`func (o *SpecSchema) GetMaxLengthOk() (*int64, bool)`

GetMaxLengthOk returns a tuple with the MaxLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxLength

`func (o *SpecSchema) SetMaxLength(v int64)`

SetMaxLength sets MaxLength field to given value.

### HasMaxLength

`func (o *SpecSchema) HasMaxLength() bool`

HasMaxLength returns a boolean if a field has been set.

### GetMaxProperties

`func (o *SpecSchema) GetMaxProperties() int64`

GetMaxProperties returns the MaxProperties field if non-nil, zero value otherwise.

### GetMaxPropertiesOk

`func (o *SpecSchema) GetMaxPropertiesOk() (*int64, bool)`

GetMaxPropertiesOk returns a tuple with the MaxProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxProperties

`func (o *SpecSchema) SetMaxProperties(v int64)`

SetMaxProperties sets MaxProperties field to given value.

### HasMaxProperties

`func (o *SpecSchema) HasMaxProperties() bool`

HasMaxProperties returns a boolean if a field has been set.

### GetMaximum

`func (o *SpecSchema) GetMaximum() float64`

GetMaximum returns the Maximum field if non-nil, zero value otherwise.

### GetMaximumOk

`func (o *SpecSchema) GetMaximumOk() (*float64, bool)`

GetMaximumOk returns a tuple with the Maximum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaximum

`func (o *SpecSchema) SetMaximum(v float64)`

SetMaximum sets Maximum field to given value.

### HasMaximum

`func (o *SpecSchema) HasMaximum() bool`

HasMaximum returns a boolean if a field has been set.

### GetMinItems

`func (o *SpecSchema) GetMinItems() int64`

GetMinItems returns the MinItems field if non-nil, zero value otherwise.

### GetMinItemsOk

`func (o *SpecSchema) GetMinItemsOk() (*int64, bool)`

GetMinItemsOk returns a tuple with the MinItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinItems

`func (o *SpecSchema) SetMinItems(v int64)`

SetMinItems sets MinItems field to given value.

### HasMinItems

`func (o *SpecSchema) HasMinItems() bool`

HasMinItems returns a boolean if a field has been set.

### GetMinLength

`func (o *SpecSchema) GetMinLength() int64`

GetMinLength returns the MinLength field if non-nil, zero value otherwise.

### GetMinLengthOk

`func (o *SpecSchema) GetMinLengthOk() (*int64, bool)`

GetMinLengthOk returns a tuple with the MinLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinLength

`func (o *SpecSchema) SetMinLength(v int64)`

SetMinLength sets MinLength field to given value.

### HasMinLength

`func (o *SpecSchema) HasMinLength() bool`

HasMinLength returns a boolean if a field has been set.

### GetMinProperties

`func (o *SpecSchema) GetMinProperties() int64`

GetMinProperties returns the MinProperties field if non-nil, zero value otherwise.

### GetMinPropertiesOk

`func (o *SpecSchema) GetMinPropertiesOk() (*int64, bool)`

GetMinPropertiesOk returns a tuple with the MinProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinProperties

`func (o *SpecSchema) SetMinProperties(v int64)`

SetMinProperties sets MinProperties field to given value.

### HasMinProperties

`func (o *SpecSchema) HasMinProperties() bool`

HasMinProperties returns a boolean if a field has been set.

### GetMinimum

`func (o *SpecSchema) GetMinimum() float64`

GetMinimum returns the Minimum field if non-nil, zero value otherwise.

### GetMinimumOk

`func (o *SpecSchema) GetMinimumOk() (*float64, bool)`

GetMinimumOk returns a tuple with the Minimum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinimum

`func (o *SpecSchema) SetMinimum(v float64)`

SetMinimum sets Minimum field to given value.

### HasMinimum

`func (o *SpecSchema) HasMinimum() bool`

HasMinimum returns a boolean if a field has been set.

### GetMultipleOf

`func (o *SpecSchema) GetMultipleOf() float64`

GetMultipleOf returns the MultipleOf field if non-nil, zero value otherwise.

### GetMultipleOfOk

`func (o *SpecSchema) GetMultipleOfOk() (*float64, bool)`

GetMultipleOfOk returns a tuple with the MultipleOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMultipleOf

`func (o *SpecSchema) SetMultipleOf(v float64)`

SetMultipleOf sets MultipleOf field to given value.

### HasMultipleOf

`func (o *SpecSchema) HasMultipleOf() bool`

HasMultipleOf returns a boolean if a field has been set.

### GetNot

`func (o *SpecSchema) GetNot() string`

GetNot returns the Not field if non-nil, zero value otherwise.

### GetNotOk

`func (o *SpecSchema) GetNotOk() (*string, bool)`

GetNotOk returns a tuple with the Not field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNot

`func (o *SpecSchema) SetNot(v string)`

SetNot sets Not field to given value.

### HasNot

`func (o *SpecSchema) HasNot() bool`

HasNot returns a boolean if a field has been set.

### GetNullable

`func (o *SpecSchema) GetNullable() bool`

GetNullable returns the Nullable field if non-nil, zero value otherwise.

### GetNullableOk

`func (o *SpecSchema) GetNullableOk() (*bool, bool)`

GetNullableOk returns a tuple with the Nullable field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNullable

`func (o *SpecSchema) SetNullable(v bool)`

SetNullable sets Nullable field to given value.

### HasNullable

`func (o *SpecSchema) HasNullable() bool`

HasNullable returns a boolean if a field has been set.

### GetOneOf

`func (o *SpecSchema) GetOneOf() []SpecSchema`

GetOneOf returns the OneOf field if non-nil, zero value otherwise.

### GetOneOfOk

`func (o *SpecSchema) GetOneOfOk() (*[]SpecSchema, bool)`

GetOneOfOk returns a tuple with the OneOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOneOf

`func (o *SpecSchema) SetOneOf(v []SpecSchema)`

SetOneOf sets OneOf field to given value.

### HasOneOf

`func (o *SpecSchema) HasOneOf() bool`

HasOneOf returns a boolean if a field has been set.

### GetPattern

`func (o *SpecSchema) GetPattern() string`

GetPattern returns the Pattern field if non-nil, zero value otherwise.

### GetPatternOk

`func (o *SpecSchema) GetPatternOk() (*string, bool)`

GetPatternOk returns a tuple with the Pattern field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPattern

`func (o *SpecSchema) SetPattern(v string)`

SetPattern sets Pattern field to given value.

### HasPattern

`func (o *SpecSchema) HasPattern() bool`

HasPattern returns a boolean if a field has been set.

### GetPatternProperties

`func (o *SpecSchema) GetPatternProperties() string`

GetPatternProperties returns the PatternProperties field if non-nil, zero value otherwise.

### GetPatternPropertiesOk

`func (o *SpecSchema) GetPatternPropertiesOk() (*string, bool)`

GetPatternPropertiesOk returns a tuple with the PatternProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPatternProperties

`func (o *SpecSchema) SetPatternProperties(v string)`

SetPatternProperties sets PatternProperties field to given value.

### HasPatternProperties

`func (o *SpecSchema) HasPatternProperties() bool`

HasPatternProperties returns a boolean if a field has been set.

### GetProperties

`func (o *SpecSchema) GetProperties() string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *SpecSchema) GetPropertiesOk() (*string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *SpecSchema) SetProperties(v string)`

SetProperties sets Properties field to given value.

### HasProperties

`func (o *SpecSchema) HasProperties() bool`

HasProperties returns a boolean if a field has been set.

### GetReadOnly

`func (o *SpecSchema) GetReadOnly() bool`

GetReadOnly returns the ReadOnly field if non-nil, zero value otherwise.

### GetReadOnlyOk

`func (o *SpecSchema) GetReadOnlyOk() (*bool, bool)`

GetReadOnlyOk returns a tuple with the ReadOnly field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReadOnly

`func (o *SpecSchema) SetReadOnly(v bool)`

SetReadOnly sets ReadOnly field to given value.

### HasReadOnly

`func (o *SpecSchema) HasReadOnly() bool`

HasReadOnly returns a boolean if a field has been set.

### GetRequired

`func (o *SpecSchema) GetRequired() []string`

GetRequired returns the Required field if non-nil, zero value otherwise.

### GetRequiredOk

`func (o *SpecSchema) GetRequiredOk() (*[]string, bool)`

GetRequiredOk returns a tuple with the Required field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRequired

`func (o *SpecSchema) SetRequired(v []string)`

SetRequired sets Required field to given value.

### HasRequired

`func (o *SpecSchema) HasRequired() bool`

HasRequired returns a boolean if a field has been set.

### GetTitle

`func (o *SpecSchema) GetTitle() string`

GetTitle returns the Title field if non-nil, zero value otherwise.

### GetTitleOk

`func (o *SpecSchema) GetTitleOk() (*string, bool)`

GetTitleOk returns a tuple with the Title field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTitle

`func (o *SpecSchema) SetTitle(v string)`

SetTitle sets Title field to given value.

### HasTitle

`func (o *SpecSchema) HasTitle() bool`

HasTitle returns a boolean if a field has been set.

### GetType

`func (o *SpecSchema) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *SpecSchema) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *SpecSchema) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *SpecSchema) HasType() bool`

HasType returns a boolean if a field has been set.

### GetUniqueItems

`func (o *SpecSchema) GetUniqueItems() bool`

GetUniqueItems returns the UniqueItems field if non-nil, zero value otherwise.

### GetUniqueItemsOk

`func (o *SpecSchema) GetUniqueItemsOk() (*bool, bool)`

GetUniqueItemsOk returns a tuple with the UniqueItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUniqueItems

`func (o *SpecSchema) SetUniqueItems(v bool)`

SetUniqueItems sets UniqueItems field to given value.

### HasUniqueItems

`func (o *SpecSchema) HasUniqueItems() bool`

HasUniqueItems returns a boolean if a field has been set.

### GetXml

`func (o *SpecSchema) GetXml() SpecXMLObject`

GetXml returns the Xml field if non-nil, zero value otherwise.

### GetXmlOk

`func (o *SpecSchema) GetXmlOk() (*SpecXMLObject, bool)`

GetXmlOk returns a tuple with the Xml field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetXml

`func (o *SpecSchema) SetXml(v SpecXMLObject)`

SetXml sets Xml field to given value.

### HasXml

`func (o *SpecSchema) HasXml() bool`

HasXml returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


