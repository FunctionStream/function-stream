# SpecSchemaProps

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**AdditionalItems** | Pointer to **string** |  | [optional] 
**AdditionalPropertiesField** | Pointer to **string** |  | [optional] 
**AllOf** | Pointer to [**[]SpecSchema**](SpecSchema.md) |  | [optional] 
**AnyOf** | Pointer to [**[]SpecSchema**](SpecSchema.md) |  | [optional] 
**Default** | Pointer to **map[string]interface{}** |  | [optional] 
**Definitions** | Pointer to [**map[string]SpecSchema**](SpecSchema.md) |  | [optional] 
**Dependencies** | Pointer to [**map[string]SpecSchemaOrStringArray**](SpecSchemaOrStringArray.md) |  | [optional] 
**Description** | Pointer to **string** |  | [optional] 
**Enum** | Pointer to **[]map[string]interface{}** |  | [optional] 
**ExclusiveMaximum** | Pointer to **bool** |  | [optional] 
**ExclusiveMinimum** | Pointer to **bool** |  | [optional] 
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
**Required** | Pointer to **[]string** |  | [optional] 
**Title** | Pointer to **string** |  | [optional] 
**Type** | Pointer to **string** |  | [optional] 
**UniqueItems** | Pointer to **bool** |  | [optional] 

## Methods

### NewSpecSchemaProps

`func NewSpecSchemaProps() *SpecSchemaProps`

NewSpecSchemaProps instantiates a new SpecSchemaProps object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSpecSchemaPropsWithDefaults

`func NewSpecSchemaPropsWithDefaults() *SpecSchemaProps`

NewSpecSchemaPropsWithDefaults instantiates a new SpecSchemaProps object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAdditionalItems

`func (o *SpecSchemaProps) GetAdditionalItems() string`

GetAdditionalItems returns the AdditionalItems field if non-nil, zero value otherwise.

### GetAdditionalItemsOk

`func (o *SpecSchemaProps) GetAdditionalItemsOk() (*string, bool)`

GetAdditionalItemsOk returns a tuple with the AdditionalItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalItems

`func (o *SpecSchemaProps) SetAdditionalItems(v string)`

SetAdditionalItems sets AdditionalItems field to given value.

### HasAdditionalItems

`func (o *SpecSchemaProps) HasAdditionalItems() bool`

HasAdditionalItems returns a boolean if a field has been set.

### GetAdditionalPropertiesField

`func (o *SpecSchemaProps) GetAdditionalPropertiesField() string`

GetAdditionalPropertiesField returns the AdditionalPropertiesField field if non-nil, zero value otherwise.

### GetAdditionalPropertiesFieldOk

`func (o *SpecSchemaProps) GetAdditionalPropertiesFieldOk() (*string, bool)`

GetAdditionalPropertiesFieldOk returns a tuple with the AdditionalPropertiesField field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalPropertiesField

`func (o *SpecSchemaProps) SetAdditionalPropertiesField(v string)`

SetAdditionalPropertiesField sets AdditionalPropertiesField field to given value.

### HasAdditionalPropertiesField

`func (o *SpecSchemaProps) HasAdditionalPropertiesField() bool`

HasAdditionalPropertiesField returns a boolean if a field has been set.

### GetAllOf

`func (o *SpecSchemaProps) GetAllOf() []SpecSchema`

GetAllOf returns the AllOf field if non-nil, zero value otherwise.

### GetAllOfOk

`func (o *SpecSchemaProps) GetAllOfOk() (*[]SpecSchema, bool)`

GetAllOfOk returns a tuple with the AllOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAllOf

`func (o *SpecSchemaProps) SetAllOf(v []SpecSchema)`

SetAllOf sets AllOf field to given value.

### HasAllOf

`func (o *SpecSchemaProps) HasAllOf() bool`

HasAllOf returns a boolean if a field has been set.

### GetAnyOf

`func (o *SpecSchemaProps) GetAnyOf() []SpecSchema`

GetAnyOf returns the AnyOf field if non-nil, zero value otherwise.

### GetAnyOfOk

`func (o *SpecSchemaProps) GetAnyOfOk() (*[]SpecSchema, bool)`

GetAnyOfOk returns a tuple with the AnyOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAnyOf

`func (o *SpecSchemaProps) SetAnyOf(v []SpecSchema)`

SetAnyOf sets AnyOf field to given value.

### HasAnyOf

`func (o *SpecSchemaProps) HasAnyOf() bool`

HasAnyOf returns a boolean if a field has been set.

### GetDefault

`func (o *SpecSchemaProps) GetDefault() map[string]interface{}`

GetDefault returns the Default field if non-nil, zero value otherwise.

### GetDefaultOk

`func (o *SpecSchemaProps) GetDefaultOk() (*map[string]interface{}, bool)`

GetDefaultOk returns a tuple with the Default field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefault

`func (o *SpecSchemaProps) SetDefault(v map[string]interface{})`

SetDefault sets Default field to given value.

### HasDefault

`func (o *SpecSchemaProps) HasDefault() bool`

HasDefault returns a boolean if a field has been set.

### GetDefinitions

`func (o *SpecSchemaProps) GetDefinitions() map[string]SpecSchema`

GetDefinitions returns the Definitions field if non-nil, zero value otherwise.

### GetDefinitionsOk

`func (o *SpecSchemaProps) GetDefinitionsOk() (*map[string]SpecSchema, bool)`

GetDefinitionsOk returns a tuple with the Definitions field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefinitions

`func (o *SpecSchemaProps) SetDefinitions(v map[string]SpecSchema)`

SetDefinitions sets Definitions field to given value.

### HasDefinitions

`func (o *SpecSchemaProps) HasDefinitions() bool`

HasDefinitions returns a boolean if a field has been set.

### GetDependencies

`func (o *SpecSchemaProps) GetDependencies() map[string]SpecSchemaOrStringArray`

GetDependencies returns the Dependencies field if non-nil, zero value otherwise.

### GetDependenciesOk

`func (o *SpecSchemaProps) GetDependenciesOk() (*map[string]SpecSchemaOrStringArray, bool)`

GetDependenciesOk returns a tuple with the Dependencies field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDependencies

`func (o *SpecSchemaProps) SetDependencies(v map[string]SpecSchemaOrStringArray)`

SetDependencies sets Dependencies field to given value.

### HasDependencies

`func (o *SpecSchemaProps) HasDependencies() bool`

HasDependencies returns a boolean if a field has been set.

### GetDescription

`func (o *SpecSchemaProps) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *SpecSchemaProps) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *SpecSchemaProps) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *SpecSchemaProps) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetEnum

`func (o *SpecSchemaProps) GetEnum() []map[string]interface{}`

GetEnum returns the Enum field if non-nil, zero value otherwise.

### GetEnumOk

`func (o *SpecSchemaProps) GetEnumOk() (*[]map[string]interface{}, bool)`

GetEnumOk returns a tuple with the Enum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEnum

`func (o *SpecSchemaProps) SetEnum(v []map[string]interface{})`

SetEnum sets Enum field to given value.

### HasEnum

`func (o *SpecSchemaProps) HasEnum() bool`

HasEnum returns a boolean if a field has been set.

### GetExclusiveMaximum

`func (o *SpecSchemaProps) GetExclusiveMaximum() bool`

GetExclusiveMaximum returns the ExclusiveMaximum field if non-nil, zero value otherwise.

### GetExclusiveMaximumOk

`func (o *SpecSchemaProps) GetExclusiveMaximumOk() (*bool, bool)`

GetExclusiveMaximumOk returns a tuple with the ExclusiveMaximum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExclusiveMaximum

`func (o *SpecSchemaProps) SetExclusiveMaximum(v bool)`

SetExclusiveMaximum sets ExclusiveMaximum field to given value.

### HasExclusiveMaximum

`func (o *SpecSchemaProps) HasExclusiveMaximum() bool`

HasExclusiveMaximum returns a boolean if a field has been set.

### GetExclusiveMinimum

`func (o *SpecSchemaProps) GetExclusiveMinimum() bool`

GetExclusiveMinimum returns the ExclusiveMinimum field if non-nil, zero value otherwise.

### GetExclusiveMinimumOk

`func (o *SpecSchemaProps) GetExclusiveMinimumOk() (*bool, bool)`

GetExclusiveMinimumOk returns a tuple with the ExclusiveMinimum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExclusiveMinimum

`func (o *SpecSchemaProps) SetExclusiveMinimum(v bool)`

SetExclusiveMinimum sets ExclusiveMinimum field to given value.

### HasExclusiveMinimum

`func (o *SpecSchemaProps) HasExclusiveMinimum() bool`

HasExclusiveMinimum returns a boolean if a field has been set.

### GetFormat

`func (o *SpecSchemaProps) GetFormat() string`

GetFormat returns the Format field if non-nil, zero value otherwise.

### GetFormatOk

`func (o *SpecSchemaProps) GetFormatOk() (*string, bool)`

GetFormatOk returns a tuple with the Format field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFormat

`func (o *SpecSchemaProps) SetFormat(v string)`

SetFormat sets Format field to given value.

### HasFormat

`func (o *SpecSchemaProps) HasFormat() bool`

HasFormat returns a boolean if a field has been set.

### GetId

`func (o *SpecSchemaProps) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *SpecSchemaProps) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *SpecSchemaProps) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *SpecSchemaProps) HasId() bool`

HasId returns a boolean if a field has been set.

### GetItems

`func (o *SpecSchemaProps) GetItems() string`

GetItems returns the Items field if non-nil, zero value otherwise.

### GetItemsOk

`func (o *SpecSchemaProps) GetItemsOk() (*string, bool)`

GetItemsOk returns a tuple with the Items field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetItems

`func (o *SpecSchemaProps) SetItems(v string)`

SetItems sets Items field to given value.

### HasItems

`func (o *SpecSchemaProps) HasItems() bool`

HasItems returns a boolean if a field has been set.

### GetMaxItems

`func (o *SpecSchemaProps) GetMaxItems() int64`

GetMaxItems returns the MaxItems field if non-nil, zero value otherwise.

### GetMaxItemsOk

`func (o *SpecSchemaProps) GetMaxItemsOk() (*int64, bool)`

GetMaxItemsOk returns a tuple with the MaxItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxItems

`func (o *SpecSchemaProps) SetMaxItems(v int64)`

SetMaxItems sets MaxItems field to given value.

### HasMaxItems

`func (o *SpecSchemaProps) HasMaxItems() bool`

HasMaxItems returns a boolean if a field has been set.

### GetMaxLength

`func (o *SpecSchemaProps) GetMaxLength() int64`

GetMaxLength returns the MaxLength field if non-nil, zero value otherwise.

### GetMaxLengthOk

`func (o *SpecSchemaProps) GetMaxLengthOk() (*int64, bool)`

GetMaxLengthOk returns a tuple with the MaxLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxLength

`func (o *SpecSchemaProps) SetMaxLength(v int64)`

SetMaxLength sets MaxLength field to given value.

### HasMaxLength

`func (o *SpecSchemaProps) HasMaxLength() bool`

HasMaxLength returns a boolean if a field has been set.

### GetMaxProperties

`func (o *SpecSchemaProps) GetMaxProperties() int64`

GetMaxProperties returns the MaxProperties field if non-nil, zero value otherwise.

### GetMaxPropertiesOk

`func (o *SpecSchemaProps) GetMaxPropertiesOk() (*int64, bool)`

GetMaxPropertiesOk returns a tuple with the MaxProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxProperties

`func (o *SpecSchemaProps) SetMaxProperties(v int64)`

SetMaxProperties sets MaxProperties field to given value.

### HasMaxProperties

`func (o *SpecSchemaProps) HasMaxProperties() bool`

HasMaxProperties returns a boolean if a field has been set.

### GetMaximum

`func (o *SpecSchemaProps) GetMaximum() float64`

GetMaximum returns the Maximum field if non-nil, zero value otherwise.

### GetMaximumOk

`func (o *SpecSchemaProps) GetMaximumOk() (*float64, bool)`

GetMaximumOk returns a tuple with the Maximum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaximum

`func (o *SpecSchemaProps) SetMaximum(v float64)`

SetMaximum sets Maximum field to given value.

### HasMaximum

`func (o *SpecSchemaProps) HasMaximum() bool`

HasMaximum returns a boolean if a field has been set.

### GetMinItems

`func (o *SpecSchemaProps) GetMinItems() int64`

GetMinItems returns the MinItems field if non-nil, zero value otherwise.

### GetMinItemsOk

`func (o *SpecSchemaProps) GetMinItemsOk() (*int64, bool)`

GetMinItemsOk returns a tuple with the MinItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinItems

`func (o *SpecSchemaProps) SetMinItems(v int64)`

SetMinItems sets MinItems field to given value.

### HasMinItems

`func (o *SpecSchemaProps) HasMinItems() bool`

HasMinItems returns a boolean if a field has been set.

### GetMinLength

`func (o *SpecSchemaProps) GetMinLength() int64`

GetMinLength returns the MinLength field if non-nil, zero value otherwise.

### GetMinLengthOk

`func (o *SpecSchemaProps) GetMinLengthOk() (*int64, bool)`

GetMinLengthOk returns a tuple with the MinLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinLength

`func (o *SpecSchemaProps) SetMinLength(v int64)`

SetMinLength sets MinLength field to given value.

### HasMinLength

`func (o *SpecSchemaProps) HasMinLength() bool`

HasMinLength returns a boolean if a field has been set.

### GetMinProperties

`func (o *SpecSchemaProps) GetMinProperties() int64`

GetMinProperties returns the MinProperties field if non-nil, zero value otherwise.

### GetMinPropertiesOk

`func (o *SpecSchemaProps) GetMinPropertiesOk() (*int64, bool)`

GetMinPropertiesOk returns a tuple with the MinProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinProperties

`func (o *SpecSchemaProps) SetMinProperties(v int64)`

SetMinProperties sets MinProperties field to given value.

### HasMinProperties

`func (o *SpecSchemaProps) HasMinProperties() bool`

HasMinProperties returns a boolean if a field has been set.

### GetMinimum

`func (o *SpecSchemaProps) GetMinimum() float64`

GetMinimum returns the Minimum field if non-nil, zero value otherwise.

### GetMinimumOk

`func (o *SpecSchemaProps) GetMinimumOk() (*float64, bool)`

GetMinimumOk returns a tuple with the Minimum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMinimum

`func (o *SpecSchemaProps) SetMinimum(v float64)`

SetMinimum sets Minimum field to given value.

### HasMinimum

`func (o *SpecSchemaProps) HasMinimum() bool`

HasMinimum returns a boolean if a field has been set.

### GetMultipleOf

`func (o *SpecSchemaProps) GetMultipleOf() float64`

GetMultipleOf returns the MultipleOf field if non-nil, zero value otherwise.

### GetMultipleOfOk

`func (o *SpecSchemaProps) GetMultipleOfOk() (*float64, bool)`

GetMultipleOfOk returns a tuple with the MultipleOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMultipleOf

`func (o *SpecSchemaProps) SetMultipleOf(v float64)`

SetMultipleOf sets MultipleOf field to given value.

### HasMultipleOf

`func (o *SpecSchemaProps) HasMultipleOf() bool`

HasMultipleOf returns a boolean if a field has been set.

### GetNot

`func (o *SpecSchemaProps) GetNot() string`

GetNot returns the Not field if non-nil, zero value otherwise.

### GetNotOk

`func (o *SpecSchemaProps) GetNotOk() (*string, bool)`

GetNotOk returns a tuple with the Not field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNot

`func (o *SpecSchemaProps) SetNot(v string)`

SetNot sets Not field to given value.

### HasNot

`func (o *SpecSchemaProps) HasNot() bool`

HasNot returns a boolean if a field has been set.

### GetNullable

`func (o *SpecSchemaProps) GetNullable() bool`

GetNullable returns the Nullable field if non-nil, zero value otherwise.

### GetNullableOk

`func (o *SpecSchemaProps) GetNullableOk() (*bool, bool)`

GetNullableOk returns a tuple with the Nullable field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNullable

`func (o *SpecSchemaProps) SetNullable(v bool)`

SetNullable sets Nullable field to given value.

### HasNullable

`func (o *SpecSchemaProps) HasNullable() bool`

HasNullable returns a boolean if a field has been set.

### GetOneOf

`func (o *SpecSchemaProps) GetOneOf() []SpecSchema`

GetOneOf returns the OneOf field if non-nil, zero value otherwise.

### GetOneOfOk

`func (o *SpecSchemaProps) GetOneOfOk() (*[]SpecSchema, bool)`

GetOneOfOk returns a tuple with the OneOf field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOneOf

`func (o *SpecSchemaProps) SetOneOf(v []SpecSchema)`

SetOneOf sets OneOf field to given value.

### HasOneOf

`func (o *SpecSchemaProps) HasOneOf() bool`

HasOneOf returns a boolean if a field has been set.

### GetPattern

`func (o *SpecSchemaProps) GetPattern() string`

GetPattern returns the Pattern field if non-nil, zero value otherwise.

### GetPatternOk

`func (o *SpecSchemaProps) GetPatternOk() (*string, bool)`

GetPatternOk returns a tuple with the Pattern field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPattern

`func (o *SpecSchemaProps) SetPattern(v string)`

SetPattern sets Pattern field to given value.

### HasPattern

`func (o *SpecSchemaProps) HasPattern() bool`

HasPattern returns a boolean if a field has been set.

### GetPatternProperties

`func (o *SpecSchemaProps) GetPatternProperties() string`

GetPatternProperties returns the PatternProperties field if non-nil, zero value otherwise.

### GetPatternPropertiesOk

`func (o *SpecSchemaProps) GetPatternPropertiesOk() (*string, bool)`

GetPatternPropertiesOk returns a tuple with the PatternProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPatternProperties

`func (o *SpecSchemaProps) SetPatternProperties(v string)`

SetPatternProperties sets PatternProperties field to given value.

### HasPatternProperties

`func (o *SpecSchemaProps) HasPatternProperties() bool`

HasPatternProperties returns a boolean if a field has been set.

### GetProperties

`func (o *SpecSchemaProps) GetProperties() string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *SpecSchemaProps) GetPropertiesOk() (*string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *SpecSchemaProps) SetProperties(v string)`

SetProperties sets Properties field to given value.

### HasProperties

`func (o *SpecSchemaProps) HasProperties() bool`

HasProperties returns a boolean if a field has been set.

### GetRequired

`func (o *SpecSchemaProps) GetRequired() []string`

GetRequired returns the Required field if non-nil, zero value otherwise.

### GetRequiredOk

`func (o *SpecSchemaProps) GetRequiredOk() (*[]string, bool)`

GetRequiredOk returns a tuple with the Required field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRequired

`func (o *SpecSchemaProps) SetRequired(v []string)`

SetRequired sets Required field to given value.

### HasRequired

`func (o *SpecSchemaProps) HasRequired() bool`

HasRequired returns a boolean if a field has been set.

### GetTitle

`func (o *SpecSchemaProps) GetTitle() string`

GetTitle returns the Title field if non-nil, zero value otherwise.

### GetTitleOk

`func (o *SpecSchemaProps) GetTitleOk() (*string, bool)`

GetTitleOk returns a tuple with the Title field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTitle

`func (o *SpecSchemaProps) SetTitle(v string)`

SetTitle sets Title field to given value.

### HasTitle

`func (o *SpecSchemaProps) HasTitle() bool`

HasTitle returns a boolean if a field has been set.

### GetType

`func (o *SpecSchemaProps) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *SpecSchemaProps) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *SpecSchemaProps) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *SpecSchemaProps) HasType() bool`

HasType returns a boolean if a field has been set.

### GetUniqueItems

`func (o *SpecSchemaProps) GetUniqueItems() bool`

GetUniqueItems returns the UniqueItems field if non-nil, zero value otherwise.

### GetUniqueItemsOk

`func (o *SpecSchemaProps) GetUniqueItemsOk() (*bool, bool)`

GetUniqueItemsOk returns a tuple with the UniqueItems field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUniqueItems

`func (o *SpecSchemaProps) SetUniqueItems(v bool)`

SetUniqueItems sets UniqueItems field to given value.

### HasUniqueItems

`func (o *SpecSchemaProps) HasUniqueItems() bool`

HasUniqueItems returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


