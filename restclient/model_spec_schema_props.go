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

// checks if the SpecSchemaProps type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &SpecSchemaProps{}

// SpecSchemaProps struct for SpecSchemaProps
type SpecSchemaProps struct {
	AdditionalItems           *string                             `json:"additionalItems,omitempty"`
	AdditionalPropertiesField *string                             `json:"additionalProperties,omitempty"`
	AllOf                     []SpecSchema                        `json:"allOf,omitempty"`
	AnyOf                     []SpecSchema                        `json:"anyOf,omitempty"`
	Default                   map[string]interface{}              `json:"default,omitempty"`
	Definitions               *map[string]SpecSchema              `json:"definitions,omitempty"`
	Dependencies              *map[string]SpecSchemaOrStringArray `json:"dependencies,omitempty"`
	Description               *string                             `json:"description,omitempty"`
	Enum                      []map[string]interface{}            `json:"enum,omitempty"`
	ExclusiveMaximum          *bool                               `json:"exclusiveMaximum,omitempty"`
	ExclusiveMinimum          *bool                               `json:"exclusiveMinimum,omitempty"`
	Format                    *string                             `json:"format,omitempty"`
	Id                        *string                             `json:"id,omitempty"`
	Items                     *string                             `json:"items,omitempty"`
	MaxItems                  *int64                              `json:"maxItems,omitempty"`
	MaxLength                 *int64                              `json:"maxLength,omitempty"`
	MaxProperties             *int64                              `json:"maxProperties,omitempty"`
	Maximum                   *float64                            `json:"maximum,omitempty"`
	MinItems                  *int64                              `json:"minItems,omitempty"`
	MinLength                 *int64                              `json:"minLength,omitempty"`
	MinProperties             *int64                              `json:"minProperties,omitempty"`
	Minimum                   *float64                            `json:"minimum,omitempty"`
	MultipleOf                *float64                            `json:"multipleOf,omitempty"`
	Not                       *string                             `json:"not,omitempty"`
	Nullable                  *bool                               `json:"nullable,omitempty"`
	OneOf                     []SpecSchema                        `json:"oneOf,omitempty"`
	Pattern                   *string                             `json:"pattern,omitempty"`
	PatternProperties         *string                             `json:"patternProperties,omitempty"`
	Properties                *string                             `json:"properties,omitempty"`
	Required                  []string                            `json:"required,omitempty"`
	Title                     *string                             `json:"title,omitempty"`
	Type                      *string                             `json:"type,omitempty"`
	UniqueItems               *bool                               `json:"uniqueItems,omitempty"`
}

// NewSpecSchemaProps instantiates a new SpecSchemaProps object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewSpecSchemaProps() *SpecSchemaProps {
	this := SpecSchemaProps{}
	return &this
}

// NewSpecSchemaPropsWithDefaults instantiates a new SpecSchemaProps object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewSpecSchemaPropsWithDefaults() *SpecSchemaProps {
	this := SpecSchemaProps{}
	return &this
}

// GetAdditionalItems returns the AdditionalItems field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetAdditionalItems() string {
	if o == nil || IsNil(o.AdditionalItems) {
		var ret string
		return ret
	}
	return *o.AdditionalItems
}

// GetAdditionalItemsOk returns a tuple with the AdditionalItems field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetAdditionalItemsOk() (*string, bool) {
	if o == nil || IsNil(o.AdditionalItems) {
		return nil, false
	}
	return o.AdditionalItems, true
}

// HasAdditionalItems returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasAdditionalItems() bool {
	if o != nil && !IsNil(o.AdditionalItems) {
		return true
	}

	return false
}

// SetAdditionalItems gets a reference to the given string and assigns it to the AdditionalItems field.
func (o *SpecSchemaProps) SetAdditionalItems(v string) {
	o.AdditionalItems = &v
}

// GetAdditionalPropertiesField returns the AdditionalPropertiesField field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetAdditionalPropertiesField() string {
	if o == nil || IsNil(o.AdditionalPropertiesField) {
		var ret string
		return ret
	}
	return *o.AdditionalPropertiesField
}

// GetAdditionalPropertiesFieldOk returns a tuple with the AdditionalPropertiesField field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetAdditionalPropertiesFieldOk() (*string, bool) {
	if o == nil || IsNil(o.AdditionalPropertiesField) {
		return nil, false
	}
	return o.AdditionalPropertiesField, true
}

// HasAdditionalPropertiesField returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasAdditionalPropertiesField() bool {
	if o != nil && !IsNil(o.AdditionalPropertiesField) {
		return true
	}

	return false
}

// SetAdditionalPropertiesField gets a reference to the given string and assigns it to the AdditionalPropertiesField field.
func (o *SpecSchemaProps) SetAdditionalPropertiesField(v string) {
	o.AdditionalPropertiesField = &v
}

// GetAllOf returns the AllOf field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetAllOf() []SpecSchema {
	if o == nil || IsNil(o.AllOf) {
		var ret []SpecSchema
		return ret
	}
	return o.AllOf
}

// GetAllOfOk returns a tuple with the AllOf field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetAllOfOk() ([]SpecSchema, bool) {
	if o == nil || IsNil(o.AllOf) {
		return nil, false
	}
	return o.AllOf, true
}

// HasAllOf returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasAllOf() bool {
	if o != nil && !IsNil(o.AllOf) {
		return true
	}

	return false
}

// SetAllOf gets a reference to the given []SpecSchema and assigns it to the AllOf field.
func (o *SpecSchemaProps) SetAllOf(v []SpecSchema) {
	o.AllOf = v
}

// GetAnyOf returns the AnyOf field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetAnyOf() []SpecSchema {
	if o == nil || IsNil(o.AnyOf) {
		var ret []SpecSchema
		return ret
	}
	return o.AnyOf
}

// GetAnyOfOk returns a tuple with the AnyOf field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetAnyOfOk() ([]SpecSchema, bool) {
	if o == nil || IsNil(o.AnyOf) {
		return nil, false
	}
	return o.AnyOf, true
}

// HasAnyOf returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasAnyOf() bool {
	if o != nil && !IsNil(o.AnyOf) {
		return true
	}

	return false
}

// SetAnyOf gets a reference to the given []SpecSchema and assigns it to the AnyOf field.
func (o *SpecSchemaProps) SetAnyOf(v []SpecSchema) {
	o.AnyOf = v
}

// GetDefault returns the Default field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetDefault() map[string]interface{} {
	if o == nil || IsNil(o.Default) {
		var ret map[string]interface{}
		return ret
	}
	return o.Default
}

// GetDefaultOk returns a tuple with the Default field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetDefaultOk() (map[string]interface{}, bool) {
	if o == nil || IsNil(o.Default) {
		return map[string]interface{}{}, false
	}
	return o.Default, true
}

// HasDefault returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasDefault() bool {
	if o != nil && !IsNil(o.Default) {
		return true
	}

	return false
}

// SetDefault gets a reference to the given map[string]interface{} and assigns it to the Default field.
func (o *SpecSchemaProps) SetDefault(v map[string]interface{}) {
	o.Default = v
}

// GetDefinitions returns the Definitions field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetDefinitions() map[string]SpecSchema {
	if o == nil || IsNil(o.Definitions) {
		var ret map[string]SpecSchema
		return ret
	}
	return *o.Definitions
}

// GetDefinitionsOk returns a tuple with the Definitions field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetDefinitionsOk() (*map[string]SpecSchema, bool) {
	if o == nil || IsNil(o.Definitions) {
		return nil, false
	}
	return o.Definitions, true
}

// HasDefinitions returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasDefinitions() bool {
	if o != nil && !IsNil(o.Definitions) {
		return true
	}

	return false
}

// SetDefinitions gets a reference to the given map[string]SpecSchema and assigns it to the Definitions field.
func (o *SpecSchemaProps) SetDefinitions(v map[string]SpecSchema) {
	o.Definitions = &v
}

// GetDependencies returns the Dependencies field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetDependencies() map[string]SpecSchemaOrStringArray {
	if o == nil || IsNil(o.Dependencies) {
		var ret map[string]SpecSchemaOrStringArray
		return ret
	}
	return *o.Dependencies
}

// GetDependenciesOk returns a tuple with the Dependencies field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetDependenciesOk() (*map[string]SpecSchemaOrStringArray, bool) {
	if o == nil || IsNil(o.Dependencies) {
		return nil, false
	}
	return o.Dependencies, true
}

// HasDependencies returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasDependencies() bool {
	if o != nil && !IsNil(o.Dependencies) {
		return true
	}

	return false
}

// SetDependencies gets a reference to the given map[string]SpecSchemaOrStringArray and assigns it to the Dependencies field.
func (o *SpecSchemaProps) SetDependencies(v map[string]SpecSchemaOrStringArray) {
	o.Dependencies = &v
}

// GetDescription returns the Description field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetDescription() string {
	if o == nil || IsNil(o.Description) {
		var ret string
		return ret
	}
	return *o.Description
}

// GetDescriptionOk returns a tuple with the Description field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetDescriptionOk() (*string, bool) {
	if o == nil || IsNil(o.Description) {
		return nil, false
	}
	return o.Description, true
}

// HasDescription returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasDescription() bool {
	if o != nil && !IsNil(o.Description) {
		return true
	}

	return false
}

// SetDescription gets a reference to the given string and assigns it to the Description field.
func (o *SpecSchemaProps) SetDescription(v string) {
	o.Description = &v
}

// GetEnum returns the Enum field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetEnum() []map[string]interface{} {
	if o == nil || IsNil(o.Enum) {
		var ret []map[string]interface{}
		return ret
	}
	return o.Enum
}

// GetEnumOk returns a tuple with the Enum field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetEnumOk() ([]map[string]interface{}, bool) {
	if o == nil || IsNil(o.Enum) {
		return nil, false
	}
	return o.Enum, true
}

// HasEnum returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasEnum() bool {
	if o != nil && !IsNil(o.Enum) {
		return true
	}

	return false
}

// SetEnum gets a reference to the given []map[string]interface{} and assigns it to the Enum field.
func (o *SpecSchemaProps) SetEnum(v []map[string]interface{}) {
	o.Enum = v
}

// GetExclusiveMaximum returns the ExclusiveMaximum field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetExclusiveMaximum() bool {
	if o == nil || IsNil(o.ExclusiveMaximum) {
		var ret bool
		return ret
	}
	return *o.ExclusiveMaximum
}

// GetExclusiveMaximumOk returns a tuple with the ExclusiveMaximum field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetExclusiveMaximumOk() (*bool, bool) {
	if o == nil || IsNil(o.ExclusiveMaximum) {
		return nil, false
	}
	return o.ExclusiveMaximum, true
}

// HasExclusiveMaximum returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasExclusiveMaximum() bool {
	if o != nil && !IsNil(o.ExclusiveMaximum) {
		return true
	}

	return false
}

// SetExclusiveMaximum gets a reference to the given bool and assigns it to the ExclusiveMaximum field.
func (o *SpecSchemaProps) SetExclusiveMaximum(v bool) {
	o.ExclusiveMaximum = &v
}

// GetExclusiveMinimum returns the ExclusiveMinimum field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetExclusiveMinimum() bool {
	if o == nil || IsNil(o.ExclusiveMinimum) {
		var ret bool
		return ret
	}
	return *o.ExclusiveMinimum
}

// GetExclusiveMinimumOk returns a tuple with the ExclusiveMinimum field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetExclusiveMinimumOk() (*bool, bool) {
	if o == nil || IsNil(o.ExclusiveMinimum) {
		return nil, false
	}
	return o.ExclusiveMinimum, true
}

// HasExclusiveMinimum returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasExclusiveMinimum() bool {
	if o != nil && !IsNil(o.ExclusiveMinimum) {
		return true
	}

	return false
}

// SetExclusiveMinimum gets a reference to the given bool and assigns it to the ExclusiveMinimum field.
func (o *SpecSchemaProps) SetExclusiveMinimum(v bool) {
	o.ExclusiveMinimum = &v
}

// GetFormat returns the Format field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetFormat() string {
	if o == nil || IsNil(o.Format) {
		var ret string
		return ret
	}
	return *o.Format
}

// GetFormatOk returns a tuple with the Format field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetFormatOk() (*string, bool) {
	if o == nil || IsNil(o.Format) {
		return nil, false
	}
	return o.Format, true
}

// HasFormat returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasFormat() bool {
	if o != nil && !IsNil(o.Format) {
		return true
	}

	return false
}

// SetFormat gets a reference to the given string and assigns it to the Format field.
func (o *SpecSchemaProps) SetFormat(v string) {
	o.Format = &v
}

// GetId returns the Id field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetId() string {
	if o == nil || IsNil(o.Id) {
		var ret string
		return ret
	}
	return *o.Id
}

// GetIdOk returns a tuple with the Id field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetIdOk() (*string, bool) {
	if o == nil || IsNil(o.Id) {
		return nil, false
	}
	return o.Id, true
}

// HasId returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasId() bool {
	if o != nil && !IsNil(o.Id) {
		return true
	}

	return false
}

// SetId gets a reference to the given string and assigns it to the Id field.
func (o *SpecSchemaProps) SetId(v string) {
	o.Id = &v
}

// GetItems returns the Items field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetItems() string {
	if o == nil || IsNil(o.Items) {
		var ret string
		return ret
	}
	return *o.Items
}

// GetItemsOk returns a tuple with the Items field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetItemsOk() (*string, bool) {
	if o == nil || IsNil(o.Items) {
		return nil, false
	}
	return o.Items, true
}

// HasItems returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasItems() bool {
	if o != nil && !IsNil(o.Items) {
		return true
	}

	return false
}

// SetItems gets a reference to the given string and assigns it to the Items field.
func (o *SpecSchemaProps) SetItems(v string) {
	o.Items = &v
}

// GetMaxItems returns the MaxItems field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMaxItems() int64 {
	if o == nil || IsNil(o.MaxItems) {
		var ret int64
		return ret
	}
	return *o.MaxItems
}

// GetMaxItemsOk returns a tuple with the MaxItems field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMaxItemsOk() (*int64, bool) {
	if o == nil || IsNil(o.MaxItems) {
		return nil, false
	}
	return o.MaxItems, true
}

// HasMaxItems returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMaxItems() bool {
	if o != nil && !IsNil(o.MaxItems) {
		return true
	}

	return false
}

// SetMaxItems gets a reference to the given int64 and assigns it to the MaxItems field.
func (o *SpecSchemaProps) SetMaxItems(v int64) {
	o.MaxItems = &v
}

// GetMaxLength returns the MaxLength field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMaxLength() int64 {
	if o == nil || IsNil(o.MaxLength) {
		var ret int64
		return ret
	}
	return *o.MaxLength
}

// GetMaxLengthOk returns a tuple with the MaxLength field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMaxLengthOk() (*int64, bool) {
	if o == nil || IsNil(o.MaxLength) {
		return nil, false
	}
	return o.MaxLength, true
}

// HasMaxLength returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMaxLength() bool {
	if o != nil && !IsNil(o.MaxLength) {
		return true
	}

	return false
}

// SetMaxLength gets a reference to the given int64 and assigns it to the MaxLength field.
func (o *SpecSchemaProps) SetMaxLength(v int64) {
	o.MaxLength = &v
}

// GetMaxProperties returns the MaxProperties field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMaxProperties() int64 {
	if o == nil || IsNil(o.MaxProperties) {
		var ret int64
		return ret
	}
	return *o.MaxProperties
}

// GetMaxPropertiesOk returns a tuple with the MaxProperties field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMaxPropertiesOk() (*int64, bool) {
	if o == nil || IsNil(o.MaxProperties) {
		return nil, false
	}
	return o.MaxProperties, true
}

// HasMaxProperties returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMaxProperties() bool {
	if o != nil && !IsNil(o.MaxProperties) {
		return true
	}

	return false
}

// SetMaxProperties gets a reference to the given int64 and assigns it to the MaxProperties field.
func (o *SpecSchemaProps) SetMaxProperties(v int64) {
	o.MaxProperties = &v
}

// GetMaximum returns the Maximum field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMaximum() float64 {
	if o == nil || IsNil(o.Maximum) {
		var ret float64
		return ret
	}
	return *o.Maximum
}

// GetMaximumOk returns a tuple with the Maximum field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMaximumOk() (*float64, bool) {
	if o == nil || IsNil(o.Maximum) {
		return nil, false
	}
	return o.Maximum, true
}

// HasMaximum returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMaximum() bool {
	if o != nil && !IsNil(o.Maximum) {
		return true
	}

	return false
}

// SetMaximum gets a reference to the given float64 and assigns it to the Maximum field.
func (o *SpecSchemaProps) SetMaximum(v float64) {
	o.Maximum = &v
}

// GetMinItems returns the MinItems field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMinItems() int64 {
	if o == nil || IsNil(o.MinItems) {
		var ret int64
		return ret
	}
	return *o.MinItems
}

// GetMinItemsOk returns a tuple with the MinItems field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMinItemsOk() (*int64, bool) {
	if o == nil || IsNil(o.MinItems) {
		return nil, false
	}
	return o.MinItems, true
}

// HasMinItems returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMinItems() bool {
	if o != nil && !IsNil(o.MinItems) {
		return true
	}

	return false
}

// SetMinItems gets a reference to the given int64 and assigns it to the MinItems field.
func (o *SpecSchemaProps) SetMinItems(v int64) {
	o.MinItems = &v
}

// GetMinLength returns the MinLength field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMinLength() int64 {
	if o == nil || IsNil(o.MinLength) {
		var ret int64
		return ret
	}
	return *o.MinLength
}

// GetMinLengthOk returns a tuple with the MinLength field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMinLengthOk() (*int64, bool) {
	if o == nil || IsNil(o.MinLength) {
		return nil, false
	}
	return o.MinLength, true
}

// HasMinLength returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMinLength() bool {
	if o != nil && !IsNil(o.MinLength) {
		return true
	}

	return false
}

// SetMinLength gets a reference to the given int64 and assigns it to the MinLength field.
func (o *SpecSchemaProps) SetMinLength(v int64) {
	o.MinLength = &v
}

// GetMinProperties returns the MinProperties field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMinProperties() int64 {
	if o == nil || IsNil(o.MinProperties) {
		var ret int64
		return ret
	}
	return *o.MinProperties
}

// GetMinPropertiesOk returns a tuple with the MinProperties field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMinPropertiesOk() (*int64, bool) {
	if o == nil || IsNil(o.MinProperties) {
		return nil, false
	}
	return o.MinProperties, true
}

// HasMinProperties returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMinProperties() bool {
	if o != nil && !IsNil(o.MinProperties) {
		return true
	}

	return false
}

// SetMinProperties gets a reference to the given int64 and assigns it to the MinProperties field.
func (o *SpecSchemaProps) SetMinProperties(v int64) {
	o.MinProperties = &v
}

// GetMinimum returns the Minimum field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMinimum() float64 {
	if o == nil || IsNil(o.Minimum) {
		var ret float64
		return ret
	}
	return *o.Minimum
}

// GetMinimumOk returns a tuple with the Minimum field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMinimumOk() (*float64, bool) {
	if o == nil || IsNil(o.Minimum) {
		return nil, false
	}
	return o.Minimum, true
}

// HasMinimum returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMinimum() bool {
	if o != nil && !IsNil(o.Minimum) {
		return true
	}

	return false
}

// SetMinimum gets a reference to the given float64 and assigns it to the Minimum field.
func (o *SpecSchemaProps) SetMinimum(v float64) {
	o.Minimum = &v
}

// GetMultipleOf returns the MultipleOf field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetMultipleOf() float64 {
	if o == nil || IsNil(o.MultipleOf) {
		var ret float64
		return ret
	}
	return *o.MultipleOf
}

// GetMultipleOfOk returns a tuple with the MultipleOf field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetMultipleOfOk() (*float64, bool) {
	if o == nil || IsNil(o.MultipleOf) {
		return nil, false
	}
	return o.MultipleOf, true
}

// HasMultipleOf returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasMultipleOf() bool {
	if o != nil && !IsNil(o.MultipleOf) {
		return true
	}

	return false
}

// SetMultipleOf gets a reference to the given float64 and assigns it to the MultipleOf field.
func (o *SpecSchemaProps) SetMultipleOf(v float64) {
	o.MultipleOf = &v
}

// GetNot returns the Not field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetNot() string {
	if o == nil || IsNil(o.Not) {
		var ret string
		return ret
	}
	return *o.Not
}

// GetNotOk returns a tuple with the Not field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetNotOk() (*string, bool) {
	if o == nil || IsNil(o.Not) {
		return nil, false
	}
	return o.Not, true
}

// HasNot returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasNot() bool {
	if o != nil && !IsNil(o.Not) {
		return true
	}

	return false
}

// SetNot gets a reference to the given string and assigns it to the Not field.
func (o *SpecSchemaProps) SetNot(v string) {
	o.Not = &v
}

// GetNullable returns the Nullable field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetNullable() bool {
	if o == nil || IsNil(o.Nullable) {
		var ret bool
		return ret
	}
	return *o.Nullable
}

// GetNullableOk returns a tuple with the Nullable field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetNullableOk() (*bool, bool) {
	if o == nil || IsNil(o.Nullable) {
		return nil, false
	}
	return o.Nullable, true
}

// HasNullable returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasNullable() bool {
	if o != nil && !IsNil(o.Nullable) {
		return true
	}

	return false
}

// SetNullable gets a reference to the given bool and assigns it to the Nullable field.
func (o *SpecSchemaProps) SetNullable(v bool) {
	o.Nullable = &v
}

// GetOneOf returns the OneOf field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetOneOf() []SpecSchema {
	if o == nil || IsNil(o.OneOf) {
		var ret []SpecSchema
		return ret
	}
	return o.OneOf
}

// GetOneOfOk returns a tuple with the OneOf field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetOneOfOk() ([]SpecSchema, bool) {
	if o == nil || IsNil(o.OneOf) {
		return nil, false
	}
	return o.OneOf, true
}

// HasOneOf returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasOneOf() bool {
	if o != nil && !IsNil(o.OneOf) {
		return true
	}

	return false
}

// SetOneOf gets a reference to the given []SpecSchema and assigns it to the OneOf field.
func (o *SpecSchemaProps) SetOneOf(v []SpecSchema) {
	o.OneOf = v
}

// GetPattern returns the Pattern field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetPattern() string {
	if o == nil || IsNil(o.Pattern) {
		var ret string
		return ret
	}
	return *o.Pattern
}

// GetPatternOk returns a tuple with the Pattern field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetPatternOk() (*string, bool) {
	if o == nil || IsNil(o.Pattern) {
		return nil, false
	}
	return o.Pattern, true
}

// HasPattern returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasPattern() bool {
	if o != nil && !IsNil(o.Pattern) {
		return true
	}

	return false
}

// SetPattern gets a reference to the given string and assigns it to the Pattern field.
func (o *SpecSchemaProps) SetPattern(v string) {
	o.Pattern = &v
}

// GetPatternProperties returns the PatternProperties field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetPatternProperties() string {
	if o == nil || IsNil(o.PatternProperties) {
		var ret string
		return ret
	}
	return *o.PatternProperties
}

// GetPatternPropertiesOk returns a tuple with the PatternProperties field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetPatternPropertiesOk() (*string, bool) {
	if o == nil || IsNil(o.PatternProperties) {
		return nil, false
	}
	return o.PatternProperties, true
}

// HasPatternProperties returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasPatternProperties() bool {
	if o != nil && !IsNil(o.PatternProperties) {
		return true
	}

	return false
}

// SetPatternProperties gets a reference to the given string and assigns it to the PatternProperties field.
func (o *SpecSchemaProps) SetPatternProperties(v string) {
	o.PatternProperties = &v
}

// GetProperties returns the Properties field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetProperties() string {
	if o == nil || IsNil(o.Properties) {
		var ret string
		return ret
	}
	return *o.Properties
}

// GetPropertiesOk returns a tuple with the Properties field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetPropertiesOk() (*string, bool) {
	if o == nil || IsNil(o.Properties) {
		return nil, false
	}
	return o.Properties, true
}

// HasProperties returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasProperties() bool {
	if o != nil && !IsNil(o.Properties) {
		return true
	}

	return false
}

// SetProperties gets a reference to the given string and assigns it to the Properties field.
func (o *SpecSchemaProps) SetProperties(v string) {
	o.Properties = &v
}

// GetRequired returns the Required field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetRequired() []string {
	if o == nil || IsNil(o.Required) {
		var ret []string
		return ret
	}
	return o.Required
}

// GetRequiredOk returns a tuple with the Required field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetRequiredOk() ([]string, bool) {
	if o == nil || IsNil(o.Required) {
		return nil, false
	}
	return o.Required, true
}

// HasRequired returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasRequired() bool {
	if o != nil && !IsNil(o.Required) {
		return true
	}

	return false
}

// SetRequired gets a reference to the given []string and assigns it to the Required field.
func (o *SpecSchemaProps) SetRequired(v []string) {
	o.Required = v
}

// GetTitle returns the Title field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetTitle() string {
	if o == nil || IsNil(o.Title) {
		var ret string
		return ret
	}
	return *o.Title
}

// GetTitleOk returns a tuple with the Title field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetTitleOk() (*string, bool) {
	if o == nil || IsNil(o.Title) {
		return nil, false
	}
	return o.Title, true
}

// HasTitle returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasTitle() bool {
	if o != nil && !IsNil(o.Title) {
		return true
	}

	return false
}

// SetTitle gets a reference to the given string and assigns it to the Title field.
func (o *SpecSchemaProps) SetTitle(v string) {
	o.Title = &v
}

// GetType returns the Type field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetType() string {
	if o == nil || IsNil(o.Type) {
		var ret string
		return ret
	}
	return *o.Type
}

// GetTypeOk returns a tuple with the Type field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetTypeOk() (*string, bool) {
	if o == nil || IsNil(o.Type) {
		return nil, false
	}
	return o.Type, true
}

// HasType returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasType() bool {
	if o != nil && !IsNil(o.Type) {
		return true
	}

	return false
}

// SetType gets a reference to the given string and assigns it to the Type field.
func (o *SpecSchemaProps) SetType(v string) {
	o.Type = &v
}

// GetUniqueItems returns the UniqueItems field value if set, zero value otherwise.
func (o *SpecSchemaProps) GetUniqueItems() bool {
	if o == nil || IsNil(o.UniqueItems) {
		var ret bool
		return ret
	}
	return *o.UniqueItems
}

// GetUniqueItemsOk returns a tuple with the UniqueItems field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *SpecSchemaProps) GetUniqueItemsOk() (*bool, bool) {
	if o == nil || IsNil(o.UniqueItems) {
		return nil, false
	}
	return o.UniqueItems, true
}

// HasUniqueItems returns a boolean if a field has been set.
func (o *SpecSchemaProps) HasUniqueItems() bool {
	if o != nil && !IsNil(o.UniqueItems) {
		return true
	}

	return false
}

// SetUniqueItems gets a reference to the given bool and assigns it to the UniqueItems field.
func (o *SpecSchemaProps) SetUniqueItems(v bool) {
	o.UniqueItems = &v
}

func (o SpecSchemaProps) MarshalJSON() ([]byte, error) {
	toSerialize, err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o SpecSchemaProps) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.AdditionalItems) {
		toSerialize["additionalItems"] = o.AdditionalItems
	}
	if !IsNil(o.AdditionalPropertiesField) {
		toSerialize["additionalProperties"] = o.AdditionalPropertiesField
	}
	if !IsNil(o.AllOf) {
		toSerialize["allOf"] = o.AllOf
	}
	if !IsNil(o.AnyOf) {
		toSerialize["anyOf"] = o.AnyOf
	}
	if !IsNil(o.Default) {
		toSerialize["default"] = o.Default
	}
	if !IsNil(o.Definitions) {
		toSerialize["definitions"] = o.Definitions
	}
	if !IsNil(o.Dependencies) {
		toSerialize["dependencies"] = o.Dependencies
	}
	if !IsNil(o.Description) {
		toSerialize["description"] = o.Description
	}
	if !IsNil(o.Enum) {
		toSerialize["enum"] = o.Enum
	}
	if !IsNil(o.ExclusiveMaximum) {
		toSerialize["exclusiveMaximum"] = o.ExclusiveMaximum
	}
	if !IsNil(o.ExclusiveMinimum) {
		toSerialize["exclusiveMinimum"] = o.ExclusiveMinimum
	}
	if !IsNil(o.Format) {
		toSerialize["format"] = o.Format
	}
	if !IsNil(o.Id) {
		toSerialize["id"] = o.Id
	}
	if !IsNil(o.Items) {
		toSerialize["items"] = o.Items
	}
	if !IsNil(o.MaxItems) {
		toSerialize["maxItems"] = o.MaxItems
	}
	if !IsNil(o.MaxLength) {
		toSerialize["maxLength"] = o.MaxLength
	}
	if !IsNil(o.MaxProperties) {
		toSerialize["maxProperties"] = o.MaxProperties
	}
	if !IsNil(o.Maximum) {
		toSerialize["maximum"] = o.Maximum
	}
	if !IsNil(o.MinItems) {
		toSerialize["minItems"] = o.MinItems
	}
	if !IsNil(o.MinLength) {
		toSerialize["minLength"] = o.MinLength
	}
	if !IsNil(o.MinProperties) {
		toSerialize["minProperties"] = o.MinProperties
	}
	if !IsNil(o.Minimum) {
		toSerialize["minimum"] = o.Minimum
	}
	if !IsNil(o.MultipleOf) {
		toSerialize["multipleOf"] = o.MultipleOf
	}
	if !IsNil(o.Not) {
		toSerialize["not"] = o.Not
	}
	if !IsNil(o.Nullable) {
		toSerialize["nullable"] = o.Nullable
	}
	if !IsNil(o.OneOf) {
		toSerialize["oneOf"] = o.OneOf
	}
	if !IsNil(o.Pattern) {
		toSerialize["pattern"] = o.Pattern
	}
	if !IsNil(o.PatternProperties) {
		toSerialize["patternProperties"] = o.PatternProperties
	}
	if !IsNil(o.Properties) {
		toSerialize["properties"] = o.Properties
	}
	if !IsNil(o.Required) {
		toSerialize["required"] = o.Required
	}
	if !IsNil(o.Title) {
		toSerialize["title"] = o.Title
	}
	if !IsNil(o.Type) {
		toSerialize["type"] = o.Type
	}
	if !IsNil(o.UniqueItems) {
		toSerialize["uniqueItems"] = o.UniqueItems
	}
	return toSerialize, nil
}

type NullableSpecSchemaProps struct {
	value *SpecSchemaProps
	isSet bool
}

func (v NullableSpecSchemaProps) Get() *SpecSchemaProps {
	return v.value
}

func (v *NullableSpecSchemaProps) Set(val *SpecSchemaProps) {
	v.value = val
	v.isSet = true
}

func (v NullableSpecSchemaProps) IsSet() bool {
	return v.isSet
}

func (v *NullableSpecSchemaProps) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableSpecSchemaProps(val *SpecSchemaProps) *NullableSpecSchemaProps {
	return &NullableSpecSchemaProps{value: val, isSet: true}
}

func (v NullableSpecSchemaProps) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableSpecSchemaProps) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
