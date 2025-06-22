/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConfigItem defines a configuration item for a module
type ConfigItem struct {
	// DisplayName is the human-readable name of the config item
	// +kubebuilder:validation:Optional
	DisplayName string `json:"displayName,omitempty"`
	// Description provides additional information about the config item
	// +kubebuilder:validation:Optional
	Description string `json:"description,omitempty"`
	// Type specifies the data type of the config item
	// +kubebuilder:validation:Optional
	Type string `json:"type,omitempty"`
	// Required indicates whether this config item is mandatory
	// +kubebuilder:validation:Optional
	Required bool `json:"required,omitempty"`
}

// Module defines a module within a package
type Module struct {
	// DisplayName is the human-readable name of the module
	// +kubebuilder:validation:Optional
	DisplayName string `json:"displayName,omitempty"`
	// Description provides additional information about the module
	// +kubebuilder:validation:Optional
	Description string `json:"description,omitempty"`
	// SourceSchema defines the input schema for the module
	// +kubebuilder:validation:Optional
	SourceSchema string `json:"sourceSchema,omitempty"`
	// SinkSchema defines the output schema for the module
	// +kubebuilder:validation:Optional
	SinkSchema string `json:"sinkSchema,omitempty"`
	// Config is a list of configuration items for the module
	// +kubebuilder:validation:Optional
	Config map[string]ConfigItem `json:"config,omitempty"`
}

// CloudType defines cloud function package configuration
type CloudType struct {
	// Image specifies the container image for cloud deployment
	Image string `json:"image"`
}

// FunctionType defines the function type configuration
type FunctionType struct {
	// Cloud contains cloud function package configuration
	// +kubebuilder:validation:Optional
	Cloud *CloudType `json:"cloud,omitempty"`
}

// PackageSpec defines the desired state of Package
type PackageSpec struct {
	// DisplayName is the human-readable name of the package
	// +kubebuilder:validation:Optional
	DisplayName string `json:"displayName,omitempty"`
	// Logo is the URL or base64 encoded image for the package logo
	// +kubebuilder:validation:Optional
	Logo string `json:"logo,omitempty"`
	// Description provides additional information about the package
	// +kubebuilder:validation:Optional
	Description string `json:"description,omitempty"`
	// FunctionType contains function type configuration
	FunctionType FunctionType `json:"functionType"`
	// Modules is a map of module names to their configurations
	Modules map[string]Module `json:"modules"`
}

// PackageStatus defines the observed state of Package.
type PackageStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=packages,scope=Namespaced,singular=package,shortName=pkg

// Package is the Schema for the packages API.
type Package struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PackageSpec   `json:"spec,omitempty"`
	Status PackageStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PackageList contains a list of Package.
type PackageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Package `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Package{}, &PackageList{})
}
