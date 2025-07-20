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
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PackageRef defines a reference to a Package resource
// +kubebuilder:object:generate=true
// +kubebuilder:validation:Optional
type PackageRef struct {
	// Name of the Package resource
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// Namespace of the Package resource
	// +kubebuilder:validation:Optional
	Namespace string `json:"namespace,omitempty"`
}

// FunctionSpec defines the desired state of Function
// +kubebuilder:object:generate=true
// +kubebuilder:validation:Optional
type FunctionSpec struct {
	// Display name of the function
	// +kubebuilder:validation:Optional
	DisplayName string `json:"displayName,omitempty"`
	// Description of the function
	// +kubebuilder:validation:Optional
	Description string `json:"description,omitempty"`
	// Package reference
	// +kubebuilder:validation:Required
	PackageRef PackageRef `json:"packageRef"`
	// Module name
	// +kubebuilder:validation:Required
	Module string `json:"module"`
	// +kubebuilder:validation:Optional
	SubscriptionName string `json:"subscriptionName,omitempty"`
	// List of sources
	// +kubebuilder:validation:Optional
	Sources []SourceSpec `json:"sources,omitempty"`
	// Request source
	// +kubebuilder:validation:Optional
	RequestSource *SourceSpec `json:"requestSource,omitempty"`
	// Sink specifies the sink configuration
	// +kubebuilder:validation:Optional
	Sink *SinkSpec `json:"sink,omitempty"`
	// Configurations as key-value pairs
	// +kubebuilder:validation:Optional
	Config map[string]v1.JSON `json:"config,omitempty"`
}

// SourceSpec defines a source or sink specification
// +kubebuilder:object:generate=true
// +kubebuilder:validation:Optional
type SourceSpec struct {
	// Pulsar source specification
	// +kubebuilder:validation:Optional
	Pulsar *PulsarSourceSpec `json:"pulsar,omitempty"`
}

// PulsarSourceSpec defines the Pulsar source details
// +kubebuilder:object:generate=true
// +kubebuilder:validation:Optional
type PulsarSourceSpec struct {
	// Topic name
	// +kubebuilder:validation:Required
	Topic string `json:"topic"`
}

// SinkSpec defines a sink specification
// +kubebuilder:object:generate=true
// +kubebuilder:validation:Optional
type SinkSpec struct {
	// Pulsar sink specification
	// +kubebuilder:validation:Optional
	Pulsar *PulsarSinkSpec `json:"pulsar,omitempty"`
}

// PulsarSinkSpec defines the Pulsar sink details
// +kubebuilder:object:generate=true
// +kubebuilder:validation:Optional
type PulsarSinkSpec struct {
	// Topic name
	// +kubebuilder:validation:Required
	Topic string `json:"topic"`
}

// FunctionStatus defines the observed state of Function
type FunctionStatus struct {
	// Number of available pods (ready for at least minReadySeconds)
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`
	// Total number of ready pods
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// Total number of non-terminated pods targeted by this deployment
	Replicas int32 `json:"replicas,omitempty"`
	// Total number of updated pods
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`
	// Most recent generation observed for this Function
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Function is the Schema for the functions API.
type Function struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FunctionSpec   `json:"spec,omitempty"`
	Status FunctionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// FunctionList contains a list of Function.
type FunctionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Function `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Function{}, &FunctionList{})
}
