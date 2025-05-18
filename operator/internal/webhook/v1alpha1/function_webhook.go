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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	fsv1alpha1 "github.com/FunctionStream/function-stream/operator/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var functionlog = logf.Log.WithName("function-resource")

// SetupFunctionWebhookWithManager registers the webhook for Function in the manager.
func SetupFunctionWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&fsv1alpha1.Function{}).
		WithValidator(&FunctionCustomValidator{Client: mgr.GetClient()}).
		WithDefaulter(&FunctionCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-fs-functionstream-github-io-v1alpha1-function,mutating=true,failurePolicy=fail,sideEffects=None,groups=fs.functionstream.github.io,resources=functions,verbs=create;update,versions=v1alpha1,name=mfunction-v1alpha1.kb.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=fs.functionstream.github.io,resources=packages,verbs=get;list;watch
// +kubebuilder:rbac:groups=fs.functionstream.github.io,resources=functions,verbs=get;list;watch

// FunctionCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Function when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type FunctionCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &FunctionCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Function.
func (d *FunctionCustomDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	function, ok := obj.(*fsv1alpha1.Function)

	if !ok {
		return fmt.Errorf("expected an Function object but got %T", obj)
	}
	functionlog.Info("Defaulting for Function", "name", function.GetName())

	// Ensure the 'package' label is always set to the current Spec.Package value
	if function.Labels == nil {
		function.Labels = make(map[string]string)
	}
	function.Labels["package"] = function.Spec.Package

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-fs-functionstream-github-io-v1alpha1-function,mutating=false,failurePolicy=fail,sideEffects=None,groups=fs.functionstream.github.io,resources=functions,verbs=create;update,versions=v1alpha1,name=vfunction-v1alpha1.kb.io,admissionReviewVersions=v1

// FunctionCustomValidator struct is responsible for validating the Function resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type FunctionCustomValidator struct {
	Client client.Client
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &FunctionCustomValidator{}

// validateReferences checks that all referenced resources in the Function exist.
func (v *FunctionCustomValidator) validateReferences(ctx context.Context, function *fsv1alpha1.Function) error {
	// Check if the referenced package exists
	var pkg fsv1alpha1.Package
	err := v.Client.Get(ctx, client.ObjectKey{
		Namespace: function.Namespace,
		Name:      function.Spec.Package,
	}, &pkg)
	if err != nil {
		return fmt.Errorf("referenced package '%s' not found in namespace '%s': %w", function.Spec.Package, function.Namespace, err)
	}
	// Add more reference checks here in the future as needed
	return nil
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Function.
func (v *FunctionCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	function, ok := obj.(*fsv1alpha1.Function)
	if !ok {
		return nil, fmt.Errorf("expected a Function object but got %T", obj)
	}
	functionlog.Info("Validation for Function upon creation", "name", function.GetName())

	if err := v.validateReferences(ctx, function); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Function.
func (v *FunctionCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	function, ok := newObj.(*fsv1alpha1.Function)
	if !ok {
		return nil, fmt.Errorf("expected a Function object for the newObj but got %T", newObj)
	}
	functionlog.Info("Validation for Function upon update", "name", function.GetName())

	if err := v.validateReferences(ctx, function); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Function.
func (v *FunctionCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	function, ok := obj.(*fsv1alpha1.Function)
	if !ok {
		return nil, fmt.Errorf("expected a Function object but got %T", obj)
	}
	functionlog.Info("Validation for Function upon deletion", "name", function.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
