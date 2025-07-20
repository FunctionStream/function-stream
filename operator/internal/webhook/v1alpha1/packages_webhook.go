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
var packageslog = logf.Log.WithName("package-resource")

// SetupPackagesWebhookWithManager registers the webhook for Packages in the manager.
func SetupPackagesWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&fsv1alpha1.Package{}).
		WithValidator(&PackagesCustomValidator{Client: mgr.GetClient()}).
		WithDefaulter(&PackagesCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-fs-functionstream-github-io-v1alpha1-package,mutating=true,failurePolicy=fail,sideEffects=None,groups=fs.functionstream.github.io,resources=packages,verbs=create;update,versions=v1alpha1,name=mpackage-v1alpha1.kb.io,admissionReviewVersions=v1

// PackagesCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Packages when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type PackagesCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &PackagesCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Packages.
func (d *PackagesCustomDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	packages, ok := obj.(*fsv1alpha1.Package)

	if !ok {
		return fmt.Errorf("expected an Packages object but got %T", obj)
	}
	packageslog.Info("Defaulting for Packages", "name", packages.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-fs-functionstream-github-io-v1alpha1-package,mutating=false,failurePolicy=fail,sideEffects=None,groups=fs.functionstream.github.io,resources=packages,verbs=create;update;delete,versions=v1alpha1,name=vpackage-v1alpha1.kb.io,admissionReviewVersions=v1

// PackagesCustomValidator struct is responsible for validating the Packages resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type PackagesCustomValidator struct {
	Client client.Client
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &PackagesCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Packages.
func (v *PackagesCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	packages, ok := obj.(*fsv1alpha1.Package)
	if !ok {
		return nil, fmt.Errorf("expected a Packages object but got %T", obj)
	}
	packageslog.Info("Validation for Packages upon creation", "name", packages.GetName())

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

func (v *PackagesCustomValidator) referencingFunctions(ctx context.Context, namespace, packageName string) ([]string, error) {
	var functionList fsv1alpha1.FunctionList
	err := v.Client.List(ctx, &functionList, client.InNamespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to list Functions in namespace '%s': %w", namespace, err)
	}
	var referencing []string
	packageLabel := fmt.Sprintf("%s.%s", namespace, packageName)
	for _, fn := range functionList.Items {
		if fn.Labels["package"] == packageLabel {
			referencing = append(referencing, fn.Name)
		}
	}
	return referencing, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Packages.
func (v *PackagesCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Packages.
func (v *PackagesCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	packages, ok := obj.(*fsv1alpha1.Package)
	if !ok {
		return nil, fmt.Errorf("expected a Packages object but got %T", obj)
	}
	packageslog.Info("Validation for Packages upon deletion", "name", packages.GetName())

	if referencing, err := v.referencingFunctions(ctx, packages.Namespace, packages.Name); err != nil {
		return nil, err
	} else if len(referencing) > 0 {
		return nil, fmt.Errorf("cannot delete Package '%s' because it is referenced by the following Functions in the same namespace: %v", packages.Name, referencing)
	}

	return nil, nil
}
