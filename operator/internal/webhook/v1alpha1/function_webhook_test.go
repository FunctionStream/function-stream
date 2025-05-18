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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	fsv1alpha1 "github.com/FunctionStream/function-stream/operator/api/v1alpha1"
	// TODO (user): Add any additional imports if needed
)

var _ = Describe("Function Webhook", func() {
	var (
		obj       *fsv1alpha1.Function
		oldObj    *fsv1alpha1.Function
		validator FunctionCustomValidator
		defaulter FunctionCustomDefaulter
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		obj = &fsv1alpha1.Function{
			ObjectMeta: fsv1alpha1.Function{}.ObjectMeta,
			Spec: fsv1alpha1.FunctionSpec{
				DisplayName: "test-function",
				Description: "desc",
				Package:     "test-pkg",
				Module:      "test-module",
			},
		}
		oldObj = &fsv1alpha1.Function{
			ObjectMeta: fsv1alpha1.Function{}.ObjectMeta,
			Spec: fsv1alpha1.FunctionSpec{
				DisplayName: "old-function",
				Description: "desc",
				Package:     "test-pkg",
				Module:      "test-module",
			},
		}
		validator = FunctionCustomValidator{Client: k8sClient}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = FunctionCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		// TODO (user): Add any setup logic common to all tests
	})

	AfterEach(func() {
		// Clean up the test package if it exists
		_ = k8sClient.Delete(ctx, &fsv1alpha1.Package{
			ObjectMeta: fsv1alpha1.Package{}.ObjectMeta,
			// Namespace and Name will be set in the test
		})
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("Reference validation", func() {
		It("should deny creation if the referenced package does not exist", func() {
			obj.Namespace = "default"
			obj.Spec.Package = "nonexistent-pkg"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced package"))
		})

		It("should allow creation if the referenced package exists", func() {
			obj.Namespace = "default"
			obj.Spec.Package = "existing-pkg"
			// Create the referenced package with required fields
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "existing-pkg"
			pkg.Namespace = "default"
			pkg.Spec.DisplayName = "test"
			pkg.Spec.Description = "desc"
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny update if the referenced package does not exist", func() {
			obj.Namespace = "default"
			obj.Spec.Package = "nonexistent-pkg"
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced package"))
		})

		It("should allow update if the referenced package exists", func() {
			obj.Namespace = "default"
			obj.Spec.Package = "existing-pkg"
			// Create the referenced package with required fields
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "existing-pkg"
			pkg.Namespace = "default"
			pkg.Spec.DisplayName = "test"
			pkg.Spec.Description = "desc"
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("Defaulter logic for package label", func() {
		It("should set the 'package' label to the value of spec.package on creation", func() {
			obj.Namespace = "default"
			obj.Spec.Package = "pkg-on-create"

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "pkg-on-create"
			pkg.Namespace = "default"
			pkg.Spec.DisplayName = "test"
			pkg.Spec.Description = "desc"
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Call the defaulter
			obj.Labels = nil // simulate no labels set
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "pkg-on-create"))
		})

		It("should update the 'package' label to the new spec.package value on update", func() {
			obj.Namespace = "default"
			obj.Spec.Package = "pkg-on-update"

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "pkg-on-update"
			pkg.Namespace = "default"
			pkg.Spec.DisplayName = "test"
			pkg.Spec.Description = "desc"
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Simulate an existing label with an old value
			obj.Labels = map[string]string{"package": "old-pkg"}
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "pkg-on-update"))
		})
	})

})
