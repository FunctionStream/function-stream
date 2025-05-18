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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	fsv1alpha1 "github.com/FunctionStream/function-stream/operator/api/v1alpha1"
	// TODO (user): Add any additional imports if needed
	"context"
)

var _ = Describe("Packages Webhook", func() {
	var (
		obj       *fsv1alpha1.Package
		oldObj    *fsv1alpha1.Package
		validator PackagesCustomValidator
		defaulter PackagesCustomDefaulter
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		obj = &fsv1alpha1.Package{}
		oldObj = &fsv1alpha1.Package{}
		obj.Name = "test-pkg"
		obj.Namespace = "default"
		obj.Spec.DisplayName = "test-pkg"
		obj.Spec.Description = "desc"
		obj.Spec.FunctionType = fsv1alpha1.FunctionType{}
		obj.Spec.Modules = map[string]fsv1alpha1.Module{"mod": {DisplayName: "mod", Description: "desc"}}
		oldObj.Name = obj.Name
		oldObj.Namespace = obj.Namespace
		oldObj.Spec = obj.Spec
		validator = PackagesCustomValidator{Client: k8sClient}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = PackagesCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		// Clean up before each test
		_ = k8sClient.Delete(ctx, obj)
	})

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, obj)
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating Packages under Defaulting Webhook", func() {
		// TODO (user): Add logic for defaulting webhooks
		// Example:
		// It("Should apply defaults when a required field is empty", func() {
		//     By("simulating a scenario where defaults should be applied")
		//     obj.SomeFieldWithDefault = ""
		//     By("calling the Default method to apply defaults")
		//     defaulter.Default(ctx, obj)
		//     By("checking that the default values are set")
		//     Expect(obj.SomeFieldWithDefault).To(Equal("default_value"))
		// })
	})

	Context("When creating or updating Packages under Validating Webhook", func() {
		// TODO (user): Add logic for validating webhooks
		// Example:
		// It("Should deny creation if a required field is missing", func() {
		//     By("simulating an invalid creation scenario")
		//     obj.SomeRequiredField = ""
		//     Expect(validator.ValidateCreate(ctx, obj)).Error().To(HaveOccurred())
		// })
		//
		// It("Should admit creation if all required fields are present", func() {
		//     By("simulating an invalid creation scenario")
		//     obj.SomeRequiredField = "valid_value"
		//     Expect(validator.ValidateCreate(ctx, obj)).To(BeNil())
		// })
		//
		// It("Should validate updates correctly", func() {
		//     By("simulating a valid update scenario")
		//     oldObj.SomeRequiredField = "updated_value"
		//     obj.SomeRequiredField = "updated_value"
		//     Expect(validator.ValidateUpdate(ctx, oldObj, obj)).To(BeNil())
		// })
	})

	Context("Validating Webhook for update/delete with referencing Functions", func() {
		It("should deny update if one Function references the Package", func() {
			// Create the Package
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			// Create a referencing Function
			fn := &fsv1alpha1.Function{
				ObjectMeta: fsv1alpha1.Function{}.ObjectMeta,
				Spec: fsv1alpha1.FunctionSpec{
					DisplayName: "fn1",
					Description: "desc",
					Package:     obj.Name,
					Module:      "mod",
				},
			}
			fn.Name = "fn1"
			fn.Namespace = obj.Namespace
			fn.Labels = map[string]string{"package": obj.Name}
			Expect(k8sClient.Create(ctx, fn)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, fn) })
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced by the following Functions"))
			Expect(err.Error()).To(ContainSubstring("fn1"))
		})

		It("should deny delete if multiple Functions reference the Package", func() {
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			// Create two referencing Functions
			fn1 := &fsv1alpha1.Function{
				ObjectMeta: fsv1alpha1.Function{}.ObjectMeta,
				Spec: fsv1alpha1.FunctionSpec{
					DisplayName: "fn1",
					Description: "desc",
					Package:     obj.Name,
					Module:      "mod",
				},
			}
			fn1.Name = "fn1"
			fn1.Namespace = obj.Namespace
			fn1.Labels = map[string]string{"package": obj.Name}
			fn2 := fn1.DeepCopy()
			fn2.Name = "fn2"
			Expect(k8sClient.Create(ctx, fn1)).To(Succeed())
			Expect(k8sClient.Create(ctx, fn2)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, fn1); _ = k8sClient.Delete(ctx, fn2) })
			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced by the following Functions"))
			Expect(err.Error()).To(ContainSubstring("fn1"))
			Expect(err.Error()).To(ContainSubstring("fn2"))
		})

		It("should allow update and delete if no Function references the Package", func() {
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).ToNot(HaveOccurred())
			_, err = validator.ValidateDelete(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
