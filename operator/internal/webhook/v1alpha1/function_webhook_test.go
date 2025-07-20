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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	// TODO (user): Add any additional imports if needed
)

const (
	defaultNamespace = "default"
	existingPkgName  = "existing-pkg"
	testDisplayName  = "test"
	testDescription  = "desc"
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
				PackageRef:  fsv1alpha1.PackageRef{Name: "test-pkg"},
				Module:      "test-module",
			},
		}
		oldObj = &fsv1alpha1.Function{
			ObjectMeta: fsv1alpha1.Function{}.ObjectMeta,
			Spec: fsv1alpha1.FunctionSpec{
				DisplayName: "old-function",
				Description: "desc",
				PackageRef:  fsv1alpha1.PackageRef{Name: "test-pkg"},
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
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef.Name = "nonexistent-pkg"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced package"))
		})

		It("should allow creation if the referenced package exists", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef.Name = existingPkgName
			// Create the referenced package with required fields
			pkg := &fsv1alpha1.Package{}
			pkg.Name = existingPkgName
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny update if the referenced package does not exist", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef.Name = "nonexistent-pkg"
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced package"))
		})

		It("should allow update if the referenced package exists", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef.Name = existingPkgName
			// Create the referenced package with required fields
			pkg := &fsv1alpha1.Package{}
			pkg.Name = existingPkgName
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
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
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef.Name = "pkg-on-create"

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "pkg-on-create"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Call the defaulter
			obj.Labels = nil // simulate no labels set
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.pkg-on-create"))
		})

		It("should update the 'package' label to the new spec.package value on update", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef.Name = "pkg-on-update"

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "pkg-on-update"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Simulate an existing label with an old value
			obj.Labels = map[string]string{"package": "old-pkg"}
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.pkg-on-update"))
		})
	})

	Context("Cross-namespace package references", func() {
		It("should allow creation if the referenced package exists in a different namespace", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "cross-pkg-1", Namespace: "other-ns-1"}

			// Create the namespace first
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-ns-1",
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })

			// Create the referenced package in a different namespace
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "cross-pkg-1"
			pkg.Namespace = "other-ns-1"
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny creation if the referenced package does not exist in the specified namespace", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "cross-pkg-2", Namespace: "wrong-ns-2"}

			// Create the namespace first
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "wrong-ns-2",
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })

			// Create another namespace for the package
			ns2 := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-ns-3",
				},
			}
			Expect(k8sClient.Create(ctx, ns2)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns2) })

			// Create a package with the same name in a different namespace
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "cross-pkg-2"
			pkg.Namespace = "other-ns-3" // different namespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("referenced package"))
		})

		It("should set the 'package' label correctly for cross-namespace references", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "cross-pkg-3", Namespace: "other-ns-4"}

			// Create the namespace first
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-ns-4",
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "cross-pkg-3"
			pkg.Namespace = "other-ns-4"
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Call the defaulter
			obj.Labels = nil // simulate no labels set
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "other-ns-4.cross-pkg-3"))
		})

		It("should set the 'package' label correctly for same-namespace references", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "same-pkg"} // no namespace specified

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "same-pkg"
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
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.same-pkg"))
		})
	})

	Context("Automatic package label generation", func() {
		It("should automatically generate package label when function is created without labels", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "auto-gen-pkg"}

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "auto-gen-pkg"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Function starts with no labels
			obj.Labels = nil
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.auto-gen-pkg"))
		})

		It("should override user-specified package label with auto-generated one", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "override-pkg"}

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "override-pkg"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// User tries to set their own package label (should be overridden)
			obj.Labels = map[string]string{
				"package": "user-specified-package",
				"app":     "my-app", // This should be preserved
			}
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.override-pkg"))
			Expect(obj.Labels).To(HaveKeyWithValue("app", "my-app"))
		})

		It("should automatically generate package label for cross-namespace references", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "cross-pkg", Namespace: "other-ns"}

			// Create namespace
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-ns",
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })

			// Create the referenced package in different namespace
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "cross-pkg"
			pkg.Namespace = "other-ns"
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Function starts with no labels
			obj.Labels = nil
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "other-ns.cross-pkg"))
		})

		It("should update auto-generated package label when package reference changes", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "old-pkg"}

			// Create old package
			oldPkg := &fsv1alpha1.Package{}
			oldPkg.Name = "old-pkg"
			oldPkg.Namespace = defaultNamespace
			oldPkg.Spec.DisplayName = testDisplayName
			oldPkg.Spec.Description = testDescription
			oldPkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			oldPkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, oldPkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, oldPkg) })

			// Create new package
			newPkg := &fsv1alpha1.Package{}
			newPkg.Name = "new-pkg"
			newPkg.Namespace = defaultNamespace
			newPkg.Spec.DisplayName = testDisplayName
			newPkg.Spec.Description = testDescription
			newPkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			newPkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, newPkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, newPkg) })

			// Initial auto-generated label
			obj.Labels = nil
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.old-pkg"))

			// Change package reference
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "new-pkg"}
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.new-pkg"))
		})

		It("should preserve other labels when auto-generating package label", func() {
			obj.Namespace = defaultNamespace
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "preserve-pkg"}

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "preserve-pkg"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Function has existing labels
			obj.Labels = map[string]string{
				"app":         "my-app",
				"version":     "v1.0.0",
				"environment": "production",
			}
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.preserve-pkg"))
			Expect(obj.Labels).To(HaveKeyWithValue("app", "my-app"))
			Expect(obj.Labels).To(HaveKeyWithValue("version", "v1.0.0"))
			Expect(obj.Labels).To(HaveKeyWithValue("environment", "production"))
		})
	})

	Context("Integration tests for automatic package label generation", func() {
		It("should create function with auto-generated package label through webhook", func() {
			obj.Namespace = defaultNamespace
			obj.Name = "auto-gen-test-fn"
			obj.Spec.PackageRef = fsv1alpha1.PackageRef{Name: "auto-gen-integration-pkg"}

			// Create the referenced package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "auto-gen-integration-pkg"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Apply defaulter to simulate webhook behavior
			Expect(defaulter.Default(ctx, obj)).To(Succeed())

			// Verify package label is auto-generated correctly
			Expect(obj.Labels).To(HaveKeyWithValue("package", "default.auto-gen-integration-pkg"))

			// Simulate creating the function in the cluster
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, obj) })

			// Verify the function was created with the auto-generated label
			createdFn := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace}, createdFn)).To(Succeed())
			Expect(createdFn.Labels).To(HaveKeyWithValue("package", "default.auto-gen-integration-pkg"))
		})

		It("should handle multiple functions with auto-generated package labels", func() {
			// Create the package
			pkg := &fsv1alpha1.Package{}
			pkg.Name = "multi-fn-pkg"
			pkg.Namespace = defaultNamespace
			pkg.Spec.DisplayName = testDisplayName
			pkg.Spec.Description = testDescription
			pkg.Spec.FunctionType = fsv1alpha1.FunctionType{}
			pkg.Spec.Modules = map[string]fsv1alpha1.Module{"test-module": {DisplayName: "mod", Description: "desc"}}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pkg) })

			// Create first function (no labels initially)
			fn1 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-fn1",
					Namespace: defaultNamespace,
				},
				Spec: fsv1alpha1.FunctionSpec{
					DisplayName: "Function 1",
					Description: "desc",
					PackageRef:  fsv1alpha1.PackageRef{Name: "multi-fn-pkg"},
					Module:      "test-module",
				},
			}
			Expect(defaulter.Default(ctx, fn1)).To(Succeed())
			Expect(k8sClient.Create(ctx, fn1)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, fn1) })

			// Create second function (with some existing labels)
			fn2 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-fn2",
					Namespace: defaultNamespace,
					Labels: map[string]string{
						"app":     "my-app",
						"version": "v1.0.0",
					},
				},
				Spec: fsv1alpha1.FunctionSpec{
					DisplayName: "Function 2",
					Description: "desc",
					PackageRef:  fsv1alpha1.PackageRef{Name: "multi-fn-pkg"},
					Module:      "test-module",
				},
			}
			Expect(defaulter.Default(ctx, fn2)).To(Succeed())
			Expect(k8sClient.Create(ctx, fn2)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, fn2) })

			// Verify both functions have the correct auto-generated package label
			createdFn1 := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn1.Name, Namespace: fn1.Namespace}, createdFn1)).To(Succeed())
			Expect(createdFn1.Labels).To(HaveKeyWithValue("package", "default.multi-fn-pkg"))

			createdFn2 := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn2.Name, Namespace: fn2.Namespace}, createdFn2)).To(Succeed())
			Expect(createdFn2.Labels).To(HaveKeyWithValue("package", "default.multi-fn-pkg"))
			Expect(createdFn2.Labels).To(HaveKeyWithValue("app", "my-app"))
			Expect(createdFn2.Labels).To(HaveKeyWithValue("version", "v1.0.0"))
		})
	})

})
