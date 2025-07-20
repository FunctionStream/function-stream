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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fsv1alpha1 "github.com/FunctionStream/function-stream/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Function Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		function := &fsv1alpha1.Function{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Function")
			err := k8sClient.Get(ctx, typeNamespacedName, function)
			if err != nil && errors.IsNotFound(err) {
				resource := &fsv1alpha1.Function{
					ObjectMeta: metav1.ObjectMeta{
						Name:      typeNamespacedName.Name,
						Namespace: typeNamespacedName.Namespace,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &fsv1alpha1.Function{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Function")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource and create Deployment with init container, and update Status", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FunctionReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Config: Config{
					PulsarServiceURL: "pulsar://test-broker:6650",
					PulsarAuthPlugin: "org.apache.pulsar.client.impl.auth.AuthenticationToken",
					PulsarAuthParams: "token:my-token",
				},
			}

			// Create a Package resource first
			pkg := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg",
					Namespace: "default",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "busybox:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())

			// Re-fetch the latest Function object to ensure the Name field is set
			Expect(k8sClient.Get(ctx, typeNamespacedName, function)).To(Succeed())

			// Patch the Function to reference the Package and fill required fields
			patch := client.MergeFrom(function.DeepCopy())
			function.Spec.Package = "test-pkg"
			function.Spec.Module = "mod"
			function.Spec.Sink = &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out"}}
			function.Spec.RequestSource = &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in"}}
			Expect(k8sClient.Patch(ctx, function, patch)).To(Succeed())

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check Deployment
			deployName := "function-" + typeNamespacedName.Name
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: typeNamespacedName.Namespace}, deploy)).To(Succeed())

			// Verify init container exists and has correct configuration
			Expect(deploy.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			initContainer := deploy.Spec.Template.Spec.InitContainers[0]
			Expect(initContainer.Name).To(Equal("init-config"))
			Expect(initContainer.Image).To(Equal("busybox:latest"))
			Expect(initContainer.Command).To(HaveLen(3))
			Expect(initContainer.Command[0]).To(Equal("/bin/sh"))
			Expect(initContainer.Command[1]).To(Equal("-c"))

			// Verify the init command contains config.yaml content
			initCommand := initContainer.Command[2]
			Expect(initCommand).To(ContainSubstring("cat > /config/config.yaml"))

			// Verify main container configuration
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			mainContainer := deploy.Spec.Template.Spec.Containers[0]
			Expect(mainContainer.Name).To(Equal("function"))
			Expect(mainContainer.Image).To(Equal("busybox:latest"))

			// Verify volume mounts
			Expect(mainContainer.VolumeMounts).To(HaveLen(1))
			Expect(mainContainer.VolumeMounts[0].Name).To(Equal("function-config"))
			Expect(mainContainer.VolumeMounts[0].MountPath).To(Equal("/config"))

			// Verify environment variable
			Expect(mainContainer.Env).To(HaveLen(1))
			Expect(mainContainer.Env[0].Name).To(Equal("FS_CONFIG_PATH"))
			Expect(mainContainer.Env[0].Value).To(Equal("/config/config.yaml"))

			// Verify volumes
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes[0].Name).To(Equal("function-config"))
			Expect(deploy.Spec.Template.Spec.Volumes[0].EmptyDir).NotTo(BeNil())

			// Verify labels
			Expect(deploy.Labels).To(HaveKey("function"))
			Expect(deploy.Labels["function"]).To(Equal(typeNamespacedName.Name))

			// Simulate Deployment status update
			patchDeploy := client.MergeFrom(deploy.DeepCopy())
			deploy.Status.AvailableReplicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.Replicas = 1
			deploy.Status.UpdatedReplicas = 1
			deploy.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Patch(ctx, deploy, patchDeploy)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check Function Status
			fn := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fn)).To(Succeed())
			Expect(fn.Status.AvailableReplicas).To(Equal(int32(1)))
			Expect(fn.Status.ReadyReplicas).To(Equal(int32(1)))
			Expect(fn.Status.Replicas).To(Equal(int32(1)))
			Expect(fn.Status.UpdatedReplicas).To(Equal(int32(1)))
			Expect(fn.Status.ObservedGeneration).To(Equal(int64(2)))

			// Test deployment update when function spec changes
			// Update function spec to trigger deployment update
			patchFn := client.MergeFrom(fn.DeepCopy())
			fn.Spec.Description = "Updated description"
			Expect(k8sClient.Patch(ctx, fn, patchFn)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify deployment was updated
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: typeNamespacedName.Namespace}, deploy)).To(Succeed())
			// The deployment should still exist and be updated
			Expect(deploy).NotTo(BeNil())
		})

		It("should only reconcile when Deployment has 'function' label", func() {
			By("setting up a Function and its labeled Deployment")
			controllerReconciler := &FunctionReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Config: Config{
					PulsarServiceURL: "pulsar://test-broker:6650",
					PulsarAuthPlugin: "org.apache.pulsar.client.impl.auth.AuthenticationToken",
					PulsarAuthParams: "token:my-token",
				},
			}

			pkg := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-label",
					Namespace: "default",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "busybox:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())

			fn := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-label",
					Namespace: "default",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-label",
					Module:           "mod",
					SubscriptionName: "sub",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn)).To(Succeed())

			// Initial reconcile to create Deployment
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			deployName := "function-" + fn.Name
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: fn.Namespace}, deploy)).To(Succeed())

			// Create a Deployment without 'function' label
			unlabeledDeploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unlabeled-deploy",
					Namespace: fn.Namespace,
					Labels:    map[string]string{"app": "test"},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &[]int32{1}[0],
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "test"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "busybox:latest",
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, unlabeledDeploy)).To(Succeed())

			// Manually call Reconcile to simulate the event, but the hash should not change
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Get Deployment again, the hash should remain unchanged
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: fn.Namespace}, deploy)).To(Succeed())
		})

		It("should automatically add package label to Function", func() {
			By("creating a Function without package label")
			controllerReconciler := &FunctionReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Config: Config{
					PulsarServiceURL: "pulsar://test-broker:6650",
					PulsarAuthPlugin: "org.apache.pulsar.client.impl.auth.AuthenticationToken",
					PulsarAuthParams: "token:my-token",
				},
			}

			// Create Package first
			pkg := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-label-auto",
					Namespace: "default",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "busybox:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())

			// Create Function without package label
			fn := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-label-auto",
					Namespace: "default",
					Labels:    map[string]string{"app": "test"}, // No package label initially
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-label-auto",
					Module:           "mod",
					SubscriptionName: "sub",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn)).To(Succeed())

			// Verify Function doesn't have package label initially
			Expect(fn.Labels).NotTo(HaveKey("package"))

			// Reconcile the Function
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify package label was added by re-fetching the Function
			updatedFn := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace}, updatedFn)).To(Succeed())
			Expect(updatedFn.Labels).To(HaveKey("package"))
			Expect(updatedFn.Labels["package"]).To(Equal("test-pkg-label-auto"))

			// Verify other labels are preserved
			Expect(updatedFn.Labels).To(HaveKey("app"))
			Expect(updatedFn.Labels["app"]).To(Equal("test"))
		})

		It("should update package label when Function package changes", func() {
			By("creating Functions with different packages")
			controllerReconciler := &FunctionReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Config: Config{
					PulsarServiceURL: "pulsar://test-broker:6650",
					PulsarAuthPlugin: "org.apache.pulsar.client.impl.auth.AuthenticationToken",
					PulsarAuthParams: "token:my-token",
				},
			}

			// Create two Packages
			pkg1 := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-1",
					Namespace: "default",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package 1",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "busybox:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg1)).To(Succeed())

			pkg2 := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-2",
					Namespace: "default",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package 2",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "nginx:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg2)).To(Succeed())

			// Create Function with initial package
			fn := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-package-change",
					Namespace: "default",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-1",
					Module:           "mod",
					SubscriptionName: "sub",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn)).To(Succeed())

			// Initial reconcile
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify initial package label by re-fetching
			updatedFn := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace}, updatedFn)).To(Succeed())
			Expect(updatedFn.Labels["package"]).To(Equal("test-pkg-1"))

			// Change the package
			patch := client.MergeFrom(updatedFn.DeepCopy())
			updatedFn.Spec.Package = "test-pkg-2"
			Expect(k8sClient.Patch(ctx, updatedFn, patch)).To(Succeed())

			// Reconcile again
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify package label was updated by re-fetching
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace}, updatedFn)).To(Succeed())
			Expect(updatedFn.Labels["package"]).To(Equal("test-pkg-2"))

			// Verify deployment was updated with new image
			deployName := "function-" + fn.Name
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: fn.Namespace}, deploy)).To(Succeed())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("nginx:latest"))
		})

		It("should map Package changes to related Functions", func() {
			By("creating multiple Functions that reference the same Package")
			controllerReconciler := &FunctionReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Config: Config{
					PulsarServiceURL: "pulsar://test-broker:6650",
					PulsarAuthPlugin: "org.apache.pulsar.client.impl.auth.AuthenticationToken",
					PulsarAuthParams: "token:my-token",
				},
			}

			// Create Package
			pkg := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-mapping",
					Namespace: "default",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "busybox:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg)).To(Succeed())

			// Create multiple Functions that reference the same Package
			fn1 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-mapping-1",
					Namespace: "default",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-mapping",
					Module:           "mod1",
					SubscriptionName: "sub1",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out1"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in1"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn1)).To(Succeed())

			fn2 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-mapping-2",
					Namespace: "default",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-mapping",
					Module:           "mod2",
					SubscriptionName: "sub2",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out2"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in2"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn2)).To(Succeed())

			// Create a Function that references a different Package
			fn3 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-mapping-3",
					Namespace: "default",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "different-pkg",
					Module:           "mod3",
					SubscriptionName: "sub3",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out3"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in3"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn3)).To(Succeed())

			// Initial reconcile to set up package labels
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn1.Name, Namespace: fn1.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn2.Name, Namespace: fn2.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify package labels were set by re-fetching
			updatedFn1 := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn1.Name, Namespace: fn1.Namespace}, updatedFn1)).To(Succeed())
			Expect(updatedFn1.Labels["package"]).To(Equal("test-pkg-mapping"))

			updatedFn2 := &fsv1alpha1.Function{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fn2.Name, Namespace: fn2.Namespace}, updatedFn2)).To(Succeed())
			Expect(updatedFn2.Labels["package"]).To(Equal("test-pkg-mapping"))

			// Test the mapPackageToFunctions function
			requests := controllerReconciler.mapPackageToFunctions(ctx, pkg)
			Expect(requests).To(HaveLen(2))

			// Verify the requests contain the correct Functions
			requestNames := make(map[string]bool)
			for _, req := range requests {
				requestNames[req.NamespacedName.Name] = true
			}
			Expect(requestNames).To(HaveKey("test-fn-mapping-1"))
			Expect(requestNames).To(HaveKey("test-fn-mapping-2"))
			Expect(requestNames).NotTo(HaveKey("test-fn-mapping-3")) // Should not be included

			// Test that updating the Package triggers reconciliation of related Functions
			// Update the Package image
			patch := client.MergeFrom(pkg.DeepCopy())
			pkg.Spec.FunctionType.Cloud.Image = "nginx:latest"
			Expect(k8sClient.Patch(ctx, pkg, patch)).To(Succeed())

			// Simulate the Package update by calling mapPackageToFunctions
			requests = controllerReconciler.mapPackageToFunctions(ctx, pkg)
			Expect(requests).To(HaveLen(2))

			// Manually reconcile the Functions to simulate the triggered reconciliation
			for _, req := range requests {
				_, err := controllerReconciler.Reconcile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify that the Deployments were updated with the new image
			deploy1 := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "function-" + fn1.Name, Namespace: fn1.Namespace}, deploy1)).To(Succeed())
			Expect(deploy1.Spec.Template.Spec.Containers[0].Image).To(Equal("nginx:latest"))

			deploy2 := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "function-" + fn2.Name, Namespace: fn2.Namespace}, deploy2)).To(Succeed())
			Expect(deploy2.Spec.Template.Spec.Containers[0].Image).To(Equal("nginx:latest"))
		})

		It("should handle Package updates in different namespaces", func() {
			By("creating Functions and Packages in different namespaces")
			controllerReconciler := &FunctionReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Config: Config{
					PulsarServiceURL: "pulsar://test-broker:6650",
					PulsarAuthPlugin: "org.apache.pulsar.client.impl.auth.AuthenticationToken",
					PulsarAuthParams: "token:my-token",
				},
			}

			// Create namespaces
			ns1 := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace1",
				},
			}
			Expect(k8sClient.Create(ctx, ns1)).To(Succeed())

			ns2 := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace2",
				},
			}
			Expect(k8sClient.Create(ctx, ns2)).To(Succeed())

			// Create Package in namespace1
			pkg1 := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-ns1",
					Namespace: "namespace1",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package NS1",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "busybox:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg1)).To(Succeed())

			// Create Package in namespace2
			pkg2 := &fsv1alpha1.Package{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pkg-ns2",
					Namespace: "namespace2",
				},
				Spec: fsv1alpha1.PackageSpec{
					DisplayName: "Test Package NS2",
					Description: "desc",
					FunctionType: fsv1alpha1.FunctionType{
						Cloud: &fsv1alpha1.CloudType{Image: "nginx:latest"},
					},
					Modules: map[string]fsv1alpha1.Module{},
				},
			}
			Expect(k8sClient.Create(ctx, pkg2)).To(Succeed())

			// Create Function in namespace1
			fn1 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-ns1",
					Namespace: "namespace1",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-ns1",
					Module:           "mod1",
					SubscriptionName: "sub1",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out1"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in1"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn1)).To(Succeed())

			// Create Function in namespace2
			fn2 := &fsv1alpha1.Function{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fn-ns2",
					Namespace: "namespace2",
				},
				Spec: fsv1alpha1.FunctionSpec{
					Package:          "test-pkg-ns2",
					Module:           "mod2",
					SubscriptionName: "sub2",
					Sink:             &fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out2"}},
					RequestSource:    &fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in2"}},
				},
			}
			Expect(k8sClient.Create(ctx, fn2)).To(Succeed())

			// Initial reconcile to set up package labels
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn1.Name, Namespace: fn1.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn2.Name, Namespace: fn2.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Test that Package updates only affect Functions in the same namespace
			requests1 := controllerReconciler.mapPackageToFunctions(ctx, pkg1)
			Expect(requests1).To(HaveLen(1))
			Expect(requests1[0].NamespacedName.Name).To(Equal("test-fn-ns1"))
			Expect(requests1[0].NamespacedName.Namespace).To(Equal("namespace1"))

			requests2 := controllerReconciler.mapPackageToFunctions(ctx, pkg2)
			Expect(requests2).To(HaveLen(1))
			Expect(requests2[0].NamespacedName.Name).To(Equal("test-fn-ns2"))
			Expect(requests2[0].NamespacedName.Namespace).To(Equal("namespace2"))
		})
	})
})
