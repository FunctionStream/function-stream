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
			function.Spec.Sink = fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out"}}
			function.Spec.RequestSource = fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in"}}
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
			Expect(deploy.Labels).To(HaveKey("configmap-hash"))
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

			// Test config hash update when function spec changes
			oldHash := deploy.Labels["configmap-hash"]

			// Update function spec to trigger hash change
			patchFn := client.MergeFrom(fn.DeepCopy())
			fn.Spec.Description = "Updated description"
			Expect(k8sClient.Patch(ctx, fn, patchFn)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify deployment hash has changed
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: typeNamespacedName.Namespace}, deploy)).To(Succeed())
			newHash := deploy.Labels["configmap-hash"]
			Expect(newHash).NotTo(Equal(oldHash))
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
					Package:       "test-pkg-label",
					Module:        "mod",
					Sink:          fsv1alpha1.SinkSpec{Pulsar: &fsv1alpha1.PulsarSinkSpec{Topic: "out"}},
					RequestSource: fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in", SubscriptionName: "sub"}},
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
			oldHash := deploy.Labels["configmap-hash"]

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
			Expect(deploy.Labels["configmap-hash"]).To(Equal(oldHash))
		})
	})
})
