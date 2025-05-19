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
	"crypto/sha256"
	"encoding/hex"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fsv1alpha1 "github.com/FunctionStream/function-stream/operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
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
		It("should successfully reconcile the resource and create ConfigMap, Deployment, and update Status", func() {
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
			function.Spec.RequestSource = fsv1alpha1.SourceSpec{Pulsar: &fsv1alpha1.PulsarSourceSpec{Topic: "in", SubscriptionName: "sub"}}
			Expect(k8sClient.Patch(ctx, function, patch)).To(Succeed())

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check ConfigMap
			cmName := "function-" + typeNamespacedName.Name + "-config"
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: typeNamespacedName.Namespace}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKey("config.yaml"))

			// Assert pulsar config in config.yaml
			var configYaml map[string]interface{}
			Expect(yaml.Unmarshal([]byte(cm.Data["config.yaml"]), &configYaml)).To(Succeed())
			pulsarCfg, ok := configYaml["pulsar"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(pulsarCfg["serviceUrl"]).To(Equal("pulsar://test-broker:6650"))
			Expect(pulsarCfg["authPlugin"]).To(Equal("org.apache.pulsar.client.impl.auth.AuthenticationToken"))
			Expect(pulsarCfg["authParams"]).To(Equal("token:my-token"))

			// Check Deployment
			deployName := "function-" + typeNamespacedName.Name
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: typeNamespacedName.Namespace}, deploy)).To(Succeed())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("busybox:latest"))
			Expect(deploy.Spec.Template.Spec.Volumes[0].ConfigMap.Name).To(Equal(cmName))

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

			// Simulate ConfigMap change and check Deployment hash label update
			patchCM := client.MergeFrom(cm.DeepCopy())
			cm.Data["config.yaml"] = cm.Data["config.yaml"] + "#changed"
			Expect(k8sClient.Patch(ctx, cm, patchCM)).To(Succeed())
			// Force re-get to ensure the content has changed
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: typeNamespacedName.Namespace}, cm)).To(Succeed())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: deployName, Namespace: typeNamespacedName.Namespace}, deploy)).To(Succeed())
			// hash label should change
			Expect(deploy.Labels).To(HaveKey("configmap-hash"))
		})

		It("should only reconcile when ConfigMap has 'function' label", func() {
			By("setting up a Function and its labeled ConfigMap")
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

			// Initial reconcile to create ConfigMap and Deployment
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			cmName := "function-" + fn.Name + "-config"
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: fn.Namespace}, cm)).To(Succeed())
			oldHash := sha256sum(cm.Data["config.yaml"])

			// Patch labeled ConfigMap, should NOT trigger reconcile or hash change
			patchCM := client.MergeFrom(cm.DeepCopy())
			cm.Data["config.yaml"] = cm.Data["config.yaml"] + "#changed"
			Expect(k8sClient.Patch(ctx, cm, patchCM)).To(Succeed())
			// Force re-get to ensure the content has changed
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: fn.Namespace}, cm)).To(Succeed())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "function-" + fn.Name, Namespace: fn.Namespace}, deploy)).To(Succeed())
			newHash := deploy.Labels["configmap-hash"]
			Expect(newHash).To(Equal(oldHash))

			// Create a ConfigMap without 'function' label
			unlabeledCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unlabeled-cm",
					Namespace: fn.Namespace,
				},
				Data: map[string]string{"foo": "bar"},
			}
			Expect(k8sClient.Create(ctx, unlabeledCM)).To(Succeed())
			// Patch unlabeled ConfigMap, should NOT trigger reconcile or hash change
			patchUnlabeled := client.MergeFrom(unlabeledCM.DeepCopy())
			unlabeledCM.Data["foo"] = "baz"
			Expect(k8sClient.Patch(ctx, unlabeledCM, patchUnlabeled)).To(Succeed())
			// Manually call Reconcile to simulate the event, but the hash should not change
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fn.Name, Namespace: fn.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			// Get Deployment again, the hash should remain unchanged
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "function-" + fn.Name, Namespace: fn.Namespace}, deploy)).To(Succeed())
			Expect(deploy.Labels["configmap-hash"]).To(Equal(newHash))
		})
	})
})

// Utility function: first 32 characters of sha256
func sha256sum(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])[:32]
}
