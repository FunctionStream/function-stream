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
	"fmt"
	"reflect"

	"github.com/FunctionStream/function-stream/operator/utils"
	"k8s.io/apimachinery/pkg/util/json"

	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fsv1alpha1 "github.com/FunctionStream/function-stream/operator/api/v1alpha1"
)

// Config holds operator configuration (e.g. for messaging systems)
type Config struct {
	PulsarServiceURL string
	PulsarAuthPlugin string
	PulsarAuthParams string
}

// FunctionReconciler reconciles a Function object
type FunctionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config Config
}

// +kubebuilder:rbac:groups=fs.functionstream.github.io,resources=functions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fs.functionstream.github.io,resources=packages,verbs=get;list;watch
// +kubebuilder:rbac:groups=fs.functionstream.github.io,resources=functions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fs.functionstream.github.io,resources=functions/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Function object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *FunctionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling Function", "function", req.NamespacedName)

	// 1. Get Function
	var fn fsv1alpha1.Function
	if err := r.Get(ctx, req.NamespacedName, &fn); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Ensure Function has package label
	if fn.Labels == nil {
		fn.Labels = make(map[string]string)
	}
	labelUpdated := false
	if fn.Labels["package"] != fn.Spec.Package {
		fn.Labels["package"] = fn.Spec.Package
		labelUpdated = true
	}

	// 3. Get Package
	var pkg fsv1alpha1.Package
	if err := r.Get(ctx, types.NamespacedName{Name: fn.Spec.Package, Namespace: req.Namespace}, &pkg); err != nil {
		log.Error(err, "Failed to get Package", "package", fn.Spec.Package)
		return ctrl.Result{}, err
	}
	image := ""
	if pkg.Spec.FunctionType.Cloud != nil {
		image = pkg.Spec.FunctionType.Cloud.Image
	}
	if image == "" {
		return ctrl.Result{}, fmt.Errorf("package %s has no image", fn.Spec.Package)
	}

	// 4. Build config yaml content
	configYaml, err := buildFunctionConfigYaml(&fn, r.Config)
	if err != nil {
		log.Error(err, "Failed to marshal config yaml")
		return ctrl.Result{}, err
	}

	// 5. Build Deployment
	deployName := fmt.Sprintf("function-%s", fn.Name)
	var replicas int32 = 1
	labels := map[string]string{
		"function": fn.Name,
	}

	// Create init command to write config file
	initCommand := fmt.Sprintf(`cat > /config/config.yaml << 'EOF'
%s
EOF
`, configYaml)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: fn.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"function": fn.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:    "init-config",
						Image:   image,
						Command: []string{"/bin/sh", "-c", initCommand},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "function-config",
							MountPath: "/config",
						}},
					}},
					Containers: []corev1.Container{{
						Name:  "function",
						Image: image,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "function-config",
							MountPath: "/config",
						}},
						Env: []corev1.EnvVar{{
							Name:  "FS_CONFIG_PATH",
							Value: "/config/config.yaml",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "function-config",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					}},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(&fn, deployment, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// 6. Create or Update Deployment
	var existingDeploy appsv1.Deployment
	deployErr := r.Get(ctx, types.NamespacedName{Name: deployName, Namespace: fn.Namespace}, &existingDeploy)
	if deployErr == nil {
		// Only update if spec or labels changed
		if !reflect.DeepEqual(existingDeploy.Spec, deployment.Spec) ||
			!reflect.DeepEqual(existingDeploy.Labels, deployment.Labels) {
			existingDeploy.Spec = deployment.Spec
			existingDeploy.Labels = deployment.Labels
			err = r.Update(ctx, &existingDeploy)
			if err != nil {
				return utils.HandleReconcileError(log, err, "Conflict when updating Deployment, will retry automatically")
			}
		}
	} else if errors.IsNotFound(deployErr) {
		err = r.Create(ctx, deployment)
		if err != nil {
			return utils.HandleReconcileError(log, err, "Conflict when creating Deployment, will retry automatically")
		}
	} else {
		return ctrl.Result{}, deployErr
	}

	// 7. Update Function Status from Deployment Status
	if err := r.Get(ctx, types.NamespacedName{Name: deployName, Namespace: fn.Namespace}, &existingDeploy); err == nil {
		fn.Status = convertDeploymentStatusToFunctionStatus(&existingDeploy.Status)
		if err := r.Status().Update(ctx, &fn); err != nil {
			return utils.HandleReconcileError(log, err, "Conflict when updating Function status, will retry automatically")
		}
	}

	// 8. Update Function labels if needed
	if labelUpdated {
		// Re-fetch the Function to ensure we have the latest version
		var latestFn fsv1alpha1.Function
		if err := r.Get(ctx, req.NamespacedName, &latestFn); err != nil {
			log.Error(err, "Failed to get latest Function for label update")
			return ctrl.Result{}, err
		}
		// Apply our label changes to the latest version
		if latestFn.Labels == nil {
			latestFn.Labels = make(map[string]string)
		}
		latestFn.Labels["package"] = fn.Spec.Package
		if err := r.Update(ctx, &latestFn); err != nil {
			return utils.HandleReconcileError(log, err, "Conflict when updating Function labels, will retry automatically")
		}
	}

	return ctrl.Result{}, nil
}

// buildFunctionConfigYaml builds the config.yaml content for the function
func buildFunctionConfigYaml(fn *fsv1alpha1.Function, operatorCfg Config) (string, error) {
	cfg := map[string]interface{}{}

	// Inject pulsar config from operator config
	cfg["pulsar"] = map[string]interface{}{
		"serviceUrl": operatorCfg.PulsarServiceURL,
		"authPlugin": operatorCfg.PulsarAuthPlugin,
		"authParams": operatorCfg.PulsarAuthParams,
	}

	if len(fn.Spec.Sources) > 0 {
		cfg["sources"] = fn.Spec.Sources
	}
	if fn.Spec.RequestSource != nil {
		cfg["requestSource"] = fn.Spec.RequestSource
	}
	if fn.Spec.SubscriptionName != "" {
		cfg["subscriptionName"] = fn.Spec.SubscriptionName
	} else {
		cfg["subscriptionName"] = fmt.Sprintf("fs-%s", fn.Name)
	}
	if fn.Spec.Sink != nil {
		cfg["sink"] = fn.Spec.Sink
	}
	if fn.Spec.Module != "" {
		cfg["module"] = fn.Spec.Module
	}
	if fn.Spec.Config != nil {
		configMap := make(map[string]interface{})
		for k, v := range fn.Spec.Config {
			var r interface{}
			if err := json.Unmarshal(v.Raw, &r); err != nil {
				return "", fmt.Errorf("failed to unmarshal config value for key %s: %w", k, err)
			}
			configMap[k] = r
		}
		cfg["config"] = configMap
	}
	if fn.Spec.Description != "" {
		cfg["description"] = fn.Spec.Description
	}
	if fn.Spec.DisplayName != "" {
		cfg["displayName"] = fn.Spec.DisplayName
	}
	if fn.Spec.Package != "" {
		cfg["package"] = fn.Spec.Package
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// convertDeploymentStatusToFunctionStatus copies DeploymentStatus fields to FunctionStatus
func convertDeploymentStatusToFunctionStatus(ds *appsv1.DeploymentStatus) fsv1alpha1.FunctionStatus {
	return fsv1alpha1.FunctionStatus{
		AvailableReplicas:  ds.AvailableReplicas,
		ReadyReplicas:      ds.ReadyReplicas,
		Replicas:           ds.Replicas,
		UpdatedReplicas:    ds.UpdatedReplicas,
		ObservedGeneration: ds.ObservedGeneration,
	}
}

func hasFunctionLabel(obj client.Object) bool {
	labels := obj.GetLabels()
	_, ok := labels["function"]
	return ok
}

// SetupWithManager sets up the controller with the Manager.
func (r *FunctionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	functionLabelPredicate := predicate.NewPredicateFuncs(hasFunctionLabel)
	return ctrl.NewControllerManagedBy(mgr).
		For(&fsv1alpha1.Function{}).
		Owns(&appsv1.Deployment{}, builder.WithPredicates(functionLabelPredicate)).
		Watches(
			&fsv1alpha1.Package{},
			handler.EnqueueRequestsFromMapFunc(r.mapPackageToFunctions),
		).
		Named("function").
		Complete(r)
}

// mapPackageToFunctions maps Package changes to related Functions
func (r *FunctionReconciler) mapPackageToFunctions(ctx context.Context, obj client.Object) []reconcile.Request {
	packageObj, ok := obj.(*fsv1alpha1.Package)
	if !ok {
		return nil
	}

	// Get Functions that reference this Package using label selector
	var functions fsv1alpha1.FunctionList
	if err := r.List(ctx, &functions,
		client.InNamespace(packageObj.Namespace),
		client.MatchingLabels(map[string]string{"package": packageObj.Name})); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, function := range functions.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      function.Name,
				Namespace: function.Namespace,
			},
		})
	}

	return requests
}
