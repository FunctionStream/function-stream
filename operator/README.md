# operator

FunctionStream Operator is a Kubernetes operator designed to manage custom resources for serverless function orchestration and package management on Kubernetes clusters.

## Description

This project provides a Kubernetes operator that automates the lifecycle of custom resources such as Functions and Packages. It enables users to define, deploy, and manage serverless functions and their dependencies using Kubernetes-native APIs. The operator ensures that the desired state specified in custom resources is reflected in the actual cluster state, supporting extensibility and integration with cloud-native workflows.

## Deploying with Helm Chart

The recommended way to deploy the FunctionStream Operator is using the provided Helm chart. This method simplifies installation, upgrades, and configuration management.

### Prerequisites

- [Helm](https://helm.sh/) v3.0+
- Access to a Kubernetes v1.11.3+ cluster

### Installation

1. **Clone this repository (if using the local chart):**

   ```sh
   git clone https://github.com/FunctionStream/function-stream.git
   cd function-stream/operator
   ```

2. **Install the operator using Helm:**

   ```sh
   helm install fs ./deploy/chart \
     --namespace fs --create-namespace
   ```
   This will install the operator in the `fs` namespace with the release name `fs`.

3. **(Optional) Customize your deployment:**
   - You can override default values by editing `deploy/chart/values.yaml`, by providing your own values file, or by using the `--set` flag.
   - To use your own values file:
     ```sh
     helm install fs ./deploy/chart \
       --namespace fs --create-namespace \
       -f my-values.yaml
     ```
   - To override values from the command line:
     ```sh
     helm install fs ./deploy/chart \
       --namespace fs \
       --set controllerManager.replicas=2
     ```
   - For a full list of configurable options, see [`deploy/chart/values.yaml`](deploy/chart/values.yaml).

### Upgrading

To upgrade the operator after making changes or pulling a new chart version:

```sh
helm upgrade fs ./deploy/chart \
  --namespace fs
```

### Uninstallation

To uninstall the operator and all associated resources:

```sh
helm uninstall fs --namespace fs
```

> **Note:** By default, CRDs are retained after uninstall. You can control this behavior via the `crd.keep` value in `values.yaml`.

## Getting Started

### Prerequisites

- go version v1.23.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster

**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don't work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall

**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/operator:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/operator/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v1-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.


**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

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

