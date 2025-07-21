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
> privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

> **NOTE**: Ensure that the samples has default values to test it out.

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

### CRD Updates

When CRD definitions are updated in `operator/config/crd/bases/`, you need to ensure the Helm chart CRD templates are also updated to match. The Helm chart CRD templates are located in `operator/deploy/chart/templates/crd/`.

To update the Helm chart CRD templates:

1. Update the CRD definitions in `operator/config/crd/bases/`
2. Manually update the corresponding files in `operator/deploy/chart/templates/crd/` to match the base definitions
3. Ensure any Helm-specific templating (like `{{- if .Values.crd.enable }}`) is preserved
4. Test the changes to ensure the CRDs work correctly

**Important:** The Helm chart CRD templates should always reflect the latest changes from the base CRD definitions to ensure consistency between direct CRD installation and Helm chart installation.

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)