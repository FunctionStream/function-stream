# Function Stream Operator

FunctionStream Operator is a Kubernetes operator designed to manage custom resources for serverless function
orchestration and package management on Kubernetes clusters.

## 🚀 Get Started Now!

**New to FunctionStream Operator?** This step-by-step tutorial will guide you through everything you need to know.

## What is FunctionStream Operator?

This project provides a Kubernetes operator that automates the lifecycle of custom resources such as Functions and
Packages. It enables users to define, deploy, and manage serverless functions and their dependencies using
Kubernetes-native APIs. The operator ensures that the desired state specified in custom resources is reflected in the
actual cluster state, supporting extensibility and integration with cloud-native workflows.

## 📋 Prerequisites

Before you begin, ensure you have:

- [Helm](https://helm.sh/) v3.0+
- Access to a Kubernetes v1.19+ cluster
- `kubectl` configured to communicate with your cluster
- cert-manager (required for TLS certificates)

## 🛠️ Installation

The recommended way to deploy the FunctionStream Operator is using the provided Helm chart.

### 1. Install cert-manager

The FunctionStream Operator requires cert-manager for TLS certificates:

```sh
./scripts/install-cert-manager.sh
```

### 2. Deploy the Operator

**Option A: With Pulsar Standalone (Recommended for testing)**
```bash
helm install fs ./deploy/chart \
  --namespace fs --create-namespace \
  --set pulsar.standalone.enable=true
```

**Option B: With External Pulsar Cluster**
```bash
helm install fs ./deploy/chart \
  --namespace fs --create-namespace \
  --set pulsar.serviceUrl=pulsar://your-pulsar-cluster:6650
```

### 3. Verify Installation

```bash
kubectl get pods -n fs
kubectl get crd | grep functionstream
```

## 📖 Next Steps

<div align="center">

### 🎯 **Ready to deploy your first function?**

**[📖 Complete Tutorial](TUTORIAL.md)** - Your step-by-step guide to success!

</div>

This comprehensive tutorial will teach you how to:
- ✅ Create your first package and function
- ✅ Test your deployment with real examples
- ✅ Monitor and troubleshoot issues
- ✅ Understand advanced configurations
- ✅ Follow best practices

**Estimated time**: 15-20 minutes

## 📁 Examples

Ready-to-use examples are available:

- `examples/package.yaml` - Sample package definition
- `examples/function.yaml` - Sample function that uses the package

## 📚 Documentation

### Getting Started
- **[📖 Complete Tutorial](TUTORIAL.md)** - Step-by-step guide with detailed explanations

### Development
- **[🔧 Developer Guide](DEVELOPER.md)** - Information for contributors and developers

## Configuration

#### Pulsar Configuration

The chart supports two modes for Pulsar:

##### 1. Pulsar Standalone Mode

When `pulsar.standalone.enable=true`, the chart will:

- Deploy a Pulsar standalone StatefulSet in the same namespace
- Create persistent storage for Pulsar data and logs
- Expose Pulsar service on ports 6650 (Pulsar) and 8080 (Admin)
- Automatically configure the operator to connect to the standalone Pulsar

```yaml
pulsar:
  standalone:
    enable: true
    image:
      repository: apachepulsar/pulsar
      tag: "3.4.0"
    resources:
      limits:
        cpu: 1000m
        memory: 2Gi
      requests:
        cpu: 500m
        memory: 1Gi
    storage:
      size: 10Gi
      storageClass: ""  # Use default storage class if empty
    service:
      type: ClusterIP
      ports:
        pulsar: 6650
        admin: 8080
```

##### 2. External Pulsar Mode

When `pulsar.standalone.enable=false` (default), you can specify an external Pulsar cluster:

```yaml
pulsar:
  serviceUrl: pulsar://your-pulsar-cluster:6650
  authPlugin: ""  # Optional: Pulsar authentication plugin
  authParams: ""  # Optional: Pulsar authentication parameters
```

#### Manager Configuration

```yaml
controllerManager:
  replicas: 1
  container:
    image:
      repository: functionstream/operator
      tag: latest
    resources:
      limits:
        cpu: 500m
        memory: 128Mi
      requests:
        cpu: 10m
        memory: 64Mi
```

#### Other Features

- **RBAC**: Enable/disable RBAC permissions
- **CRDs**: Control CRD installation and retention
- **Metrics**: Enable metrics export
- **Webhooks**: Enable admission webhooks
- **Prometheus**: Enable ServiceMonitor for Prometheus
- **Cert-Manager**: Enable cert-manager integration
- **Network Policies**: Enable network policies

### Accessing Pulsar

#### When Using Pulsar Standalone

The Pulsar standalone service is exposed as:

- **Pulsar Service**: `pulsar-standalone:6650`
- **Admin Interface**: `pulsar-standalone:8080`

You can access the admin interface by port-forwarding:

```bash
kubectl port-forward svc/pulsar-standalone 8080:8080
```

Then visit `http://localhost:8080` in your browser.

#### Pulsar Client Configuration

When using Pulsar standalone, your Pulsar clients should connect to:

```
pulsar://pulsar-standalone:6650
```

### Storage

When Pulsar standalone is enabled, the chart creates two PersistentVolumeClaims:

- `pulsar-data`: For Pulsar data storage
- `pulsar-logs`: For Pulsar logs storage

Both use the same storage size and storage class configuration.

### Troubleshooting

#### Certificate Mounting Issues

If you encounter errors like:

```
Warning  FailedMount  95s (x9 over 3m43s)  kubelet  MountVolume.SetUp failed for volume "metrics-certs" : secret "metrics-server-cert" not found
Warning  FailedMount  95s (x9 over 3m43s)  kubelet  MountVolume.SetUp failed for volume "webhook-cert" : secret "webhook-server-cert" not found
```

This happens because cert-manager is not installed or not running in your cluster. The operator requires cert-manager to
create TLS certificates for webhooks and metrics.

**Solution:**

1. **Verify cert-manager installation:**
   ```bash
   kubectl get pods -n cert-manager
   ```
   All cert-manager pods should be in `Running` status.

2. **Check cert-manager namespace exists:**
   ```bash
   kubectl get namespace cert-manager
   ```

3. **If cert-manager is not installed, install it:**
   ```bash
   # Using the provided script
   chmod +x scripts/install-cert-manager.sh
   ./scripts/install-cert-manager.sh
   
   # Or manually
   kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
   ```

4. **Wait for cert-manager to be ready:**
   ```bash
   kubectl wait --for=jsonpath='{.status.phase}=Running' pods -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=300s
   ```

5. **Reinstall the operator after cert-manager is ready:**
   ```bash
   helm uninstall fs -n fs
   helm install fs ./deploy/chart --namespace fs --create-namespace
   ```

#### Check cert-manager Status

To verify that cert-manager is working correctly:

```bash
# Check cert-manager pods
kubectl get pods -n cert-manager

# Check cert-manager CRDs
kubectl get crd | grep cert-manager

# Check cert-manager logs
kubectl logs -n cert-manager -l app.kubernetes.io/name=cert-manager
```

### Upgrading

To upgrade the operator after making changes or pulling a new chart version:

```sh
helm upgrade fs ./deploy/chart \
  --namespace fs
```

### Uninstallation

To uninstall the operator and all associated resources:

```bash
helm uninstall fs -n fs
```

**Note**:

- By default, CRDs are deleted during uninstall. If you want to retain CRDs after uninstall, set `crd.keep: true` in
  your values file. Be aware that retaining CRDs will also prevent the deletion of any custom resources (Functions,
  Packages, etc.) that depend on these CRDs.
- If you enabled Pulsar standalone, the persistent volumes will remain unless you manually delete them.

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