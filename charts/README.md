# Function Stream Helm Chart

This is the helm chart for installing FunctionStream on kubernetes.

## Install

1. Register the CRDs with the Kubernetes cluster.

    ```bash
    kubectl apply -f ./crds
    ```

2. Create a Kubernetes namespace.

    ```bash
    kubectl create namespace <k8s-namespace>
    ```
3. Install the FunctionStream.

    ```bash
    helm install function-stream . -n <k8s-namespace> --values {Your values yaml file}
    ```

4. Verify that the Function Stream is installed successfully.

    ```bash
    kubectl get pods -n <k8s-namespace>
    ```

    Expected outputs:
    ```
    NAME                                                READY   STATUS    RESTARTS   AGE
    function-mesh-controller-manager-7968cb458b-zscpt   1/1     Running   0          9m3s
    pulsar-standalone-0                                 1/1     Running   0          9m3s
    ```

## Uninstall

* Use the following command to uninstall FunctionMesh operator.

    ```bash
    helm uninstall function-stream -n <k8s-namespace>
    ```
