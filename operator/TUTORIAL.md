# FunctionStream Operator Tutorial

Welcome to the FunctionStream Operator tutorial! This guide will walk you through creating and deploying your first serverless function using the FunctionStream Operator on Kubernetes.

## Overview

FunctionStream Operator is a Kubernetes operator that manages custom resources for serverless function orchestration and package management. In this tutorial, you'll learn how to:

- Deploy a Package resource that defines a reusable function module
- Deploy a Function resource that instantiates the package
- Monitor and manage your deployed functions
- Understand the architecture and components

## Prerequisites

Before you begin, ensure you have:

- A Kubernetes cluster (v1.19+) with kubectl configured
- FunctionStream Operator installed (see [Installation Guide](README.md))
- Basic understanding of Kubernetes concepts

Follow the [Installation Guide](README.md) to set up the FunctionStream Operator if you haven't done so already.

## Step 1: Verify Installation

First, let's verify that the FunctionStream Operator is properly installed:

```bash
# Check if the operator namespace exists
kubectl get namespace fs

# Verify operator pods are running
kubectl get pods -n fs

# Check that Custom Resource Definitions are installed
kubectl get crd | grep functionstream
```

Expected output:
```
NAME                                          READY   STATUS    RESTARTS   AGE
fs-pulsar-standalone-0                        1/1     Running   1          21h
operator-controller-manager-c99489d8b-zk78h   1/1     Running   0          21h

NAME                                    CREATED AT
functions.fs.functionstream.github.io   2025-06-23T14:53:30Z
packages.fs.functionstream.github.io    2025-06-23T14:53:30Z
```

## Step 2: Create Your First Package

A Package defines a reusable function module with its container image and available functions. Let's create a simple "current time" package:

```yaml
# examples/package.yaml
apiVersion: fs.functionstream.github.io/v1alpha1
kind: Package
metadata:
  name: current-time
spec:
  displayName: Get Current Time
  logo: ""
  description: "A function for getting the current time."
  functionType:
    cloud:
      image: "functionstream/time-function:latest"
  modules:
    getCurrentTime:
      displayName: Get Current Time
      description: "A tool that returns the current time."
```

### Package Components Explained

- **`displayName`**: Human-readable name for the package
- **`description`**: Detailed description of what the package does
- **`functionType.cloud.image`**: Docker image containing the function code
- **`modules`**: Available functions within the package
  - Each module has a unique key (e.g., `getCurrentTime`)
  - Modules can have their own display names and descriptions

### Deploy the Package

```bash
kubectl apply -f examples/package.yaml
```

Verify the package was created:

```bash
kubectl get packages
kubectl describe package current-time
```

Expected output:
```
NAME           AGE
current-time   21h

Name:         current-time
Namespace:    default
Spec:
  Description:   A function for getting the current time.
  Display Name:  Get Current Time
  Function Type:
    Cloud:
      Image:  functionstream/time-function:latest
  Modules:
    Get Current Time:
      Description:   A tool that returns the current time.
      Display Name:  Get Current Time
```

## Step 3: Create Your First Function

A Function instantiates a package with specific configuration and request sources. Let's create a function that uses our current-time package:

```yaml
# examples/function.yaml
apiVersion: fs.functionstream.github.io/v1alpha1
kind: Function
metadata:
   name: current-time-function
spec:
   displayName: Get Current Time Function
   package: current-time
   module: getCurrentTime
   requestSource: # RPC
      pulsar:
         topic: request_current_time
   source:
      pulsar:
         topic: current_time_source
   sink:
      pulsar:
         topic: current_time_sink
```

### Function Components Explained

- **`package`**: References the package name to instantiate
- **`module`**: Specifies which module from the package to use
- **`requestSource.pulsar.topic`**: Pulsar topic that triggers the function
- **`displayName`**: Human-readable name for the function instance

### Deploy the Function

```bash
kubectl apply -f examples/function.yaml
```

Verify the function was created:

```bash
kubectl get functions
kubectl describe function current-time-function
```

Expected output:
```
NAME                    AGE
current-time-function   21h

Name:         current-time-function
Namespace:    default
Labels:       package=current-time
Spec:
  Display Name:  Get Current Time Function
  Module:        getCurrentTime
  Package:       current-time
  Request Source:
    Pulsar:
      Topic:  request_current_time
Status:
  Available Replicas:   1
  Ready Replicas:       1
  Replicas:             1
  Updated Replicas:     1
```

## Step 4: Monitor Function Deployment

The operator automatically creates Kubernetes resources to run your function. Let's check what was created:

```bash
# Check the function pod
kubectl get pods -l function=current-time-function

# Check the deployment
kubectl get deployments -l function=current-time-function
```

Expected output:
```
NAME                                          READY   STATUS    RESTARTS   AGE
function-current-time-function-b8b89f856-brvx7   1/1     Running   0          21h

NAME                                    READY   UP-TO-DATE   AVAILABLE   AGE
function-current-time-function          1/1     1            1           21h
```

## Step 5: Test Your Function

Now let's test the function by sending a message to the Pulsar topic. First, let's access Pulsar:

```bash
# Port forward Pulsar service
kubectl port-forward svc/fs-pulsar-standalone 6650:6650 -n fs &
kubectl port-forward svc/fs-pulsar-standalone 8080:8080 -n fs &
```

### Using Pulsar Admin Interface

1. Open your browser and navigate to `http://localhost:8080`
2. You'll see the Pulsar admin interface
3. Navigate to "Topics" to see the `request_current_time` topic

### Using Pulsar Client

You can test the function by shelling into the Pulsar standalone pod:

```bash
# Shell into the Pulsar standalone pod
kubectl exec -it fs-pulsar-standalone-0 -n fs -- bash
```

**1. Start a consumer in a separate terminal window**

Open a new terminal window and shell into the Pulsar pod:

```bash
kubectl exec -it fs-pulsar-standalone-0 -n fs -- bash
```

Then start consuming messages from the function output topic:

```bash
# Start consuming messages from the function output topic
pulsar-client consume current_time_sink -s "test-subscription"
```

This will start listening for messages from the function's output topic.

**2. Send a test message in another terminal window**

In your original terminal window (or another terminal), shell into the Pulsar pod and send a test message:

```bash
kubectl exec -it fs-pulsar-standalone-0 -n fs -- bash
```

Then send a test message to trigger the function:

```bash
# Send a test message to trigger the function
pulsar-client produce request_current_time -m "{}"
```

You should see the function process the message and output the current time to the `current_time_sink` topic, which will appear in your consumer window.

```
publishTime:[1750775397910], eventTime:[1750775397907], key:[null], properties:[], content:{"result": "The current time is 2025-06-24 14:29:57 ."}
```

## Step 6: Cleanup

When you're done testing, clean up the resources:

```bash
# Delete the function
kubectl delete function current-time-function

# Delete the package
kubectl delete package current-time

# Verify cleanup
kubectl get packages
kubectl get functions
kubectl get pods -l function=current-time-function
```

## Troubleshooting

### Common Issues

1. **Package Not Found**
   ```
   Error: package "current-time" not found
   ```
   **Solution**: Ensure the package is created before the function

2. **Image Pull Errors**
   ```
   Error: ImagePullBackOff
   ```
   **Solution**: Check if the container image exists and is accessible

3. **Pulsar Connection Issues**
   ```
   Error: Failed to connect to Pulsar
   ```
   **Solution**: Verify Pulsar is running and accessible

### Debug Commands

```bash
# Check operator logs
kubectl logs -n fs -l app.kubernetes.io/name=operator

# Check function pod events
kubectl describe pod -l function=current-time-function
```

Congratulations! You've successfully deployed your first serverless function using FunctionStream Operator. The operator handled all the complexity of managing Kubernetes resources, scaling, and integration with Pulsar, allowing you to focus on your function logic. 