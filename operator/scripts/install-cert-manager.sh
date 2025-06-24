#!/bin/bash

# FunctionStream Operator - cert-manager installation script
# This script installs cert-manager which is required for the operator to work properly

set -e

echo "FunctionStream Operator - cert-manager installation script"
echo "=========================================================="

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed or not in PATH"
    exit 1
fi

# Check if we can connect to the cluster
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster"
    exit 1
fi

echo "Installing cert-manager..."

# Install cert-manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml

echo "Waiting for cert-manager to be ready..."

# Wait for cert-manager namespace to be created
kubectl wait --for=jsonpath='{.status.phase}=Active' namespace/cert-manager --timeout=60s

# Wait for cert-manager pods to be ready
kubectl wait --for=jsonpath='{.status.phase}=Running' pods -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=300s


echo "cert-manager installation completed successfully!"
echo ""
echo "You can now install the FunctionStream operator:"
echo "  helm install fs ./deploy/chart"
echo ""
echo "Or if you want to install with Pulsar standalone:"
echo "  helm install fs ./deploy/chart --set pulsar.standalone.enable=true"