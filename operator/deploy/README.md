# FunctionStream Operator Helm Chart

This Helm chart deploys the FunctionStream Operator, including its Custom Resource Definitions (CRDs) and manager deployment. The chart is highly configurable to fit a variety of Kubernetes environments.

## Features
- Installs all required CRDs
- Deploys the operator manager
- Configurable image, resources, RBAC, service account, metrics, network policy, and Prometheus monitoring

## Prerequisites
- Kubernetes 1.16+
- Helm 3.0+

## Installation

```sh
helm repo add functionstream https://functionstream.github.io/charts
helm install fs-operator functionstream/functionstream-operator
```

Or from local directory:

```sh
helm install fs-operator ./deploy
```

## Configuration

The following table lists the configurable parameters of the chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Operator image repository | `functionstream/operator` |
| `image.tag` | Operator image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `replicaCount` | Number of manager replicas | `1` |
| `serviceAccount.create` | Create service account | `true` |
| `serviceAccount.name` | Service account name | `""` |
| `rbac.create` | Create RBAC resources | `true` |
| `resources` | Resource requests/limits | `{}` |
| `affinity` | Pod affinity | `{}` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `metrics.enabled` | Enable metrics service | `true` |
| `metrics.service.type` | Metrics service type | `ClusterIP` |
| `metrics.service.port` | Metrics service port | `8080` |
| `networkPolicy.enabled` | Enable network policy | `true` |
| `prometheus.enabled` | Enable Prometheus monitoring | `true` |
| `prometheus.serviceMonitor.enabled` | Create ServiceMonitor | `true` |
| `prometheus.serviceMonitor.interval` | Scrape interval | `30s` |
| `prometheus.serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |

You can override any of these values using `--set` or by editing `values.yaml`.

## CRDs

The chart will install all required CRDs automatically from the `crds/` directory. If you wish to manage CRDs separately, set `--skip-crds` when installing the chart.

## Uninstallation

```sh
helm uninstall fs-operator
```

## Example

```sh
helm install fs-operator ./deploy \
  --set image.tag=v0.2.0 \
  --set replicaCount=2 \
  --set prometheus.enabled=false
```

## License
Apache 2.0 