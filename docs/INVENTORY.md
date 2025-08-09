# Cluster Inventory Feature

The `inventory` command provides a focused view of what's running in your Kubernetes cluster, showing applications, their versions, and how they're managed.

## Quick Start

```bash
# Show all user applications (excludes system namespaces by default)
kubitor inventory

# Include system namespaces
kubitor inventory --include-system

# Filter to specific namespace
kubitor inventory -n production

# Output as JSON or YAML
kubitor inventory --format json
kubitor inventory --format yaml

# Group by different criteria
kubitor inventory --group-by namespace
kubitor inventory --group-by kind
kubitor inventory --group-by management  # default
```

## What It Shows

The inventory command collects and displays:

- **Cluster Information**: Name, Kubernetes version, node count
- **Applications**: All deployments, statefulsets, daemonsets, jobs, and standalone pods
- **Version Information**: Application versions from labels
- **Management Detection**: How each resource is deployed and managed
- **Resource Status**: Replica counts and current state

## Management Detection

The inventory automatically detects how resources are managed:

- **📦 Helm**: Resources deployed via Helm charts
- **🔄 ArgoCD**: GitOps deployments via ArgoCD
- **⚡ Flux**: GitOps deployments via Flux
- **🤖 Operator**: Resources managed by Kubernetes operators
- **🔧 Kustomize**: Resources deployed via Kustomize
- **🏗️ Terraform**: Resources deployed via Terraform
- **✋ Manual**: Directly applied resources
- **❓ Unknown**: Management tool couldn't be determined

## System Resource Filtering

By default, the inventory excludes system namespaces and resources to focus on user applications:

### Excluded Namespaces
- `kube-system`, `kube-public`, `kube-node-lease`
- Cloud provider namespaces: `eks-*`, `aks-*`, `gke-*`
- Service mesh: `istio-system`, `linkerd`
- Monitoring: `prometheus-system`, `monitoring`, `logging`
- GitOps tools: `flux-system`, `argocd`
- And more...

Use `--include-system` to see everything.

## Output Formats

### Table Format (Default)
```
CLUSTER INVENTORY
================================================================================
Cluster: production-eks
Kubernetes Version: v1.28.5
Nodes: 5
Applications: 12

📦 Helm Managed (5)
┌─────────────────┬───────────┬────────────┬─────────┬──────────┬──────────────────┐
│ Name            │ Namespace │ Kind       │ Version │ Replicas │ Management       │
├─────────────────┼───────────┼────────────┼─────────┼──────────┼──────────────────┤
│ nginx-ingress   │ ingress   │ Deployment │ 4.7.1   │ 2        │ Helm:nginx       │
│ prometheus      │ monitoring│ StatefulSet│ 15.0.0  │ 1        │ Helm:prometheus  │
└─────────────────┴───────────┴────────────┴─────────┴──────────┴──────────────────┘
```

### JSON Format
```json
{
  "cluster": "production-eks",
  "kubernetes_version": "v1.28.5",
  "node_count": 5,
  "total_applications": 12,
  "applications": [
    {
      "name": "nginx-ingress",
      "namespace": "ingress",
      "kind": "Deployment",
      "version": "4.7.1",
      "management": "Helm",
      "replicas": 2,
      "images": ["nginx:1.21.6"]
    }
  ]
}
```

### YAML Format
```yaml
cluster: production-eks
kubernetes_version: v1.28.5
node_count: 5
total_applications: 12
applications:
  - name: nginx-ingress
    namespace: ingress
    kind: Deployment
    version: 4.7.1
    management: Helm
    replicas: 2
    images:
      - nginx:1.21.6
```

## Use Cases

1. **Quick Cluster Overview**: Get a high-level view of what's deployed
2. **Audit Management Tools**: See what percentage of resources are managed by each tool
3. **Version Tracking**: Identify application versions across namespaces
4. **Migration Planning**: Understand current deployment methods before migrating
5. **Troubleshooting**: Quickly identify unmanaged or misconfigured resources

## Integration with Other Commands

The inventory command complements other Kubitor features:

- Use `kubitor scan` for detailed resource extraction
- Use `kubitor report` for comprehensive cluster analysis
- Use `kubitor history` to track changes over time

## Examples

```bash
# Get inventory for production cluster
kubitor inventory --context production-eks

# Show only ArgoCD managed applications in production namespace
kubitor inventory -n production | grep ArgoCD

# Export inventory to JSON for processing
kubitor inventory --format json > cluster-inventory.json

# Compare inventories between clusters
kubitor inventory --context cluster1 --format json > cluster1.json
kubitor inventory --context cluster2 --format json > cluster2.json
diff cluster1.json cluster2.json
```