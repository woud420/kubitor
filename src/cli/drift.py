"""Drift analysis between IaC and running cluster."""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.tree import Tree
import typer

console = Console()


class DriftAnalyzer:
    """Analyze drift between IaC definitions and cluster reality."""

    def __init__(self, iac_path: str, cluster_output_path: str, hide_system: bool = False):
        self.iac_path = Path(iac_path)
        self.cluster_path = Path(cluster_output_path)
        self.hide_system = hide_system

        # EKS system resources to filter out
        self.eks_system_resources = {
            # System namespaces
            "kube-system",
            "kube-public",
            "kube-node-lease",
            # AWS/EKS specific resources
            "aws-node",
            "aws-vpc-cni",
            "metrics-server",
            "coredns",
            "kube-proxy",
            "amazon-network-policy-controller-k8s",
            "vpc-resource-controller",
            # System resource patterns
            "system:",
            "eks:",
            "aws-",
            "kube-",
            "cluster-admin",
            "admin",
        }

    def load_iac_resources(self) -> Dict[str, Dict[str, Any]]:
        """Load IaC resource definitions."""
        resources = {}

        # Load base resources
        base_path = self.iac_path / "base"
        if base_path.exists():
            resources.update(self._load_yaml_files(base_path))

        return resources

    def load_cluster_resources(self) -> Dict[str, Dict[str, Any]]:
        """Load cluster resource exports."""
        resources = {}

        # Load all YAML files from cluster export
        if self.cluster_path.exists():
            resources.update(self._load_yaml_files(self.cluster_path))

        return resources

    def _load_yaml_files(self, path: Path) -> Dict[str, Dict[str, Any]]:
        """Recursively load YAML files from a directory."""
        resources = {}

        for yaml_file in path.rglob("*.yaml"):
            if yaml_file.name in ["kustomization.yaml", "kustomization.yml"]:
                continue

            try:
                with open(yaml_file) as f:
                    docs = list(yaml.safe_load_all(f))
                    for doc in docs:
                        if doc and isinstance(doc, dict):
                            key = self._resource_key(doc)
                            if key:
                                resources[key] = doc
            except Exception as e:
                console.print(f"[red]Error loading {yaml_file}: {e}[/red]")

        return resources

    def _resource_key(self, resource: Dict[str, Any]) -> Optional[str]:
        """Generate unique key for a resource."""
        try:
            kind = resource.get("kind", "")
            name = resource.get("metadata", {}).get("name", "")
            namespace = resource.get("metadata", {}).get("namespace", "default")

            if kind and name:
                return f"{kind}/{namespace}/{name}"
        except:
            pass
        return None

    def _is_system_resource(self, resource: Dict[str, Any]) -> bool:
        """Check if resource is an EKS/K8s system resource."""
        if not self.hide_system:
            return False

        # Check namespace
        namespace = resource.get("metadata", {}).get("namespace", "")
        if namespace in self.eks_system_resources:
            return True

        # Check resource name
        name = resource.get("metadata", {}).get("name", "")
        for pattern in self.eks_system_resources:
            if pattern in name.lower():
                return True

        # Check labels for system components
        labels = resource.get("metadata", {}).get("labels", {})
        for label_key, label_value in labels.items():
            label_combined = f"{label_key}:{label_value}".lower()
            for pattern in self.eks_system_resources:
                if pattern in label_combined:
                    return True

        # Check kind for system resources
        kind = resource.get("kind", "")
        system_kinds = {
            "Node",
            "ComponentStatus",
            "APIService",
            "ValidatingWebhookConfiguration",
            "MutatingWebhookConfiguration",
            "CSIDriver",
            "CSINode",
            "StorageClass",
            "PriorityClass",
            "FlowSchema",
            "PriorityLevelConfiguration",
        }
        if kind in system_kinds:
            return True

        return False

    def _clean_resource_for_comparison(self, resource: Dict[str, Any]) -> Dict[str, Any]:
        """Clean resource for fair comparison."""
        cleaned = resource.copy()

        # Remove runtime-only fields
        metadata = cleaned.get("metadata", {})
        metadata.pop("resourceVersion", None)
        metadata.pop("uid", None)
        metadata.pop("generation", None)
        metadata.pop("creationTimestamp", None)
        metadata.pop("managedFields", None)

        # Remove runtime annotations
        annotations = metadata.get("annotations", {})
        runtime_annotations = [
            "deployment.kubernetes.io/revision",
            "kubectl.kubernetes.io/last-applied-configuration",
            "kubectl.kubernetes.io/restartedAt",
        ]
        for ann in runtime_annotations:
            annotations.pop(ann, None)

        if not annotations:
            metadata.pop("annotations", None)

        # Remove status unless it's a namespace
        if cleaned.get("kind") != "Namespace":
            cleaned.pop("status", None)

        # Remove defaults that Kubernetes adds
        spec = cleaned.get("spec", {})
        if "progressDeadlineSeconds" in spec and spec["progressDeadlineSeconds"] == 600:
            spec.pop("progressDeadlineSeconds")
        if "revisionHistoryLimit" in spec and spec["revisionHistoryLimit"] == 10:
            spec.pop("revisionHistoryLimit")

        return cleaned

    def analyze_drift(self) -> Dict[str, Any]:
        """Analyze drift between IaC and cluster."""
        iac_resources = self.load_iac_resources()
        cluster_resources = self.load_cluster_resources()

        # Clean resources for comparison
        iac_clean = {k: self._clean_resource_for_comparison(v) for k, v in iac_resources.items()}
        cluster_clean = {
            k: self._clean_resource_for_comparison(v) for k, v in cluster_resources.items()
        }

        drift_analysis = {
            "only_in_iac": [],
            "only_in_cluster": [],
            "configuration_drift": [],
            "matches": [],
        }

        # Find resources only in IaC
        for key in iac_clean:
            if key not in cluster_clean:
                drift_analysis["only_in_iac"].append({"key": key, "resource": iac_resources[key]})

        # Find resources only in cluster (filter system resources if requested)
        for key in cluster_clean:
            if key not in iac_clean:
                resource = cluster_resources[key]
                if not self._is_system_resource(resource):
                    drift_analysis["only_in_cluster"].append({"key": key, "resource": resource})

        # Find configuration drift
        for key in iac_clean:
            if key in cluster_clean:
                if iac_clean[key] != cluster_clean[key]:
                    drift_analysis["configuration_drift"].append(
                        {
                            "key": key,
                            "iac": iac_resources[key],
                            "cluster": cluster_resources[key],
                            "differences": self._find_differences(
                                iac_clean[key], cluster_clean[key]
                            ),
                        }
                    )
                else:
                    drift_analysis["matches"].append(key)

        return drift_analysis

    def _find_differences(
        self, iac: Dict[str, Any], cluster: Dict[str, Any], path: str = ""
    ) -> List[str]:
        """Find specific differences between two resources."""
        differences = []

        def compare_dicts(d1, d2, current_path):
            all_keys = set(d1.keys()) | set(d2.keys())

            for key in all_keys:
                key_path = f"{current_path}.{key}" if current_path else key

                if key not in d1:
                    differences.append(f"+ {key_path}: {d2[key]} (only in cluster)")
                elif key not in d2:
                    differences.append(f"- {key_path}: {d1[key]} (only in IaC)")
                elif d1[key] != d2[key]:
                    if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                        compare_dicts(d1[key], d2[key], key_path)
                    else:
                        differences.append(f"~ {key_path}: {d1[key]} → {d2[key]}")

        compare_dicts(iac, cluster, path)
        return differences

    def print_drift_report(self):
        """Print a comprehensive drift report."""
        drift = self.analyze_drift()

        console.print("\n")
        console.print(Panel.fit("🔍 INFRASTRUCTURE DRIFT ANALYSIS", style="bold blue"))

        # Summary statistics
        summary_table = Table(title="Drift Summary", show_header=True)
        summary_table.add_column("Category", style="cyan")
        summary_table.add_column("Count", style="green")
        summary_table.add_column("Description", style="white")

        filter_note = " (system resources filtered)" if self.hide_system else ""

        summary_table.add_row(
            "Matches", str(len(drift["matches"])), "Resources identical in IaC and cluster"
        )
        summary_table.add_row(
            "Config Drift",
            str(len(drift["configuration_drift"])),
            "Resources with configuration differences",
        )
        summary_table.add_row(
            "Only in IaC", str(len(drift["only_in_iac"])), "Defined in IaC but not running"
        )
        summary_table.add_row(
            "Only in Cluster",
            str(len(drift["only_in_cluster"])),
            f"Running but not in IaC{filter_note}",
        )

        console.print(summary_table)
        console.print("\n")

        # Resources only in IaC (missing from cluster)
        if drift["only_in_iac"]:
            console.print(
                Panel("📋 Resources Defined in IaC but Missing from Cluster", style="yellow")
            )
            for item in drift["only_in_iac"]:
                console.print(f"[red]✗[/red] {item['key']}")
                console.print(f"  Reason: Likely removed by AWS overlay patches or not deployed")

        # Resources only in cluster
        if drift["only_in_cluster"]:
            console.print(
                Panel("🏃 Resources Running in Cluster but Not in IaC", style="bright_yellow")
            )
            cluster_tree = Tree("Cluster-Only Resources")

            # Group by category
            by_category = {}
            for item in drift["only_in_cluster"]:
                kind = item["resource"].get("kind", "Unknown")
                namespace = item["resource"].get("metadata", {}).get("namespace", "cluster-scoped")
                category = f"{kind}s"

                if category not in by_category:
                    by_category[category] = []
                by_category[category].append(f"{item['key']} ({namespace})")

            for category, resources in by_category.items():
                category_node = cluster_tree.add(f"[cyan]{category}[/cyan] ({len(resources)})")
                for resource in resources[:5]:  # Show first 5
                    category_node.add(f"[white]{resource}[/white]")
                if len(resources) > 5:
                    category_node.add(f"[dim]... and {len(resources) - 5} more[/dim]")

            console.print(cluster_tree)

        # Configuration drift
        if drift["configuration_drift"]:
            console.print(Panel("⚠️  Configuration Drift Detected", style="red"))

            for item in drift["configuration_drift"]:
                console.print(f"\n[bold yellow]{item['key']}[/bold yellow]")

                # Show key differences
                differences = item["differences"][:10]  # Show first 10 differences
                for diff in differences:
                    if diff.startswith("+"):
                        console.print(f"  [green]{diff}[/green]")
                    elif diff.startswith("-"):
                        console.print(f"  [red]{diff}[/red]")
                    elif diff.startswith("~"):
                        console.print(f"  [yellow]{diff}[/yellow]")

                if len(item["differences"]) > 10:
                    console.print(
                        f"  [dim]... and {len(item['differences']) - 10} more differences[/dim]"
                    )


def analyze_drift(
    iac_path: str = typer.Argument(
        ..., help="Path to IaC directory (e.g., ~/workspace/syntin/infra/k8s)"
    ),
    cluster_path: str = typer.Argument(
        ..., help="Path to cluster export directory (e.g., ./k8s-resources)"
    ),
    hide_system: bool = typer.Option(
        False, "--hide-system", help="Hide EKS/K8s system resources from analysis"
    ),
):
    """Analyze drift between Infrastructure as Code and running cluster."""

    analyzer = DriftAnalyzer(iac_path, cluster_path, hide_system=hide_system)
    analyzer.print_drift_report()


if __name__ == "__main__":
    typer.run(analyze_drift)
