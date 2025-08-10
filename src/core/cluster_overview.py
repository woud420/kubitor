"""Cluster overview scanner for high-level cluster information."""

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict

from ..k8s import K8sClient
from ..model.overview import (
    ClusterOverviewData,
    CronJobOverview,
    DaemonSetOverview,
    DeploymentOverview,
    JobOverview,
    NamespaceOverview,
    PodStatus,
    ResourceCounts,
    StatefulSetOverview,
    WorkloadSummary,
)
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ClusterOverview:
    """Provides high-level overview of a Kubernetes cluster."""

    def __init__(self, client: K8sClient):
        self.client = client

    def get_overview(self) -> ClusterOverviewData:
        """Get comprehensive cluster overview."""
        logger.info("Collecting cluster overview information")

        # Get cluster info
        cluster_info = self.client.get_cluster_info()

        # Build overview data
        overview = ClusterOverviewData(
            cluster_version=(
                cluster_info.server_version.git_version
                if cluster_info.server_version
                else "Unknown"
            ),
            platform=(
                cluster_info.server_version.platform
                if cluster_info.server_version
                else "Unknown"
            ),
            node_count=len(cluster_info.nodes),
            nodes=[self._node_to_dict(node) for node in cluster_info.nodes],
        )

        # Populate namespaces
        overview.namespaces = self._get_namespace_overviews()
        overview.namespace_count = len(overview.namespaces)

        # Populate workloads
        overview.workloads = self._get_workload_summary()

        # Populate resource counts
        overview.resource_counts = self._get_resource_counts()

        return overview

    def _node_to_dict(self, node) -> Dict[str, Any]:
        """Convert node info to dictionary."""
        return {
            "name": node.name,
            "status": node.status,
            "roles": node.roles,
            "version": node.version,
            "os": node.os,
            "container_runtime": node.container_runtime,
        }

    def _get_namespace_overviews(self) -> list[NamespaceOverview]:
        """Get namespace information with pod counts."""
        namespaces_data = self.client.get_json("namespaces")
        if not namespaces_data or "items" not in namespaces_data:
            return []

        namespace_overviews = []

        # Get pods for all namespaces
        pods_data = self.client.get_json("pods", all_namespaces=True)
        pods_by_namespace = defaultdict(list)

        if pods_data and "items" in pods_data:
            for pod in pods_data["items"]:
                ns = pod["metadata"]["namespace"]
                pods_by_namespace[ns].append(pod)

        # Process each namespace
        for ns in namespaces_data["items"]:
            ns_name = ns["metadata"]["name"]
            ns_pods = pods_by_namespace.get(ns_name, [])

            # Count pods by status
            pod_status = PodStatus()
            for pod in ns_pods:
                phase = pod.get("status", {}).get("phase", "Unknown")
                phase_lower = phase.lower()

                if phase_lower == "running":
                    pod_status.running += 1
                elif phase_lower == "pending":
                    pod_status.pending += 1
                elif phase_lower == "succeeded":
                    pod_status.succeeded += 1
                elif phase_lower == "failed":
                    pod_status.failed += 1
                else:
                    pod_status.unknown += 1

            namespace_overview = NamespaceOverview(
                name=ns_name,
                status=ns["status"]["phase"],
                pod_count=len(ns_pods),
                pod_status=pod_status,
                labels=ns["metadata"].get("labels", {}),
            )
            namespace_overviews.append(namespace_overview)

        return namespace_overviews

    def _get_workload_summary(self) -> WorkloadSummary:
        """Get summary of workload resources."""
        summary = WorkloadSummary()

        # Get Deployments
        deployments_data = self.client.get_json("deployments", all_namespaces=True)
        if deployments_data and "items" in deployments_data:
            for item in deployments_data["items"]:
                metadata = item["metadata"]
                spec = item.get("spec", {})
                status = item.get("status", {})

                deployment = DeploymentOverview(
                    name=metadata["name"],
                    namespace=metadata["namespace"],
                    replicas=spec.get("replicas", 0),
                    ready_replicas=status.get("readyReplicas", 0),
                    available_replicas=status.get("availableReplicas", 0),
                )
                summary.deployments.append(deployment)

        # Get StatefulSets
        statefulsets_data = self.client.get_json("statefulsets", all_namespaces=True)
        if statefulsets_data and "items" in statefulsets_data:
            for item in statefulsets_data["items"]:
                metadata = item["metadata"]
                spec = item.get("spec", {})
                status = item.get("status", {})

                statefulset = StatefulSetOverview(
                    name=metadata["name"],
                    namespace=metadata["namespace"],
                    replicas=spec.get("replicas", 0),
                    ready_replicas=status.get("readyReplicas", 0),
                )
                summary.statefulsets.append(statefulset)

        # Get DaemonSets
        daemonsets_data = self.client.get_json("daemonsets", all_namespaces=True)
        if daemonsets_data and "items" in daemonsets_data:
            for item in daemonsets_data["items"]:
                metadata = item["metadata"]
                status = item.get("status", {})

                daemonset = DaemonSetOverview(
                    name=metadata["name"],
                    namespace=metadata["namespace"],
                    desired=status.get("desiredNumberScheduled", 0),
                    ready=status.get("numberReady", 0),
                )
                summary.daemonsets.append(daemonset)

        # Get Jobs
        jobs_data = self.client.get_json("jobs", all_namespaces=True)
        if jobs_data and "items" in jobs_data:
            for item in jobs_data["items"]:
                metadata = item["metadata"]
                spec = item.get("spec", {})
                status = item.get("status", {})

                job = JobOverview(
                    name=metadata["name"],
                    namespace=metadata["namespace"],
                    completions=spec.get("completions", 1),
                    succeeded=status.get("succeeded", 0),
                )
                summary.jobs.append(job)

        # Get CronJobs
        cronjobs_data = self.client.get_json("cronjobs", all_namespaces=True)
        if cronjobs_data and "items" in cronjobs_data:
            for item in cronjobs_data["items"]:
                metadata = item["metadata"]
                spec = item.get("spec", {})
                status = item.get("status", {})

                last_schedule = status.get("lastScheduleTime")
                last_schedule_time = None
                if last_schedule:
                    try:
                        last_schedule_time = datetime.fromisoformat(
                            last_schedule.replace("Z", "+00:00")
                        )
                    except Exception:
                        pass

                cronjob = CronJobOverview(
                    name=metadata["name"],
                    namespace=metadata["namespace"],
                    schedule=spec.get("schedule", ""),
                    suspend=spec.get("suspend", False),
                    last_schedule_time=last_schedule_time,
                )
                summary.cronjobs.append(cronjob)

        return summary

    def _get_resource_counts(self) -> ResourceCounts:
        """Get counts of various resource types."""
        counts = ResourceCounts()

        # Define resource mappings
        resource_mappings = [
            ("services", "services"),
            ("ingresses", "ingresses"),
            ("configmaps", "configmaps"),
            ("secrets", "secrets"),
            ("persistentvolumeclaims", "persistent_volume_claims"),
            ("persistentvolumes", "persistent_volumes"),
            ("serviceaccounts", "service_accounts"),
            ("networkpolicies", "network_policies"),
        ]

        for k8s_name, model_field in resource_mappings:
            data = self.client.get_json(k8s_name, all_namespaces=True)
            if data and "items" in data:
                setattr(counts, model_field, len(data["items"]))

        return counts

    def format_overview(self, overview: ClusterOverviewData, format: str = "text") -> str:
        """Format overview data for display."""
        if format == "text":
            return self._format_text(overview)
        elif format == "json":
            import json
            # Convert to dict and handle datetime
            data = overview.model_dump()
            data["scan_timestamp"] = overview.scan_timestamp.isoformat()
            return json.dumps(data, indent=2)
        elif format == "yaml":
            import yaml
            # Convert to dict and handle datetime
            data = overview.model_dump()
            data["scan_timestamp"] = overview.scan_timestamp.isoformat()
            return yaml.dump(data, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _format_text(self, overview: ClusterOverviewData) -> str:
        """Format overview as human-readable text."""
        lines = []
        lines.append("=" * 80)
        lines.append("KUBERNETES CLUSTER OVERVIEW")
        lines.append("=" * 80)

        # Cluster Info
        lines.append("\n📊 CLUSTER INFORMATION")
        lines.append("-" * 40)
        lines.append(f"Version:     {overview.cluster_version}")
        lines.append(f"Platform:    {overview.platform}")
        lines.append(f"Node Count:  {overview.node_count}")
        lines.append(f"Health:      {overview.healthy_percentage:.1f}% workloads healthy")

        if overview.nodes:
            lines.append("\nNodes:")
            for node in overview.nodes:
                lines.append(
                    f"  • {node['name']:<30} {node['status']:<10} "
                    f"{node['roles']:<20} {node['version']}"
                )

        # Namespace Summary
        lines.append("\n📁 NAMESPACES")
        lines.append("-" * 40)
        lines.append(f"Total Namespaces: {overview.namespace_count}")
        lines.append(f"Total Pods:       {overview.total_pods}")

        top_namespaces = overview.get_namespaces_by_pod_count(limit=10)
        if top_namespaces:
            lines.append("\nTop Namespaces by Pod Count:")
            for ns in top_namespaces:
                pod_status_parts = []
                if ns.pod_status.running > 0:
                    pod_status_parts.append(f"Running:{ns.pod_status.running}")
                if ns.pod_status.pending > 0:
                    pod_status_parts.append(f"Pending:{ns.pod_status.pending}")
                if ns.pod_status.failed > 0:
                    pod_status_parts.append(f"Failed:{ns.pod_status.failed}")

                status_str = ", ".join(pod_status_parts) if pod_status_parts else "No pods"
                lines.append(f"  • {ns.name:<30} {ns.pod_count:>3} pods ({status_str})")

        # Workload Summary
        lines.append("\n🚀 WORKLOADS")
        lines.append("-" * 40)
        lines.append(f"Total Workloads: {overview.workloads.total_workloads}")

        if overview.workloads.deployments:
            lines.append(f"\nDeployments ({len(overview.workloads.deployments)}):")
            for item in overview.workloads.deployments[:5]:
                health = "✓" if item.is_healthy else "✗"
                lines.append(
                    f"  {health} {item.namespace}/{item.name}: "
                    f"{item.ready_replicas}/{item.replicas} ready"
                )

        if overview.workloads.statefulsets:
            lines.append(f"\nStatefulSets ({len(overview.workloads.statefulsets)}):")
            for item in overview.workloads.statefulsets[:5]:
                health = "✓" if item.is_healthy else "✗"
                lines.append(
                    f"  {health} {item.namespace}/{item.name}: "
                    f"{item.ready_replicas}/{item.replicas} ready"
                )

        if overview.workloads.daemonsets:
            lines.append(f"\nDaemonSets ({len(overview.workloads.daemonsets)}):")
            for item in overview.workloads.daemonsets[:5]:
                health = "✓" if item.is_healthy else "✗"
                lines.append(
                    f"  {health} {item.namespace}/{item.name}: "
                    f"{item.ready}/{item.desired} ready"
                )

        if overview.workloads.jobs:
            lines.append(f"\nJobs ({len(overview.workloads.jobs)}):")
            for item in overview.workloads.jobs[:5]:
                complete = "✓" if item.is_complete else "⟳"
                lines.append(
                    f"  {complete} {item.namespace}/{item.name}: "
                    f"{item.succeeded}/{item.completions} completed"
                )

        if overview.workloads.cronjobs:
            lines.append(f"\nCronJobs ({len(overview.workloads.cronjobs)}):")
            for item in overview.workloads.cronjobs[:5]:
                status = "paused" if item.suspend else "active"
                lines.append(f"  • {item.namespace}/{item.name}: {item.schedule} ({status})")

        # Check for unhealthy workloads
        unhealthy = overview.workloads.get_unhealthy_workloads()
        if unhealthy:
            lines.append(f"\n⚠️  Unhealthy Workloads ({len(unhealthy)}):")
            for item in unhealthy[:5]:
                lines.append(f"  • {item.kind}: {item.namespace}/{item.name}")

        # Resource Summary
        lines.append("\n📦 RESOURCE SUMMARY")
        lines.append("-" * 40)

        resource_summary = overview.get_resource_summary()
        for resource_type, count in sorted(resource_summary.items()):
            if count > 0:
                lines.append(f"{resource_type:<25} {count:>5}")

        lines.append("-" * 40)
        lines.append(f"{'Total Resources':<25} {overview.resource_counts.total:>5}")

        # Footer
        lines.append("\n" + "=" * 80)
        lines.append(f"Scan completed: {overview.scan_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

        return "\n".join(lines)
