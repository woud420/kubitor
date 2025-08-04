"""Cluster report generator."""

import json
import yaml
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict

from ..model.kubernetes import K8sResource
from ..model.cluster import ClusterInfo, NodeInfo, ClusterVersion
from ..model.report import ClusterReport, ResourceSummary, ReportFormat
from ..k8s import K8sClient
from ..upgrade import UpgradeAdvisor
from ..utils.logger import get_logger
from .helm import HelmClient

logger = get_logger(__name__)


class ClusterReporter:
    """Generates comprehensive cluster reports."""

    def __init__(self, client: K8sClient):
        self.client = client
        self.helm_client = HelmClient()
        self.upgrade_advisor = UpgradeAdvisor()

    def generate_report(self, resources: List[K8sResource], output_format: ReportFormat) -> str:
        """Generate a cluster report."""
        logger.info("Generating cluster report")

        # Gather all information
        report = ClusterReport(
            timestamp=datetime.now(),
            cluster_info=self._get_cluster_info(),
            helm_releases=self.helm_client.get_releases(),
            helm_repositories=self.helm_client.get_repositories(),
            resources=self._get_resource_summary(resources),
            upgrade_suggestions=None,
        )

        # Add upgrade suggestions if we have version info
        if report.cluster_info.server_version:
            version = report.cluster_info.server_version.git_version
            report.upgrade_suggestions = self.upgrade_advisor.get_suggestions(version)

        # Format report
        if output_format == ReportFormat.JSON:
            return json.dumps(report.dict(), indent=2, default=str)
        elif output_format == ReportFormat.YAML:
            return yaml.dump(report.dict(), default_flow_style=False, sort_keys=False)
        else:
            return self._format_text_report(report)

    def _get_cluster_info(self) -> ClusterInfo:
        """Get cluster version and node information."""
        cluster_info = ClusterInfo()

        # Get version
        version_data = self.client.get_version()
        if version_data:
            if "serverVersion" in version_data:
                cluster_info.server_version = ClusterVersion(**version_data["serverVersion"])
            if "clientVersion" in version_data:
                cluster_info.client_version = ClusterVersion(**version_data["clientVersion"])

        # Get nodes
        nodes_data = self.client.get_json("nodes")
        if nodes_data and "items" in nodes_data:
            for node in nodes_data["items"]:
                node_info = self._parse_node_info(node)
                if node_info:
                    cluster_info.nodes.append(node_info)

        return cluster_info

    def _parse_node_info(self, node_data: Dict[str, Any]) -> NodeInfo:
        """Parse node information."""
        try:
            metadata = node_data["metadata"]
            status = node_data["status"]

            # Get roles from labels
            roles = []
            for label in metadata.get("labels", {}):
                if "node-role.kubernetes.io" in label:
                    role = label.split("/")[-1]
                    if role:
                        roles.append(role)

            # Get node condition
            conditions = status.get("conditions", [])
            node_status = "Unknown"
            for condition in conditions:
                if condition.get("type") == "Ready":
                    node_status = "Ready" if condition.get("status") == "True" else "NotReady"
                    break

            node_info = status["nodeInfo"]

            return NodeInfo(
                name=metadata["name"],
                status=node_status,
                roles=",".join(roles) if roles else "worker",
                version=node_info["kubeletVersion"],
                os=node_info["osImage"],
                container_runtime=node_info["containerRuntimeVersion"],
            )
        except KeyError as e:
            logger.error(f"Failed to parse node info: {e}")
            return None

    def _get_resource_summary(self, resources: List[K8sResource]) -> ResourceSummary:
        """Generate resource summary statistics."""
        summary = ResourceSummary()
        summary.total_resources = len(resources)

        by_namespace = defaultdict(int)
        by_type = defaultdict(int)

        for resource in resources:
            # Count by type
            by_type[resource.kind] += 1

            # Count by namespace
            namespace = resource.namespace or "cluster-scoped"
            by_namespace[namespace] += 1

            # Count Helm managed
            if resource.is_helm_managed:
                summary.helm_managed += 1
            else:
                summary.non_helm_managed += 1

        summary.by_namespace = dict(by_namespace)
        summary.by_type = dict(by_type)

        return summary

    def _format_text_report(self, report: ClusterReport) -> str:
        """Format report as human-readable text."""
        lines = []
        lines.append("=" * 80)
        lines.append("KUBERNETES CLUSTER REPORT")
        lines.append("=" * 80)
        lines.append(f"Generated: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # Cluster info
        lines.append("CLUSTER INFORMATION")
        lines.append("-" * 40)
        if report.cluster_info.server_version:
            server = report.cluster_info.server_version
            lines.append(f"Server Version: {server.git_version}")
            lines.append(f"Platform: {server.platform or 'Unknown'}")
        if report.cluster_info.client_version:
            client = report.cluster_info.client_version
            lines.append(f"Client Version: {client.git_version}")
        lines.append("")

        # Nodes
        if report.cluster_info.nodes:
            lines.append("NODES")
            lines.append("-" * 40)
            for node in report.cluster_info.nodes:
                lines.append(f"- {node.name}: {node.version} ({node.os})")
                lines.append(f"  Status: {node.status}, Roles: {node.roles}")
                lines.append(f"  Runtime: {node.container_runtime}")
            lines.append("")

        # Helm
        if report.helm_releases or report.helm_repositories:
            lines.append("HELM INFORMATION")
            lines.append("-" * 40)

            if report.helm_releases:
                lines.append(f"Releases: {len(report.helm_releases)}")
                for release in report.helm_releases[:10]:  # Show first 10
                    lines.append(
                        f"- {release.name} ({release.namespace}): "
                        f"{release.chart} - {release.status}"
                    )
                if len(report.helm_releases) > 10:
                    lines.append(f"  ... and {len(report.helm_releases) - 10} more")

            if report.helm_repositories:
                lines.append(f"\nRepositories: {len(report.helm_repositories)}")
                for repo in report.helm_repositories:
                    lines.append(f"- {repo.name}: {repo.url}")
            lines.append("")

        # Resources
        lines.append("RESOURCE SUMMARY")
        lines.append("-" * 40)
        lines.append(f"Total Resources: {report.resources.total_resources}")
        lines.append(f"Helm Managed: {report.resources.helm_managed}")
        lines.append(f"Non-Helm Managed: {report.resources.non_helm_managed}")

        if report.resources.by_type:
            lines.append("\nBy Type:")
            for kind, count in sorted(report.resources.by_type.items()):
                lines.append(f"  {kind}: {count}")

        if report.resources.by_namespace:
            lines.append("\nBy Namespace (top 10):")
            sorted_ns = sorted(
                report.resources.by_namespace.items(), key=lambda x: x[1], reverse=True
            )
            for ns, count in sorted_ns[:10]:
                lines.append(f"  {ns}: {count}")
            if len(sorted_ns) > 10:
                lines.append(f"  ... and {len(sorted_ns) - 10} more namespaces")
        lines.append("")

        # Upgrade suggestions
        if report.upgrade_suggestions:
            lines.append("UPGRADE SUGGESTIONS")
            lines.append("-" * 40)
            sugg = report.upgrade_suggestions
            lines.append(f"Current Version: {sugg.current_version}")
            lines.append(f"Suggested Next Version: {sugg.suggested_next_version}")

            if sugg.upgrade_notes:
                lines.append("\nUpgrade Notes:")
                for note in sugg.upgrade_notes:
                    lines.append(f"  • {note}")

            if sugg.api_deprecations:
                lines.append("\nAPI Deprecations:")
                for dep in sugg.api_deprecations:
                    lines.append(f"  ⚠️  {dep}")

            if sugg.required_actions:
                lines.append("\nRequired Actions:")
                for action in sugg.required_actions:
                    lines.append(f"  ➤ {action}")

            if sugg.general_recommendations:
                lines.append("\nGeneral Recommendations:")
                for rec in sugg.general_recommendations:
                    lines.append(f"  • {rec}")

        return "\n".join(lines)
