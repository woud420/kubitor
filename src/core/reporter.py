"""Cluster report generator."""

import json
import yaml
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import defaultdict

from ..model.kubernetes import K8sResource
from ..model.cluster import ClusterInfo, NodeInfo, ClusterVersion
from ..model.report import (
    ClusterReport,
    ResourceSummary,
    ResourceDetail,
    NamespaceDetail,
    ReportFormat,
    ResourceUpgradeAction,
)
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
        self.hide_system_resources: bool = False

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
            resource_upgrade_actions=[],
        )

        # Add upgrade suggestions if we have version info
        if report.cluster_info.server_version:
            version = report.cluster_info.server_version.git_version
            report.upgrade_suggestions = self.upgrade_advisor.get_suggestions(version)

        # Check resources for deprecated API usage
        report.resource_upgrade_actions = self.upgrade_advisor.get_deprecated_resources(
            resources
        )

        # Format report using format handlers
        format_handlers = {
            ReportFormat.JSON: lambda r: json.dumps(r.model_dump(), indent=2, default=str),
            ReportFormat.YAML: lambda r: yaml.dump(r.model_dump(), default_flow_style=False, sort_keys=False),
            ReportFormat.TEXT: lambda r: self._format_text_report(r),
        }
        
        handler = format_handlers.get(output_format, lambda r: self._format_text_report(r))
        return handler(report)

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

    def _parse_node_info(self, node_data: Dict[str, Any]) -> Optional[NodeInfo]:
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

        by_namespace: Dict[str, int] = defaultdict(int)
        by_type: Dict[str, int] = defaultdict(int)
        resources_by_ns = defaultdict(list)
        namespace_helm_releases = defaultdict(set)

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

            # Create detailed resource entry
            detail = ResourceDetail(
                name=resource.name,
                kind=resource.kind,
                api_version=resource.api_version,
                namespace=resource.namespace,
                labels=resource.labels
            )
            
            # Extract helm information if available
            if resource.is_helm_managed:
                helm_release = resource.labels.get("helm.sh/release") or resource.labels.get("app.kubernetes.io/instance")
                helm_chart = resource.labels.get("helm.sh/chart") or resource.labels.get("app.kubernetes.io/name")
                helm_version = resource.labels.get("app.kubernetes.io/version")
                
                detail.helm_release = helm_release
                detail.helm_chart = helm_chart
                detail.helm_version = helm_version
                
                if helm_release:
                    namespace_helm_releases[namespace].add(helm_release)
            
            resources_by_ns[namespace].append(detail)

        summary.by_namespace = dict(by_namespace)
        summary.by_type = dict(by_type)
        summary.resources_by_namespace = dict(resources_by_ns)
        
        # Create namespace details
        for ns, count in by_namespace.items():
            ns_detail = NamespaceDetail(
                name=ns,
                resource_count=count,
                resources=resources_by_ns.get(ns, []),
                helm_releases=list(namespace_helm_releases.get(ns, set()))
            )
            summary.namespace_details.append(ns_detail)
        
        # Sort namespace details by resource count
        summary.namespace_details.sort(key=lambda x: x.resource_count, reverse=True)

        return summary

    def _format_text_report(self, report: ClusterReport) -> str:
        """Format report as human-readable text."""
        lines = []
        lines.append("=" * 80)
        lines.append("KUBERNETES CLUSTER REPORT")
        lines.append("=" * 80)
        lines.append(f"Generated: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Quick summary
        lines.append("QUICK SUMMARY")
        lines.append("-" * 40)
        lines.append(f"Total Namespaces: {len(report.resources.by_namespace)}")
        lines.append(f"Total Resources: {report.resources.total_resources}")
        lines.append(f"Total Pods: {report.resources.by_type.get('Pod', 0)}")
        lines.append(f"Total Services: {report.resources.by_type.get('Service', 0)}")
        lines.append(f"Total Deployments: {report.resources.by_type.get('Deployment', 0)}")
        lines.append(f"Total ConfigMaps: {report.resources.by_type.get('ConfigMap', 0)}")
        lines.append(f"Total Secrets: {report.resources.by_type.get('Secret', 0)}")
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

        # Nodes - Enhanced display
        if report.cluster_info.nodes:
            lines.append("CLUSTER NODES")
            lines.append("-" * 40)
            lines.append(f"Total Nodes: {len(report.cluster_info.nodes)}")
            lines.append("")
            
            # Group nodes by role
            by_role = defaultdict(list)
            for node in report.cluster_info.nodes:
                by_role[node.roles].append(node)
            
            for role, nodes in sorted(by_role.items()):
                lines.append(f"{role.upper()} NODES ({len(nodes)}):")
                for node in nodes:
                    lines.append(f"  • {node.name}")
                    lines.append(f"    Status: {node.status} | Version: {node.version}")
                    lines.append(f"    OS: {node.os}")
                    lines.append(f"    Runtime: {node.container_runtime}")
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

        # Resources - Enhanced view
        lines.append("RESOURCE OVERVIEW")
        lines.append("-" * 40)
        lines.append(f"Total Resources: {report.resources.total_resources}")
        lines.append(f"Helm Managed: {report.resources.helm_managed} ({report.resources.helm_managed * 100 // max(report.resources.total_resources, 1)}%)")
        lines.append(f"Non-Helm Managed: {report.resources.non_helm_managed} ({report.resources.non_helm_managed * 100 // max(report.resources.total_resources, 1)}%)")
        lines.append(f"Total Namespaces: {len(report.resources.by_namespace)}")
        lines.append("")

        # Resource types breakdown
        if report.resources.by_type:
            lines.append("RESOURCE TYPES")
            lines.append("-" * 40)
            sorted_types = sorted(report.resources.by_type.items(), key=lambda x: x[1], reverse=True)
            for kind, count in sorted_types[:15]:
                lines.append(f"  {kind:20} {count:>5}")
            if len(sorted_types) > 15:
                lines.append(f"  ... and {len(sorted_types) - 15} more types")
            lines.append("")

        # Complete namespace breakdown
        if report.resources.namespace_details:
            lines.append("COMPLETE NAMESPACE BREAKDOWN")
            lines.append("=" * 80)
            
            # Categorize namespaces
            system_namespaces = []
            app_namespaces = []
            
            for ns_detail in report.resources.namespace_details:
                if self._is_system_namespace(ns_detail.name):
                    system_namespaces.append(ns_detail)
                else:
                    app_namespaces.append(ns_detail)
            
            # Show application namespaces first
            if app_namespaces:
                lines.append("\nAPPLICATION NAMESPACES")
                lines.append("-" * 40)
                for ns_detail in sorted(app_namespaces, key=lambda x: x.resource_count, reverse=True):
                    lines.extend(self._format_namespace_detail(ns_detail))
            
            # Show system namespaces if not filtered
            if system_namespaces and not getattr(self, 'hide_system_resources', False):
                lines.append("\nSYSTEM NAMESPACES (Kubernetes/EKS Managed)")
                lines.append("-" * 40)
                for ns_detail in sorted(system_namespaces, key=lambda x: x.resource_count, reverse=True):
                    lines.extend(self._format_namespace_detail(ns_detail, is_system=True))
        lines.append("")

        # Deprecated API usage
        if report.resource_upgrade_actions:
            lines.append("DEPRECATED API USAGE")
            lines.append("-" * 40)
            by_api = defaultdict(list)
            for action in report.resource_upgrade_actions:
                api = f"{action.current_version}/{action.kind}"
                identifier = (
                    f"{action.namespace}/{action.name}" if action.namespace else action.name
                )
                by_api[api].append((identifier, action.suggested_version))
            for api, resources in by_api.items():
                lines.append(f"{api} → {resources[0][1]}")
                for name, _ in resources:
                    lines.append(f"  - {name}")
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
                for required_action in sugg.required_actions:
                    lines.append(f"  ➤ {required_action}")

            if sugg.general_recommendations:
                lines.append("\nGeneral Recommendations:")
                for rec in sugg.general_recommendations:
                    lines.append(f"  • {rec}")

        return "\n".join(lines)
    
    def _is_system_namespace(self, namespace: str) -> bool:
        """Check if namespace is a system/managed namespace."""
        system_prefixes = [
            'kube-', 'eks-', 'aks-', 'gke-', 'openshift-',
            'istio-', 'linkerd-', 'cert-manager', 'ingress-',
            'monitoring', 'logging', 'tracing', 'elastic-',
            'prometheus', 'grafana', 'jaeger', 'fluentd',
            'datadog', 'newrelic', 'splunk', 'dynatrace'
        ]
        
        # Check if it's kube-system, kube-public, kube-node-lease, or default
        if namespace in ['kube-system', 'kube-public', 'kube-node-lease', 'default']:
            return True
        
        # Check for known system prefixes
        for prefix in system_prefixes:
            if namespace.startswith(prefix):
                return True
        
        return False
    
    def _format_namespace_detail(self, ns_detail: NamespaceDetail, is_system: bool = False) -> List[str]:
        """Format detailed namespace information."""
        lines = []
        
        # Namespace header
        header = f"\n📁 {ns_detail.name}"
        if is_system:
            header += " [SYSTEM]"
        lines.append(header)
        lines.append(f"   Total Resources: {ns_detail.resource_count}")
        
        # Group resources by kind
        by_kind = defaultdict(list)
        for res in ns_detail.resources:
            by_kind[res.kind].append(res)
        
        # Show Services and their associated Pods
        services = by_kind.get('Service', [])
        pods = by_kind.get('Pod', [])
        deployments = by_kind.get('Deployment', [])
        statefulsets = by_kind.get('StatefulSet', [])
        daemonsets = by_kind.get('DaemonSet', [])
        
        # Show workloads
        if deployments or statefulsets or daemonsets:
            lines.append("   Workloads:")
            if deployments:
                lines.append(f"     • Deployments ({len(deployments)}):")
                for dep in deployments[:5]:
                    helm_info = f" [Helm: {dep.helm_release}]" if dep.helm_release else ""
                    lines.append(f"       - {dep.name}{helm_info}")
                if len(deployments) > 5:
                    lines.append(f"       ... and {len(deployments) - 5} more")
            
            if statefulsets:
                lines.append(f"     • StatefulSets ({len(statefulsets)}):")
                for sts in statefulsets[:3]:
                    helm_info = f" [Helm: {sts.helm_release}]" if sts.helm_release else ""
                    lines.append(f"       - {sts.name}{helm_info}")
                if len(statefulsets) > 3:
                    lines.append(f"       ... and {len(statefulsets) - 3} more")
            
            if daemonsets:
                lines.append(f"     • DaemonSets ({len(daemonsets)}):")
                for ds in daemonsets[:3]:
                    helm_info = f" [Helm: {ds.helm_release}]" if ds.helm_release else ""
                    lines.append(f"       - {ds.name}{helm_info}")
                if len(daemonsets) > 3:
                    lines.append(f"       ... and {len(daemonsets) - 3} more")
        
        # Show Services
        if services:
            lines.append(f"   Services ({len(services)}):")
            for svc in services[:5]:
                helm_info = f" [Helm: {svc.helm_release}]" if svc.helm_release else ""
                # Try to match pods to services
                svc_pods = self._find_pods_for_service(svc, pods)
                pod_info = f" → {len(svc_pods)} pods" if svc_pods else ""
                lines.append(f"     • {svc.name}{helm_info}{pod_info}")
                if svc_pods and len(svc_pods) <= 3:
                    for pod in svc_pods:
                        lines.append(f"       └─ {pod.name}")
            if len(services) > 5:
                lines.append(f"     ... and {len(services) - 5} more services")
        
        # Show Pods not associated with services
        if pods:
            unmatched_pods = [p for p in pods if not any(
                self._pod_matches_service(p, s) for s in services
            )]
            if unmatched_pods:
                lines.append(f"   Standalone Pods ({len(unmatched_pods)}):")
                for pod in unmatched_pods[:3]:
                    helm_info = f" [Helm: {pod.helm_release}]" if pod.helm_release else ""
                    lines.append(f"     • {pod.name}{helm_info}")
                if len(unmatched_pods) > 3:
                    lines.append(f"     ... and {len(unmatched_pods) - 3} more")
        
        # Show ConfigMaps and Secrets count
        configmaps = by_kind.get('ConfigMap', [])
        secrets = by_kind.get('Secret', [])
        ingresses = by_kind.get('Ingress', [])
        
        if configmaps or secrets:
            lines.append("   Configuration:")
            if configmaps:
                lines.append(f"     • ConfigMaps: {len(configmaps)}")
            if secrets:
                lines.append(f"     • Secrets: {len(secrets)}")
        
        if ingresses:
            lines.append(f"   Ingresses ({len(ingresses)}):")
            for ing in ingresses:
                helm_info = f" [Helm: {ing.helm_release}]" if ing.helm_release else ""
                lines.append(f"     • {ing.name}{helm_info}")
        
        # Show Helm releases
        if ns_detail.helm_releases:
            lines.append(f"   Helm Releases: {', '.join(ns_detail.helm_releases)}")
        
        # Show other resource types if significant
        other_kinds = {k: v for k, v in by_kind.items() 
                      if k not in ['Service', 'Pod', 'Deployment', 'StatefulSet', 
                                  'DaemonSet', 'ConfigMap', 'Secret', 'Ingress']}
        if other_kinds:
            significant_others = [(k, len(v)) for k, v in other_kinds.items() if len(v) > 1]
            if significant_others:
                lines.append("   Other Resources:")
                for kind, count in sorted(significant_others, key=lambda x: x[1], reverse=True)[:5]:
                    lines.append(f"     • {kind}: {count}")
        
        return lines
    
    def _find_pods_for_service(self, service: ResourceDetail, pods: List[ResourceDetail]) -> List[ResourceDetail]:
        """Find pods that match a service's selector."""
        matching_pods = []
        for pod in pods:
            if self._pod_matches_service(pod, service):
                matching_pods.append(pod)
        return matching_pods
    
    def _pod_matches_service(self, pod: ResourceDetail, service: ResourceDetail) -> bool:
        """Check if a pod matches a service's selector."""
        # This is a simplified check - in reality we'd need to look at service selectors
        # For now, we'll do basic name matching
        if service.name in pod.name:
            return True
        # Check if they're from the same helm release
        if service.helm_release and service.helm_release == pod.helm_release:
            return True
        return False
