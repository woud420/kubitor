"""Cluster overview models."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PodStatus(BaseModel):
    """Pod status counts."""

    running: int = 0
    pending: int = 0
    succeeded: int = 0
    failed: int = 0
    unknown: int = 0

    @property
    def total(self) -> int:
        """Total pod count."""
        return self.running + self.pending + self.succeeded + self.failed + self.unknown

    model_config = ConfigDict(extra="allow")


class NamespaceOverview(BaseModel):
    """Overview of a single namespace."""

    name: str
    status: str = "Active"
    pod_count: int = 0
    pod_status: PodStatus = Field(default_factory=PodStatus)
    labels: Dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class WorkloadItem(BaseModel):
    """Base workload item."""

    name: str
    namespace: str
    kind: str

    model_config = ConfigDict(extra="allow")


class DeploymentOverview(WorkloadItem):
    """Deployment overview."""

    kind: str = "Deployment"
    replicas: int = 0
    ready_replicas: int = 0
    available_replicas: int = 0

    @property
    def is_healthy(self) -> bool:
        """Check if deployment is healthy."""
        return self.ready_replicas == self.replicas


class StatefulSetOverview(WorkloadItem):
    """StatefulSet overview."""

    kind: str = "StatefulSet"
    replicas: int = 0
    ready_replicas: int = 0

    @property
    def is_healthy(self) -> bool:
        """Check if statefulset is healthy."""
        return self.ready_replicas == self.replicas


class DaemonSetOverview(WorkloadItem):
    """DaemonSet overview."""

    kind: str = "DaemonSet"
    desired: int = 0
    ready: int = 0

    @property
    def is_healthy(self) -> bool:
        """Check if daemonset is healthy."""
        return self.ready == self.desired


class JobOverview(WorkloadItem):
    """Job overview."""

    kind: str = "Job"
    completions: int = 1
    succeeded: int = 0

    @property
    def is_complete(self) -> bool:
        """Check if job is complete."""
        return self.succeeded == self.completions


class CronJobOverview(WorkloadItem):
    """CronJob overview."""

    kind: str = "CronJob"
    schedule: str = ""
    suspend: bool = False
    last_schedule_time: Optional[datetime] = None


class WorkloadSummary(BaseModel):
    """Summary of all workloads."""

    deployments: List[DeploymentOverview] = Field(default_factory=list)
    statefulsets: List[StatefulSetOverview] = Field(default_factory=list)
    daemonsets: List[DaemonSetOverview] = Field(default_factory=list)
    jobs: List[JobOverview] = Field(default_factory=list)
    cronjobs: List[CronJobOverview] = Field(default_factory=list)

    @property
    def total_workloads(self) -> int:
        """Total number of workloads."""
        return (
            len(self.deployments) +
            len(self.statefulsets) +
            len(self.daemonsets) +
            len(self.jobs) +
            len(self.cronjobs)
        )

    def get_unhealthy_workloads(self) -> List[WorkloadItem]:
        """Get list of unhealthy workloads."""
        unhealthy: List[WorkloadItem] = []

        for deployment in self.deployments:
            if not deployment.is_healthy:
                unhealthy.append(deployment)

        for statefulset in self.statefulsets:
            if not statefulset.is_healthy:
                unhealthy.append(statefulset)

        for daemonset in self.daemonsets:
            if not daemonset.is_healthy:
                unhealthy.append(daemonset)

        return unhealthy


class ResourceCounts(BaseModel):
    """Resource type counts."""

    services: int = 0
    ingresses: int = 0
    configmaps: int = 0
    secrets: int = 0
    persistent_volume_claims: int = 0
    persistent_volumes: int = 0
    service_accounts: int = 0
    network_policies: int = 0

    @property
    def total(self) -> int:
        """Total resource count."""
        return sum(self.model_dump().values())

    model_config = ConfigDict(extra="allow")


class ClusterOverviewData(BaseModel):
    """Complete cluster overview data."""

    # Basic cluster info
    cluster_version: str
    platform: str = "Unknown"
    node_count: int = 0
    nodes: List[Dict[str, Any]] = Field(default_factory=list)

    # Namespace information
    namespace_count: int = 0
    namespaces: List[NamespaceOverview] = Field(default_factory=list)

    # Workload summary
    workloads: WorkloadSummary = Field(default_factory=WorkloadSummary)

    # Resource counts
    resource_counts: ResourceCounts = Field(default_factory=ResourceCounts)

    # Metadata
    scan_timestamp: datetime = Field(default_factory=lambda: datetime.now())

    @property
    def total_pods(self) -> int:
        """Total pod count across all namespaces."""
        return sum(ns.pod_count for ns in self.namespaces)

    @property
    def healthy_percentage(self) -> float:
        """Percentage of healthy workloads."""
        total = self.workloads.total_workloads
        if total == 0:
            return 100.0

        unhealthy = len(self.workloads.get_unhealthy_workloads())
        return ((total - unhealthy) / total) * 100

    def get_namespaces_by_pod_count(self, limit: Optional[int] = None) -> List[NamespaceOverview]:
        """Get namespaces sorted by pod count."""
        sorted_ns = sorted(self.namespaces, key=lambda x: x.pod_count, reverse=True)
        return sorted_ns[:limit] if limit else sorted_ns

    def get_resource_summary(self) -> Dict[str, int]:
        """Get resource summary as dictionary."""
        return {
            "Services": self.resource_counts.services,
            "Ingresses": self.resource_counts.ingresses,
            "ConfigMaps": self.resource_counts.configmaps,
            "Secrets": self.resource_counts.secrets,
            "PersistentVolumeClaims": self.resource_counts.persistent_volume_claims,
            "PersistentVolumes": self.resource_counts.persistent_volumes,
            "ServiceAccounts": self.resource_counts.service_accounts,
            "NetworkPolicies": self.resource_counts.network_policies,
        }

    model_config = ConfigDict(populate_by_name=True)
