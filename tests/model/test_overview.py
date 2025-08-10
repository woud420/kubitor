"""Tests for overview models."""

import pytest
from datetime import datetime

from src.model.overview import (
    PodStatus,
    NamespaceOverview,
    DeploymentOverview,
    StatefulSetOverview,
    DaemonSetOverview,
    JobOverview,
    CronJobOverview,
    WorkloadSummary,
    ResourceCounts,
    ClusterOverviewData,
)


class TestPodStatus:
    """Test PodStatus model."""

    def test_pod_status_creation(self):
        """Test PodStatus creation with defaults."""
        status = PodStatus()
        assert status.running == 0
        assert status.pending == 0
        assert status.succeeded == 0
        assert status.failed == 0
        assert status.unknown == 0
        assert status.total == 0

    def test_pod_status_total(self):
        """Test total property calculation."""
        status = PodStatus(running=5, pending=2, failed=1)
        assert status.total == 8

    def test_pod_status_extra_fields(self):
        """Test that extra fields are allowed."""
        status = PodStatus(running=1, custom_field="value")
        assert status.running == 1


class TestNamespaceOverview:
    """Test NamespaceOverview model."""

    def test_namespace_overview_creation(self):
        """Test NamespaceOverview creation."""
        ns = NamespaceOverview(
            name="default",
            status="Active",
            pod_count=10,
            pod_status=PodStatus(running=8, pending=2)
        )
        assert ns.name == "default"
        assert ns.status == "Active"
        assert ns.pod_count == 10
        assert ns.pod_status.running == 8

    def test_namespace_overview_defaults(self):
        """Test default values."""
        ns = NamespaceOverview(name="test")
        assert ns.status == "Active"
        assert ns.pod_count == 0
        assert isinstance(ns.pod_status, PodStatus)
        assert ns.labels == {}


class TestWorkloadModels:
    """Test individual workload models."""

    def test_deployment_overview(self):
        """Test DeploymentOverview model."""
        deployment = DeploymentOverview(
            name="app1",
            namespace="default",
            replicas=3,
            ready_replicas=3,
            available_replicas=3
        )
        assert deployment.kind == "Deployment"
        assert deployment.is_healthy is True
        
        unhealthy = DeploymentOverview(
            name="app2",
            namespace="default",
            replicas=3,
            ready_replicas=1
        )
        assert unhealthy.is_healthy is False

    def test_statefulset_overview(self):
        """Test StatefulSetOverview model."""
        sts = StatefulSetOverview(
            name="db1",
            namespace="default",
            replicas=2,
            ready_replicas=2
        )
        assert sts.kind == "StatefulSet"
        assert sts.is_healthy is True

    def test_daemonset_overview(self):
        """Test DaemonSetOverview model."""
        ds = DaemonSetOverview(
            name="monitor",
            namespace="kube-system",
            desired=5,
            ready=5
        )
        assert ds.kind == "DaemonSet"
        assert ds.is_healthy is True

    def test_job_overview(self):
        """Test JobOverview model."""
        job = JobOverview(
            name="backup",
            namespace="default",
            completions=1,
            succeeded=1
        )
        assert job.kind == "Job"
        assert job.is_complete is True

    def test_cronjob_overview(self):
        """Test CronJobOverview model."""
        cronjob = CronJobOverview(
            name="cleanup",
            namespace="default",
            schedule="0 * * * *",
            suspend=False,
            last_schedule_time=datetime.now()
        )
        assert cronjob.kind == "CronJob"
        assert cronjob.schedule == "0 * * * *"
        assert cronjob.suspend is False


class TestWorkloadSummary:
    """Test WorkloadSummary model."""

    def test_workload_summary_creation(self):
        """Test WorkloadSummary creation."""
        summary = WorkloadSummary()
        assert summary.deployments == []
        assert summary.statefulsets == []
        assert summary.daemonsets == []
        assert summary.jobs == []
        assert summary.cronjobs == []
        assert summary.total_workloads == 0

    def test_workload_summary_total(self):
        """Test total_workloads property."""
        summary = WorkloadSummary(
            deployments=[
                DeploymentOverview(name="d1", namespace="ns"),
                DeploymentOverview(name="d2", namespace="ns")
            ],
            statefulsets=[
                StatefulSetOverview(name="s1", namespace="ns")
            ]
        )
        assert summary.total_workloads == 3

    def test_get_unhealthy_workloads(self):
        """Test get_unhealthy_workloads method."""
        summary = WorkloadSummary(
            deployments=[
                DeploymentOverview(name="healthy", namespace="ns", replicas=2, ready_replicas=2),
                DeploymentOverview(name="unhealthy", namespace="ns", replicas=3, ready_replicas=1)
            ],
            statefulsets=[
                StatefulSetOverview(name="unhealthy-sts", namespace="ns", replicas=2, ready_replicas=0)
            ]
        )
        
        unhealthy = summary.get_unhealthy_workloads()
        assert len(unhealthy) == 2
        assert unhealthy[0].name == "unhealthy"
        assert unhealthy[1].name == "unhealthy-sts"


class TestResourceCounts:
    """Test ResourceCounts model."""

    def test_resource_counts_creation(self):
        """Test ResourceCounts creation with defaults."""
        counts = ResourceCounts()
        assert counts.services == 0
        assert counts.ingresses == 0
        assert counts.configmaps == 0
        assert counts.total == 0

    def test_resource_counts_total(self):
        """Test total property calculation."""
        counts = ResourceCounts(
            services=10,
            ingresses=2,
            configmaps=20,
            secrets=15
        )
        assert counts.total == 47

    def test_resource_counts_extra_fields(self):
        """Test that extra fields are allowed."""
        counts = ResourceCounts(services=5, custom_resource=10)
        assert counts.services == 5


class TestClusterOverviewData:
    """Test ClusterOverviewData model."""

    def test_cluster_overview_creation(self):
        """Test ClusterOverviewData creation."""
        overview = ClusterOverviewData(
            cluster_version="v1.28.0",
            platform="linux/amd64",
            node_count=3
        )
        assert overview.cluster_version == "v1.28.0"
        assert overview.platform == "linux/amd64"
        assert overview.node_count == 3
        assert isinstance(overview.scan_timestamp, datetime)

    def test_cluster_overview_defaults(self):
        """Test default values."""
        overview = ClusterOverviewData(cluster_version="v1.28.0")
        assert overview.platform == "Unknown"
        assert overview.node_count == 0
        assert overview.namespaces == []
        assert isinstance(overview.workloads, WorkloadSummary)
        assert isinstance(overview.resource_counts, ResourceCounts)

    def test_total_pods_property(self):
        """Test total_pods calculation."""
        overview = ClusterOverviewData(
            cluster_version="v1.28.0",
            namespaces=[
                NamespaceOverview(name="ns1", pod_count=10),
                NamespaceOverview(name="ns2", pod_count=5),
                NamespaceOverview(name="ns3", pod_count=8)
            ]
        )
        assert overview.total_pods == 23

    def test_healthy_percentage_property(self):
        """Test healthy_percentage calculation."""
        overview = ClusterOverviewData(
            cluster_version="v1.28.0",
            workloads=WorkloadSummary(
                deployments=[
                    DeploymentOverview(name="d1", namespace="ns", replicas=2, ready_replicas=2),
                    DeploymentOverview(name="d2", namespace="ns", replicas=3, ready_replicas=1)
                ]
            )
        )
        assert overview.healthy_percentage == 50.0
        
        # Test with no workloads
        empty_overview = ClusterOverviewData(cluster_version="v1.28.0")
        assert empty_overview.healthy_percentage == 100.0

    def test_get_namespaces_by_pod_count(self):
        """Test namespace sorting by pod count."""
        overview = ClusterOverviewData(
            cluster_version="v1.28.0",
            namespaces=[
                NamespaceOverview(name="small", pod_count=2),
                NamespaceOverview(name="large", pod_count=20),
                NamespaceOverview(name="medium", pod_count=10)
            ]
        )
        
        sorted_ns = overview.get_namespaces_by_pod_count()
        assert sorted_ns[0].name == "large"
        assert sorted_ns[1].name == "medium"
        assert sorted_ns[2].name == "small"
        
        # Test with limit
        top_2 = overview.get_namespaces_by_pod_count(limit=2)
        assert len(top_2) == 2
        assert top_2[0].name == "large"

    def test_get_resource_summary(self):
        """Test resource summary dictionary."""
        overview = ClusterOverviewData(
            cluster_version="v1.28.0",
            resource_counts=ResourceCounts(
                services=10,
                ingresses=2,
                configmaps=20
            )
        )
        
        summary = overview.get_resource_summary()
        assert isinstance(summary, dict)
        assert summary["Services"] == 10
        assert summary["Ingresses"] == 2
        assert summary["ConfigMaps"] == 20