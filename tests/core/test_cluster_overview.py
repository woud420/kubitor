"""Tests for cluster overview functionality."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from src.core.cluster_overview import ClusterOverview
from src.model.overview import (
    ClusterOverviewData,
    NamespaceOverview,
    PodStatus,
    DeploymentOverview,
    StatefulSetOverview,
    DaemonSetOverview,
    JobOverview,
    CronJobOverview,
    WorkloadSummary,
    ResourceCounts,
)


class TestClusterOverview:
    """Test ClusterOverview class."""

    def test_init(self):
        """Test ClusterOverview initialization."""
        mock_client = Mock()
        overview = ClusterOverview(mock_client)
        assert overview.client == mock_client

    def test_get_overview_basic(self):
        """Test get_overview returns ClusterOverviewData."""
        mock_client = Mock()
        
        # Mock cluster info
        mock_cluster_info = Mock()
        mock_cluster_info.server_version.git_version = "v1.28.0"
        mock_cluster_info.server_version.platform = "linux/amd64"
        mock_cluster_info.nodes = []
        mock_client.get_cluster_info.return_value = mock_cluster_info
        
        # Mock namespaces
        mock_client.get_json.return_value = {"items": []}
        
        overview = ClusterOverview(mock_client)
        result = overview.get_overview()
        
        assert isinstance(result, ClusterOverviewData)
        assert result.cluster_version == "v1.28.0"
        assert result.platform == "linux/amd64"
        assert result.node_count == 0

    def test_get_namespace_overviews(self):
        """Test namespace overview collection."""
        mock_client = Mock()
        
        # Mock namespace data
        namespaces_data = {
            "items": [
                {
                    "metadata": {"name": "default", "labels": {}},
                    "status": {"phase": "Active"}
                },
                {
                    "metadata": {"name": "kube-system", "labels": {}},
                    "status": {"phase": "Active"}
                }
            ]
        }
        
        # Mock pods data
        pods_data = {
            "items": [
                {
                    "metadata": {"namespace": "default"},
                    "status": {"phase": "Running"}
                },
                {
                    "metadata": {"namespace": "default"},
                    "status": {"phase": "Pending"}
                },
                {
                    "metadata": {"namespace": "kube-system"},
                    "status": {"phase": "Running"}
                }
            ]
        }
        
        mock_client.get_json.side_effect = [namespaces_data, pods_data]
        
        overview = ClusterOverview(mock_client)
        namespaces = overview._get_namespace_overviews()
        
        assert len(namespaces) == 2
        assert isinstance(namespaces[0], NamespaceOverview)
        
        # Check default namespace
        default_ns = next(ns for ns in namespaces if ns.name == "default")
        assert default_ns.pod_count == 2
        assert default_ns.pod_status.running == 1
        assert default_ns.pod_status.pending == 1
        
        # Check kube-system namespace
        kube_ns = next(ns for ns in namespaces if ns.name == "kube-system")
        assert kube_ns.pod_count == 1
        assert kube_ns.pod_status.running == 1

    def test_get_workload_summary(self):
        """Test workload summary collection."""
        mock_client = Mock()
        
        # Mock deployments
        deployments_data = {
            "items": [
                {
                    "metadata": {"name": "app1", "namespace": "default"},
                    "spec": {"replicas": 3},
                    "status": {"readyReplicas": 3, "availableReplicas": 3}
                }
            ]
        }
        
        # Mock statefulsets
        statefulsets_data = {
            "items": [
                {
                    "metadata": {"name": "db1", "namespace": "default"},
                    "spec": {"replicas": 2},
                    "status": {"readyReplicas": 2}
                }
            ]
        }
        
        # Mock other workload types (empty)
        empty_data = {"items": []}
        
        mock_client.get_json.side_effect = [
            deployments_data,
            statefulsets_data,
            empty_data,  # daemonsets
            empty_data,  # jobs
            empty_data,  # cronjobs
        ]
        
        overview = ClusterOverview(mock_client)
        workloads = overview._get_workload_summary()
        
        assert isinstance(workloads, WorkloadSummary)
        assert len(workloads.deployments) == 1
        assert workloads.deployments[0].name == "app1"
        assert workloads.deployments[0].is_healthy is True
        
        assert len(workloads.statefulsets) == 1
        assert workloads.statefulsets[0].name == "db1"
        assert workloads.statefulsets[0].is_healthy is True

    def test_get_resource_counts(self):
        """Test resource counting."""
        mock_client = Mock()
        
        # Mock various resource responses
        mock_client.get_json.side_effect = [
            {"items": [1, 2, 3]},  # 3 services
            {"items": [1]},         # 1 ingress
            {"items": [1, 2, 3, 4, 5]},  # 5 configmaps
            {"items": [1, 2]},      # 2 secrets
            {"items": []},          # 0 PVCs
            {"items": []},          # 0 PVs
            {"items": [1, 2, 3]},   # 3 service accounts
            {"items": []},          # 0 network policies
        ]
        
        overview = ClusterOverview(mock_client)
        counts = overview._get_resource_counts()
        
        assert isinstance(counts, ResourceCounts)
        assert counts.services == 3
        assert counts.ingresses == 1
        assert counts.configmaps == 5
        assert counts.secrets == 2
        assert counts.service_accounts == 3
        assert counts.total == 14

    def test_format_overview_text(self):
        """Test text formatting of overview."""
        mock_client = Mock()
        
        overview_data = ClusterOverviewData(
            cluster_version="v1.28.0",
            platform="linux/amd64",
            node_count=3,
            namespace_count=2,
            namespaces=[
                NamespaceOverview(
                    name="default",
                    status="Active",
                    pod_count=5,
                    pod_status=PodStatus(running=4, pending=1)
                )
            ],
            workloads=WorkloadSummary(
                deployments=[
                    DeploymentOverview(
                        name="app1",
                        namespace="default",
                        replicas=3,
                        ready_replicas=3
                    )
                ]
            ),
            resource_counts=ResourceCounts(services=10, configmaps=20)
        )
        
        overview = ClusterOverview(mock_client)
        text_output = overview.format_overview(overview_data, format="text")
        
        assert "KUBERNETES CLUSTER OVERVIEW" in text_output
        assert "v1.28.0" in text_output
        assert "linux/amd64" in text_output
        assert "default" in text_output
        assert "Running:4" in text_output

    def test_format_overview_json(self):
        """Test JSON formatting of overview."""
        mock_client = Mock()
        
        overview_data = ClusterOverviewData(
            cluster_version="v1.28.0",
            platform="linux/amd64",
            node_count=1
        )
        
        overview = ClusterOverview(mock_client)
        json_output = overview.format_overview(overview_data, format="json")
        
        import json
        parsed = json.loads(json_output)
        assert parsed["cluster_version"] == "v1.28.0"
        assert parsed["platform"] == "linux/amd64"
        assert parsed["node_count"] == 1

    def test_format_overview_yaml(self):
        """Test YAML formatting of overview."""
        mock_client = Mock()
        
        overview_data = ClusterOverviewData(
            cluster_version="v1.28.0",
            platform="linux/amd64",
            node_count=1
        )
        
        overview = ClusterOverview(mock_client)
        yaml_output = overview.format_overview(overview_data, format="yaml")
        
        import yaml
        parsed = yaml.safe_load(yaml_output)
        assert parsed["cluster_version"] == "v1.28.0"
        assert parsed["platform"] == "linux/amd64"
        assert parsed["node_count"] == 1

    def test_format_overview_invalid_format(self):
        """Test invalid format raises ValueError."""
        mock_client = Mock()
        overview_data = ClusterOverviewData(cluster_version="v1.28.0")
        
        overview = ClusterOverview(mock_client)
        with pytest.raises(ValueError, match="Unsupported format"):
            overview.format_overview(overview_data, format="invalid")