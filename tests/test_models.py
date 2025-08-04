"""Test data models."""

import pytest
from src.model.kubernetes import K8sResource, ResourceType
from src.model.cluster import NodeInfo, ClusterVersion, UpgradeSuggestion
from src.model.export import ExportFormat, OrganizeBy
from src.model.report import ReportFormat, HelmRelease


class TestK8sResource:
    def test_resource_creation(self):
        """Test creating a K8s resource."""
        resource = K8sResource(
            api_version="v1",
            kind="Pod",
            metadata={"name": "test-pod", "namespace": "default"},
            spec={"containers": []},
        )

        assert resource.name == "test-pod"
        assert resource.namespace == "default"
        assert resource.kind == "Pod"

    def test_helm_managed_resource(self):
        """Test Helm-managed resource detection."""
        resource = K8sResource(
            api_version="v1",
            kind="Pod",
            metadata={"name": "test-pod", "labels": {"helm.sh/release": "my-release"}},
        )

        assert resource.is_helm_managed is True

    def test_non_helm_managed_resource(self):
        """Test non-Helm resource detection."""
        resource = K8sResource(api_version="v1", kind="Pod", metadata={"name": "test-pod"})

        assert resource.is_helm_managed is False


class TestNodeInfo:
    def test_node_info_creation(self):
        """Test creating node info."""
        node = NodeInfo(
            name="node1",
            status="Ready",
            roles="control-plane",
            version="v1.28.0",
            os="Ubuntu 22.04",
            container_runtime="containerd://1.7.0",
        )

        assert node.name == "node1"
        assert node.status == "Ready"
        assert node.version == "v1.28.0"


class TestUpgradeSuggestion:
    def test_upgrade_suggestion_creation(self):
        """Test creating upgrade suggestion."""
        suggestion = UpgradeSuggestion(
            current_version="v1.27.0",
            suggested_next_version="1.28",
            upgrade_notes=["Feature X is GA"],
            api_deprecations=["API Y deprecated"],
            required_actions=["Migrate from X to Y"],
        )

        assert suggestion.current_version == "v1.27.0"
        assert suggestion.suggested_next_version == "1.28"
        assert len(suggestion.upgrade_notes) == 1


class TestEnums:
    def test_export_format_enum(self):
        """Test export format enum."""
        assert ExportFormat.YAML == "yaml"
        assert ExportFormat.JSON == "json"

    def test_organize_by_enum(self):
        """Test organize by enum."""
        assert OrganizeBy.SERVICE == "service"
        assert OrganizeBy.NAMESPACE == "namespace"
        assert OrganizeBy.TYPE == "type"

    def test_report_format_enum(self):
        """Test report format enum."""
        assert ReportFormat.TEXT == "text"
        assert ReportFormat.JSON == "json"
        assert ReportFormat.YAML == "yaml"
