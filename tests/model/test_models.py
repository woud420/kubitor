"""Unit tests for Pydantic models."""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.model.cluster import ClusterInfo, NodeInfo, ClusterVersion
from src.model.kubernetes import K8sResource, ResourceType


class TestClusterModels:
    """Test cluster-related models."""

    def test_node_info_creation(self):
        """Test NodeInfo model creation."""
        node = NodeInfo(
            name="test-node",
            status="Ready",
            roles="worker",
            version="v1.28.0",
            os="Amazon Linux 2",
            container_runtime="containerd://1.7.0",
        )

        assert node.name == "test-node"
        assert node.status == "Ready"
        assert node.roles == "worker"
        assert node.version == "v1.28.0"

    def test_cluster_version_creation(self):
        """Test ClusterVersion model creation."""
        version = ClusterVersion(
            major="1", minor="28", gitVersion="v1.28.0", platform="linux/amd64"
        )

        assert version.major == "1"
        assert version.minor == "28"
        assert version.git_version == "v1.28.0"
        assert version.platform == "linux/amd64"

    def test_cluster_version_alias_handling(self):
        """Test ClusterVersion handles gitVersion alias."""
        # Test with alias
        version1 = ClusterVersion(major="1", minor="28", gitVersion="v1.28.0")

        # Test with field name
        version2 = ClusterVersion(major="1", minor="28", git_version="v1.28.0")

        assert version1.git_version == "v1.28.0"
        assert version2.git_version == "v1.28.0"

    def test_cluster_info_creation(self):
        """Test ClusterInfo model creation."""
        server_version = ClusterVersion(major="1", minor="28", gitVersion="v1.28.0")

        node = NodeInfo(
            name="test-node",
            status="Ready",
            roles="worker",
            version="v1.28.0",
            os="Amazon Linux 2",
            container_runtime="containerd://1.7.0",
        )

        cluster_info = ClusterInfo(server_version=server_version, nodes=[node])

        assert cluster_info.server_version.git_version == "v1.28.0"
        assert len(cluster_info.nodes) == 1
        assert cluster_info.nodes[0].name == "test-node"

    def test_cluster_info_defaults(self):
        """Test ClusterInfo model with defaults."""
        cluster_info = ClusterInfo()

        assert cluster_info.server_version is None
        assert cluster_info.client_version is None
        assert cluster_info.nodes == []


class TestKubernetesModels:
    """Test Kubernetes resource models."""

    def test_resource_type_creation(self):
        """Test ResourceType model creation."""
        resource_type = ResourceType(
            name="pods", kind="Pod", namespaced=True, api_group="", version="v1"
        )

        assert resource_type.name == "pods"
        assert resource_type.kind == "Pod"
        assert resource_type.namespaced is True
        assert resource_type.api_group == ""
        assert resource_type.version == "v1"

    def test_k8s_resource_creation(self):
        """Test K8sResource model creation."""
        resource = K8sResource(
            api_version="v1",
            kind="Pod",
            metadata={"name": "test-pod", "namespace": "default", "labels": {"app": "test"}},
            spec={"containers": [{"name": "nginx", "image": "nginx:1.21"}]},
            status={"phase": "Running"},
        )

        assert resource.api_version == "v1"
        assert resource.kind == "Pod"
        assert resource.name == "test-pod"
        assert resource.namespace == "default"
        assert resource.labels == {"app": "test"}

    def test_k8s_resource_computed_properties(self):
        """Test K8sResource computed properties."""
        resource = K8sResource(
            api_version="v1",
            kind="Pod",
            metadata={
                "name": "test-pod",
                "namespace": "default",
                "labels": {"app.kubernetes.io/managed-by": "Helm"},
            },
        )

        assert resource.name == "test-pod"
        assert resource.namespace == "default"
        assert resource.is_helm_managed is True

    def test_k8s_resource_cluster_scoped(self):
        """Test K8sResource cluster-scoped resource."""
        resource = K8sResource(
            api_version="v1", kind="Namespace", metadata={"name": "test-namespace"}
        )

        assert resource.name == "test-namespace"
        assert resource.namespace is None
        assert resource.is_helm_managed is False

    def test_k8s_resource_helm_detection(self):
        """Test K8sResource Helm detection."""
        # Test with Helm label
        helm_resource = K8sResource(
            api_version="v1",
            kind="Service",
            metadata={"name": "test-service", "labels": {"app.kubernetes.io/managed-by": "Helm"}},
        )

        # Test with Helm annotation
        helm_resource2 = K8sResource(
            api_version="v1",
            kind="Service",
            metadata={
                "name": "test-service2",
                "annotations": {"meta.helm.sh/release-name": "my-release"},
            },
        )

        # Test without Helm indicators
        non_helm_resource = K8sResource(
            api_version="v1",
            kind="Service",
            metadata={"name": "test-service3", "labels": {"app": "test"}},
        )

        assert helm_resource.is_helm_managed is True
        assert helm_resource2.is_helm_managed is True
        assert non_helm_resource.is_helm_managed is False

    def test_k8s_resource_helm_release_name(self):
        """Test K8sResource Helm release name extraction."""
        resource = K8sResource(
            api_version="v1",
            kind="Service",
            metadata={
                "name": "test-service",
                "annotations": {"meta.helm.sh/release-name": "my-app"},
                "labels": {"app.kubernetes.io/managed-by": "Helm"},
            },
        )

        assert resource.helm_release == "my-app"
