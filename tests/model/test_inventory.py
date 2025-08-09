"""Tests for inventory models."""

import pytest
from datetime import datetime

from src.model.inventory import Application, ClusterInventory
from src.model.management import ManagementTool, ManagementInfo


class TestApplication:
    """Test Application model."""
    
    def test_application_creation(self):
        """Test creating an Application instance."""
        app = Application(
            name="test-app",
            namespace="default",
            kind="Deployment",
            version="1.0.0",
            replicas=3,
            containers=["app"],
            images=["test:1.0.0"]
        )
        
        assert app.name == "test-app"
        assert app.namespace == "default"
        assert app.kind == "Deployment"
        assert app.version == "1.0.0"
        assert app.replicas == 3
        assert app.containers == ["app"]
        assert app.images == ["test:1.0.0"]
    
    def test_application_display_name(self):
        """Test display name property."""
        # Without management info
        app = Application(name="test-app", namespace="default", kind="Deployment")
        assert app.display_name == "test-app"
        
        # With management info
        mgmt = ManagementInfo(ManagementTool.HELM, {"release_name": "my-release"})
        app = Application(name="test-app", namespace="default", kind="Deployment", management=mgmt)
        assert app.display_name == "my-release"
    
    def test_application_management_tool(self):
        """Test management tool property."""
        # Without management info
        app = Application(name="test-app", namespace="default", kind="Deployment")
        assert app.management_tool == "Unknown"
        
        # With management info
        mgmt = ManagementInfo(ManagementTool.HELM)
        app = Application(name="test-app", namespace="default", kind="Deployment", management=mgmt)
        assert app.management_tool == "Helm"


class TestClusterInventory:
    """Test ClusterInventory model."""
    
    def test_cluster_inventory_creation(self):
        """Test creating a ClusterInventory instance."""
        inventory = ClusterInventory(
            cluster_name="test-cluster",
            kubernetes_version="v1.28.0",
            node_count=3,
            scan_timestamp=datetime.now()
        )
        
        assert inventory.cluster_name == "test-cluster"
        assert inventory.kubernetes_version == "v1.28.0"
        assert inventory.node_count == 3
        assert inventory.total_applications == 0
    
    def test_cluster_inventory_with_applications(self):
        """Test ClusterInventory with applications."""
        app1 = Application(name="app1", namespace="default", kind="Deployment")
        app2 = Application(name="app2", namespace="production", kind="StatefulSet")
        
        inventory = ClusterInventory(
            cluster_name="test-cluster",
            kubernetes_version="v1.28.0",
            node_count=3,
            scan_timestamp=datetime.now(),
            applications=[app1, app2]
        )
        
        assert inventory.total_applications == 2
        assert len(inventory.applications) == 2
    
    def test_applications_by_namespace(self):
        """Test grouping applications by namespace."""
        app1 = Application(name="app1", namespace="default", kind="Deployment")
        app2 = Application(name="app2", namespace="production", kind="StatefulSet")
        app3 = Application(name="app3", namespace="default", kind="Pod")
        
        inventory = ClusterInventory(
            cluster_name="test-cluster",
            kubernetes_version="v1.28.0",
            node_count=3,
            scan_timestamp=datetime.now(),
            applications=[app1, app2, app3]
        )
        
        by_namespace = inventory.applications_by_namespace
        assert len(by_namespace["default"]) == 2
        assert len(by_namespace["production"]) == 1
    
    def test_applications_by_management(self):
        """Test grouping applications by management tool."""
        mgmt_helm = ManagementInfo(ManagementTool.HELM)
        mgmt_argo = ManagementInfo(ManagementTool.ARGOCD)
        
        app1 = Application(name="app1", namespace="default", kind="Deployment", management=mgmt_helm)
        app2 = Application(name="app2", namespace="default", kind="StatefulSet", management=mgmt_argo)
        app3 = Application(name="app3", namespace="default", kind="Pod", management=mgmt_helm)
        
        inventory = ClusterInventory(
            cluster_name="test-cluster",
            kubernetes_version="v1.28.0",
            node_count=3,
            scan_timestamp=datetime.now(),
            applications=[app1, app2, app3]
        )
        
        by_mgmt = inventory.applications_by_management
        assert len(by_mgmt["Helm"]) == 2
        assert len(by_mgmt["ArgoCD"]) == 1