"""Tests for inventory functionality."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from src.core.inventory import InventoryCollector
from src.core.management_detector import ManagementDetector
from src.model.inventory import Application, ClusterInventory
from src.model.management import ManagementTool, ManagementInfo


class TestManagementDetector:
    """Test management detection."""
    
    def test_detect_helm_managed(self):
        """Test detection of Helm-managed resources."""
        detector = ManagementDetector()
        
        resource = {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/managed-by": "Helm",
                    "app.kubernetes.io/instance": "nginx-ingress",
                    "helm.sh/chart": "nginx-ingress-4.7.1",
                    "app.kubernetes.io/version": "1.8.1"
                }
            }
        }
        
        result = detector.detect(resource)
        assert result.tool == ManagementTool.HELM
        assert result.release_name == "nginx-ingress"
        assert result.chart == "nginx-ingress-4.7.1"
    
    def test_detect_argocd_managed(self):
        """Test detection of ArgoCD-managed resources."""
        detector = ManagementDetector()
        
        resource = {
            "metadata": {
                "labels": {
                    "argocd.argoproj.io/instance": "payment-service"
                }
            }
        }
        
        result = detector.detect(resource)
        assert result.tool == ManagementTool.ARGOCD
        assert result.release_name == "payment-service"
    
    def test_detect_flux_managed(self):
        """Test detection of Flux-managed resources."""
        detector = ManagementDetector()
        
        resource = {
            "metadata": {
                "labels": {
                    "kustomize.toolkit.fluxcd.io/name": "config-reloader",
                    "kustomize.toolkit.fluxcd.io/namespace": "flux-system"
                }
            }
        }
        
        result = detector.detect(resource)
        assert result.tool == ManagementTool.FLUX
        assert result.release_name == "config-reloader"
    
    def test_detect_operator_managed(self):
        """Test detection of operator-managed resources."""
        detector = ManagementDetector()
        
        resource = {
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "example.io/v1",
                        "kind": "CustomApp",
                        "name": "my-app",
                        "controller": True
                    }
                ]
            }
        }
        
        result = detector.detect(resource)
        assert result.tool == ManagementTool.OPERATOR
        assert result.details["operator_kind"] == "CustomApp"
        assert result.details["operator_name"] == "my-app"
    
    def test_detect_manual(self):
        """Test detection of manually deployed resources."""
        detector = ManagementDetector()
        
        resource = {
            "metadata": {
                "labels": {
                    "app": "test-app"
                }
            }
        }
        
        result = detector.detect(resource)
        assert result.tool == ManagementTool.MANUAL


class TestInventoryCollector:
    """Test inventory collection."""
    
    def test_system_namespace_filtering(self):
        """Test filtering of system namespaces."""
        mock_client = Mock()
        collector = InventoryCollector(mock_client, include_system=False)
        
        # Test system namespace detection
        app = Application(
            name="coredns",
            namespace="kube-system",
            kind="Deployment"
        )
        assert collector._is_system_resource(app) is True
        
        # Test user namespace
        app = Application(
            name="my-app",
            namespace="production",
            kind="Deployment"
        )
        assert collector._is_system_resource(app) is False
        
        # Test EKS system namespace
        app = Application(
            name="aws-node",
            namespace="eks-managed",
            kind="DaemonSet"
        )
        assert collector._is_system_resource(app) is True
    
    def test_create_application(self):
        """Test creating Application from resource."""
        mock_client = Mock()
        collector = InventoryCollector(mock_client)
        
        resource = {
            "metadata": {
                "name": "test-app",
                "namespace": "default",
                "labels": {
                    "app.kubernetes.io/version": "1.2.3",
                    "app.kubernetes.io/managed-by": "Helm"
                }
            },
            "spec": {
                "replicas": 3,
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "app",
                                "image": "myapp:1.2.3"
                            }
                        ]
                    }
                }
            },
            "status": {
                "readyReplicas": 2
            }
        }
        
        app = collector._create_application(resource, "Deployment")
        
        assert app is not None
        assert app.name == "test-app"
        assert app.namespace == "default"
        assert app.kind == "Deployment"
        assert app.version == "1.2.3"
        assert app.replicas == 2
        assert app.containers == ["app"]
        assert app.images == ["myapp:1.2.3"]
        assert app.management.tool == ManagementTool.HELM
    
    @patch('src.core.inventory.InventoryCollector._get_resources')
    def test_collect_inventory(self, mock_get_resources):
        """Test full inventory collection."""
        mock_client = Mock()
        mock_client.context = "test-cluster"
        mock_client.get_version.return_value = {
            "serverVersion": {"gitVersion": "v1.28.0"}
        }
        mock_client.get_json.return_value = {
            "items": [
                {"metadata": {"labels": {}}}
            ]
        }
        
        # Mock deployments
        mock_get_resources.return_value = [
            {
                "metadata": {
                    "name": "test-app",
                    "namespace": "production",
                    "labels": {}
                },
                "spec": {"replicas": 1},
                "status": {"readyReplicas": 1}
            }
        ]
        
        collector = InventoryCollector(mock_client, include_system=False)
        inventory = collector.collect()
        
        assert isinstance(inventory, ClusterInventory)
        assert inventory.cluster_name == "test-cluster"
        assert inventory.kubernetes_version == "v1.28.0"
        assert inventory.node_count == 1