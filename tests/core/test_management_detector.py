"""Tests for dictionary dispatch pattern refactoring."""

import pytest
from unittest.mock import Mock, MagicMock
import json
import yaml

from src.core.management_detector import ManagementDetector
from src.core.inventory import InventoryCollector
from src.model.inventory import Application
from src.model.management import ManagementTool
from src.core.reporter import ClusterReporter
from src.model.report import ReportFormat, ClusterReport
from datetime import datetime


class TestDictionaryDispatchPatterns:
    """Test that dictionary dispatch patterns work correctly."""
    
    def test_management_detector_dispatch(self):
        """Test management detector uses dispatch pattern correctly."""
        detector = ManagementDetector()
        
        # Test Helm detection
        helm_resource = {
            "metadata": {
                "labels": {"app.kubernetes.io/managed-by": "Helm"}
            }
        }
        result = detector.detect(helm_resource)
        assert result.tool == ManagementTool.HELM
        
        # Test ArgoCD detection
        argo_resource = {
            "metadata": {
                "labels": {"argocd.argoproj.io/instance": "app"}
            }
        }
        result = detector.detect(argo_resource)
        assert result.tool == ManagementTool.ARGOCD
        
        # Test manual (no match)
        manual_resource = {
            "metadata": {
                "labels": {"app": "test"}
            }
        }
        result = detector.detect(manual_resource)
        assert result.tool == ManagementTool.MANUAL
    
    def test_inventory_replica_extractors(self):
        """Test inventory uses replica extractors correctly."""
        mock_client = Mock()
        collector = InventoryCollector(mock_client)
        
        # Test Deployment replicas
        deployment = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {"replicas": 3},
            "status": {"readyReplicas": 2}
        }
        app = collector._create_application(deployment, "Deployment")
        assert app.replicas == 2
        
        # Test DaemonSet replicas
        daemonset = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {},
            "status": {"numberReady": 5}
        }
        app = collector._create_application(daemonset, "DaemonSet")
        assert app.replicas == 5
        
        # Test Pod replicas
        pod_running = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {},
            "status": {"phase": "Running"}
        }
        app = collector._create_application(pod_running, "Pod")
        assert app.replicas == 1
        
        pod_pending = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {},
            "status": {"phase": "Pending"}
        }
        app = collector._create_application(pod_pending, "Pod")
        assert app.replicas == 0
    
    def test_inventory_pod_spec_extractors(self):
        """Test inventory uses pod spec extractors correctly."""
        mock_client = Mock()
        collector = InventoryCollector(mock_client)
        
        # Test Deployment pod spec
        deployment = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {"name": "app", "image": "nginx:latest"}
                        ]
                    }
                }
            },
            "status": {}
        }
        app = collector._create_application(deployment, "Deployment")
        assert app.containers == ["app"]
        assert app.images == ["nginx:latest"]
        
        # Test Pod spec
        pod = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {
                "containers": [
                    {"name": "main", "image": "alpine:3.18"}
                ]
            },
            "status": {}
        }
        app = collector._create_application(pod, "Pod")
        assert app.containers == ["main"]
        assert app.images == ["alpine:3.18"]
        
        # Test CronJob pod spec
        cronjob = {
            "metadata": {"name": "test", "namespace": "default", "labels": {}},
            "spec": {
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {"name": "job", "image": "busybox:1.35"}
                                ]
                            }
                        }
                    }
                }
            },
            "status": {}
        }
        app = collector._create_application(cronjob, "CronJob")
        assert app.containers == ["job"]
        assert app.images == ["busybox:1.35"]
    
    def test_cluster_name_extraction(self):
        """Test cluster name extraction with label dispatch."""
        mock_client = Mock()
        mock_client.context = None
        collector = InventoryCollector(mock_client)
        
        # Test EKS cluster name
        mock_client.get_json.return_value = {
            "items": [{
                "metadata": {
                    "labels": {
                        "eks.amazonaws.com/cluster-name": "my-eks-cluster"
                    }
                }
            }]
        }
        name = collector._get_cluster_name()
        assert name == "my-eks-cluster"
        
        # Test AKS cluster name
        mock_client.get_json.return_value = {
            "items": [{
                "metadata": {
                    "labels": {
                        "kubernetes.azure.com/cluster": "my-aks-cluster"
                    }
                }
            }]
        }
        name = collector._get_cluster_name()
        assert name == "my-aks-cluster"
        
        # Test GKE cluster name
        mock_client.get_json.return_value = {
            "items": [{
                "metadata": {
                    "labels": {
                        "cloud.google.com/gke-cluster-name": "my-gke-cluster"
                    }
                }
            }]
        }
        name = collector._get_cluster_name()
        assert name == "my-gke-cluster"
    
    def test_reporter_format_handlers(self):
        """Test reporter uses format handlers correctly."""
        mock_client = Mock()
        mock_client.get_version.return_value = {
            "serverVersion": {
                "gitVersion": "v1.28.0", 
                "platform": "linux/amd64",
                "major": "1",
                "minor": "28"
            }
        }
        mock_client.get_json.return_value = {"items": []}
        
        reporter = ClusterReporter(mock_client)
        
        # Test JSON format
        result = reporter.generate_report([], ReportFormat.JSON)
        data = json.loads(result)
        assert "cluster_info" in data
        assert "resources" in data
        
        # Test YAML format
        result = reporter.generate_report([], ReportFormat.YAML)
        data = yaml.safe_load(result)
        assert "cluster_info" in data
        assert "resources" in data
        
        # Test TEXT format (should not raise an error)
        result = reporter.generate_report([], ReportFormat.TEXT)
        assert isinstance(result, str)
        assert "KUBERNETES CLUSTER REPORT" in result