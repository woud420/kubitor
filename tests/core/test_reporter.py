"""Tests for cluster reporter."""

import pytest
import json
import yaml
from unittest.mock import Mock, MagicMock

from src.core.reporter import ClusterReporter
from src.model.report import ReportFormat
from src.model.kubernetes import K8sResource


class TestClusterReporter:
    """Test ClusterReporter functionality."""
    
    def test_reporter_initialization(self):
        """Test ClusterReporter initialization."""
        mock_client = Mock()
        reporter = ClusterReporter(mock_client)
        
        assert reporter.client == mock_client
        assert reporter.helm_client is not None
        assert reporter.upgrade_advisor is not None
    
    def test_generate_report_json(self):
        """Test generating JSON report."""
        mock_client = Mock()
        mock_client.get_version.return_value = {
            "serverVersion": {
                "gitVersion": "v1.28.0",
                "major": "1",
                "minor": "28",
                "platform": "linux/amd64"
            }
        }
        mock_client.get_json.return_value = {"items": []}
        
        reporter = ClusterReporter(mock_client)
        result = reporter.generate_report([], ReportFormat.JSON)
        
        # Verify it's valid JSON
        data = json.loads(result)
        assert "cluster_info" in data
        assert "resources" in data
        assert "timestamp" in data
    
    def test_generate_report_yaml(self):
        """Test generating YAML report."""
        mock_client = Mock()
        mock_client.get_version.return_value = {
            "serverVersion": {
                "gitVersion": "v1.28.0",
                "major": "1",
                "minor": "28",
                "platform": "linux/amd64"
            }
        }
        mock_client.get_json.return_value = {"items": []}
        
        reporter = ClusterReporter(mock_client)
        result = reporter.generate_report([], ReportFormat.YAML)
        
        # Verify it's valid YAML
        data = yaml.safe_load(result)
        assert "cluster_info" in data
        assert "resources" in data
        assert "timestamp" in data
    
    def test_generate_report_text(self):
        """Test generating text report."""
        mock_client = Mock()
        mock_client.get_version.return_value = {
            "serverVersion": {
                "gitVersion": "v1.28.0",
                "major": "1",
                "minor": "28",
                "platform": "linux/amd64"
            }
        }
        mock_client.get_json.return_value = {"items": []}
        
        reporter = ClusterReporter(mock_client)
        result = reporter.generate_report([], ReportFormat.TEXT)
        
        # Verify it contains expected text sections
        assert "KUBERNETES CLUSTER REPORT" in result
        assert "CLUSTER INFORMATION" in result
        assert "Server Version: v1.28.0" in result
    
    def test_format_handlers_dispatch(self):
        """Test that format handlers use dictionary dispatch."""
        mock_client = Mock()
        mock_client.get_version.return_value = None
        mock_client.get_json.return_value = {"items": []}
        
        reporter = ClusterReporter(mock_client)
        
        # Test each format type
        for format_type in [ReportFormat.JSON, ReportFormat.YAML, ReportFormat.TEXT]:
            result = reporter.generate_report([], format_type)
            assert isinstance(result, str)
            assert len(result) > 0
    
    def test_system_namespace_detection(self):
        """Test system namespace detection."""
        mock_client = Mock()
        reporter = ClusterReporter(mock_client)
        
        # Test system namespaces
        assert reporter._is_system_namespace("kube-system") is True
        assert reporter._is_system_namespace("kube-public") is True
        assert reporter._is_system_namespace("eks-auth") is True
        assert reporter._is_system_namespace("istio-system") is True
        assert reporter._is_system_namespace("prometheus-system") is True
        
        # Test user namespaces
        assert reporter._is_system_namespace("production") is False
        assert reporter._is_system_namespace("staging") is False
        assert reporter._is_system_namespace("my-app") is False