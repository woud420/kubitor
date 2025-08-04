"""Tests for scan service."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.api.scan_service import ScanService


@pytest.mark.unit
class TestScanService:
    """Unit tests for ScanService."""

    def test_init_with_default_database_url(self):
        """Test service initialization with default database URL."""
        with patch("src.api.scan_service.DatabaseConnection") as mock_db_conn:
            service = ScanService()

            # Should use default SQLite database
            mock_db_conn.assert_called_once()
            call_args = mock_db_conn.call_args[0]
            assert "sqlite:///" in call_args[0]
            assert "/.k8s-scanner/history.db" in call_args[0]

    def test_init_with_custom_database_url(self):
        """Test service initialization with custom database URL."""
        custom_url = "postgresql://user:pass@localhost/test"

        with patch("src.api.scan_service.DatabaseConnection") as mock_db_conn:
            service = ScanService(database_url=custom_url)
            mock_db_conn.assert_called_once_with(custom_url)

    @patch("src.api.scan_service.K8sClient")
    @patch("src.api.scan_service.ResourceScanner")
    def test_perform_scan_success(self, mock_scanner_class, mock_client_class):
        """Test successful scan performance."""
        # Mock dependencies
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_cluster_info = Mock()
        mock_cluster_info.dict.return_value = {"nodes": ["node1"], "version": "v1.28.0"}
        mock_cluster_info.server_version.git_version = "v1.28.0"
        mock_cluster_info.nodes = ["node1"]
        mock_client.get_cluster_info.return_value = mock_cluster_info

        mock_scanner = Mock()
        mock_scanner_class.return_value = mock_scanner

        # Mock resources
        mock_resource = Mock()
        mock_resource.dict.return_value = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
        }
        mock_resource.api_version = "v1"
        mock_resource.kind = "Pod"
        mock_resource.name = "test-pod"
        mock_resource.namespace = "default"
        mock_resource.labels = {}
        mock_resource.annotations = {}

        mock_scanner.scan_all_namespaces.return_value = [mock_resource]

        # Mock repositories
        with patch("src.api.scan_service.DatabaseConnection"), patch(
            "src.api.scan_service.ScanRepository"
        ) as mock_scan_repo_class, patch(
            "src.api.scan_service.ResourceRepository"
        ) as mock_resource_repo_class, patch(
            "src.api.scan_service.ChangeRepository"
        ) as mock_change_repo_class:
            # Mock repository instances
            mock_scan_repo = Mock()
            mock_scan_repo_class.return_value = mock_scan_repo
            mock_scan_repo.create_scan_record.return_value = 123
            mock_scan_repo.get_recent_scans.return_value = []  # No previous scans

            mock_resource_repo = Mock()
            mock_resource_repo_class.return_value = mock_resource_repo
            mock_resource_repo.bulk_create_resources.return_value = 1

            mock_change_repo = Mock()
            mock_change_repo_class.return_value = mock_change_repo

            # Create service and perform scan
            service = ScanService()
            result = service.perform_scan(context="test-context", namespace=None)

            # Verify result
            assert result["scan_id"] == 123
            assert result["cluster_context"] == "test-context"
            assert result["total_resources"] == 1
            assert result["changes_detected"] == 0

            # Verify repository calls
            mock_scan_repo.create_scan_record.assert_called_once()
            mock_resource_repo.bulk_create_resources.assert_called_once()

    def test_get_scan_history(self):
        """Test getting scan history."""
        with patch("src.api.scan_service.DatabaseConnection"), patch(
            "src.api.scan_service.ScanRepository"
        ) as mock_scan_repo_class:
            mock_scan_repo = Mock()
            mock_scan_repo_class.return_value = mock_scan_repo

            expected_scans = [
                {"id": 1, "cluster_context": "test", "total_resources": 10},
                {"id": 2, "cluster_context": "test", "total_resources": 12},
            ]
            mock_scan_repo.get_recent_scans.return_value = expected_scans

            service = ScanService()
            result = service.get_scan_history(
                context="test", namespace="default", days=30, limit=50
            )

            assert result == expected_scans
            mock_scan_repo.get_recent_scans.assert_called_once_with(
                cluster_context="test", namespace="default", days=30, limit=50
            )

    def test_get_scan_details(self):
        """Test getting detailed scan information."""
        with patch("src.api.scan_service.DatabaseConnection"), patch(
            "src.api.scan_service.ScanRepository"
        ) as mock_scan_repo_class, patch(
            "src.api.scan_service.ResourceRepository"
        ) as mock_resource_repo_class, patch(
            "src.api.scan_service.ChangeRepository"
        ) as mock_change_repo_class:
            # Mock repositories
            mock_scan_repo = Mock()
            mock_scan_repo_class.return_value = mock_scan_repo

            mock_resource_repo = Mock()
            mock_resource_repo_class.return_value = mock_resource_repo

            mock_change_repo = Mock()
            mock_change_repo_class.return_value = mock_change_repo

            # Mock data
            scan_data = {"id": 123, "cluster_context": "test", "total_resources": 5}
            resources_data = [
                {"id": 1, "kind": "Pod", "name": "pod1"},
                {"id": 2, "kind": "Service", "name": "svc1"},
            ]
            changes_data = [
                {"id": 1, "change_type": "created", "resource_id": 1},
                {"id": 2, "change_type": "updated", "resource_id": 2},
            ]

            mock_scan_repo.get_scan_by_id.return_value = scan_data
            mock_resource_repo.get_resources_by_scan.return_value = resources_data
            mock_change_repo.get_changes_by_resource.return_value = changes_data

            service = ScanService()
            result = service.get_scan_details(123)

            # Verify result structure
            assert result["id"] == 123
            assert result["resources"] == resources_data
            assert result["resource_count"] == 2
            assert result["change_count"] == 4  # 2 resources * 2 changes each

            # Verify repository calls
            mock_scan_repo.get_scan_by_id.assert_called_once_with(123)
            mock_resource_repo.get_resources_by_scan.assert_called_once_with(123)

    def test_get_scan_details_not_found(self):
        """Test getting details for non-existent scan."""
        with patch("src.api.scan_service.DatabaseConnection"), patch(
            "src.api.scan_service.ScanRepository"
        ) as mock_scan_repo_class:
            mock_scan_repo = Mock()
            mock_scan_repo_class.return_value = mock_scan_repo
            mock_scan_repo.get_scan_by_id.return_value = None

            service = ScanService()
            result = service.get_scan_details(999)

            assert result is None

    def test_cleanup_old_scans(self):
        """Test cleanup of old scans."""
        with patch("src.api.scan_service.DatabaseConnection"), patch(
            "src.api.scan_service.ScanRepository"
        ) as mock_scan_repo_class:
            mock_scan_repo = Mock()
            mock_scan_repo_class.return_value = mock_scan_repo
            mock_scan_repo.cleanup_old_scans.return_value = 5

            service = ScanService()
            result = service.cleanup_old_scans(keep_days=90)

            assert result["deleted_scans"] == 5
            assert "cutoff_date" in result
            mock_scan_repo.cleanup_old_scans.assert_called_once_with(90)

    def test_calculate_resource_hash(self):
        """Test resource hash calculation."""
        with patch("src.api.scan_service.DatabaseConnection"):
            service = ScanService()

            # Test data with volatile fields
            resource_data = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "test-pod",
                    "resourceVersion": "12345",  # Should be filtered out
                    "generation": 2,  # Should be filtered out
                    "managedFields": [],  # Should be filtered out
                    "creationTimestamp": "2023-01-01T00:00:00Z",  # Should be filtered out
                },
                "status": {  # Should be filtered out
                    "phase": "Running"
                },
                "spec": {"containers": [{"name": "test", "image": "nginx"}]},
            }

            hash1 = service._calculate_resource_hash(resource_data)

            # Same data but with different volatile fields
            resource_data2 = resource_data.copy()
            resource_data2["metadata"]["resourceVersion"] = "67890"
            resource_data2["metadata"]["generation"] = 3
            resource_data2["status"] = {"phase": "Pending"}

            hash2 = service._calculate_resource_hash(resource_data2)

            # Hashes should be the same since volatile fields are filtered
            assert hash1 == hash2

            # But changing spec should result in different hash
            resource_data3 = resource_data.copy()
            resource_data3["spec"]["containers"][0]["image"] = "apache"
            hash3 = service._calculate_resource_hash(resource_data3)

            assert hash1 != hash3

    def test_close(self):
        """Test service cleanup."""
        with patch("src.api.scan_service.DatabaseConnection") as mock_db_conn_class:
            mock_db_conn = Mock()
            mock_db_conn_class.return_value = mock_db_conn

            service = ScanService()
            service.close()

            mock_db_conn.close.assert_called_once()
