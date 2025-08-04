"""Unit tests for DAO layer."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.model.dao.scan_dao import ScanDAO
from src.model.dao.resource_dao import ResourceDAO
from src.model.dao.change_dao import ChangeDAO


class TestScanDAO:
    """Test ScanDAO functionality."""

    def test_create_scan_sqlite(self, mock_db_connection, sample_scan_data):
        """Test creating a scan record in SQLite."""
        # Setup
        mock_db_connection.database_url = "sqlite:///test.db"
        mock_db_connection.insert_returning_id.return_value = 1

        scan_dao = ScanDAO(mock_db_connection)

        # Execute
        scan_id = scan_dao.create_scan(
            cluster_context="test-cluster",
            namespace="default",
            total_resources=100,
            cluster_version="v1.28.0",
            node_count=3,
        )

        # Assert
        assert scan_id == 1
        mock_db_connection.insert_returning_id.assert_called_once()
        call_args = mock_db_connection.insert_returning_id.call_args
        assert call_args[0][0] == [
            "cluster_context",
            "namespace",
            "scan_type",
            "total_resources",
            "cluster_version",
            "node_count",
            "cluster_info",
        ]

    def test_create_scan_postgresql(self, mock_db_connection):
        """Test creating a scan record in PostgreSQL."""
        # Setup
        mock_db_connection.database_url = "postgresql://test"
        mock_db_connection.dialect = "postgresql"
        mock_db_connection.insert_returning_id.return_value = 1

        scan_dao = ScanDAO(mock_db_connection)

        # Execute
        scan_id = scan_dao.create_scan(
            cluster_context="test-cluster", cluster_info={"nodes": ["node1"]}
        )

        # Assert
        assert scan_id == 1
        mock_db_connection.insert_returning_id.assert_called_once()

    def test_find_by_id(self, mock_db_connection, sample_scan_data):
        """Test finding scan by ID."""
        # Setup
        mock_db_connection.fetch_one.return_value = sample_scan_data

        with patch("src.model.dao.scan_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.find_by_id.return_value = sample_scan_data
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            scan_dao = ScanDAO(mock_db_connection)

            # Execute
            result = scan_dao.find_by_id(1)

            # Assert
            assert result == sample_scan_data

    def test_get_recent_scans_with_context(self, mock_db_connection):
        """Test getting recent scans filtered by context."""
        # Setup
        mock_scans = [
            {"id": 1, "cluster_context": "test-cluster", "timestamp": datetime.now()},
            {"id": 2, "cluster_context": "test-cluster", "timestamp": datetime.now()},
        ]

        with patch("src.model.dao.scan_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.find_where.return_value = mock_scans
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            scan_dao = ScanDAO(mock_db_connection)

            # Execute
            result = scan_dao.get_recent_scans(limit=10, context="test-cluster")

            # Assert
            assert result == mock_scans
            mock_base_dao.find_where.assert_called_once_with(
                "cluster_context = ?", ["test-cluster"], "timestamp DESC", 10
            )

    def test_get_recent_scans_no_context(self, mock_db_connection):
        """Test getting recent scans without context filter."""
        # Setup
        mock_scans = [
            {"id": 1, "timestamp": datetime.now()},
            {"id": 2, "timestamp": datetime.now()},
        ]
        mock_db_connection.fetch_all.return_value = mock_scans

        with patch("src.model.dao.scan_dao.create_base_dao"):
            scan_dao = ScanDAO(mock_db_connection)

            # Execute
            result = scan_dao.get_recent_scans(limit=5)

            # Assert
            assert result == mock_scans
            mock_db_connection.fetch_all.assert_called_once_with(
                "SELECT * FROM scan_records ORDER BY timestamp DESC LIMIT 5"
            )

    def test_get_scan_statistics(self, mock_db_connection):
        """Test getting scan statistics."""
        # Setup
        mock_db_connection.fetch_one.side_effect = [
            {"min_date": datetime(2024, 1, 1), "max_date": datetime(2024, 1, 31)}
        ]
        mock_db_connection.fetch_all.side_effect = [
            [{"cluster_context": "prod", "count": 10}, {"cluster_context": None, "count": 5}],
            [{"cluster_version": "v1.28.0", "count": 15}],
        ]

        with patch("src.model.dao.scan_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.count_all.return_value = 15
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            scan_dao = ScanDAO(mock_db_connection)

            # Execute
            stats = scan_dao.get_scan_statistics(30)

            # Assert
            assert stats["total_scans"] == 15
            assert stats["scans_by_context"] == {"prod": 10, "default": 5}
            assert stats["cluster_versions"] == {"v1.28.0": 15}

    def test_cleanup_old_scans(self, mock_db_connection):
        """Test cleaning up old scans."""
        # Setup
        with patch("src.model.dao.scan_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.delete_where.return_value = 5
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            scan_dao = ScanDAO(mock_db_connection)

            # Execute
            deleted_count = scan_dao.cleanup_old_scans(90)

            # Assert
            assert deleted_count == 5
            mock_base_dao.delete_where.assert_called_once()


class TestResourceDAO:
    """Test ResourceDAO functionality."""

    def test_create_resource_sqlite(self, mock_db_connection):
        """Test creating a resource record in SQLite."""
        # Setup
        mock_db_connection.database_url = "sqlite:///test.db"
        mock_db_connection.dialect = "sqlite"

        with patch("src.model.dao.resource_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.insert_returning_id.return_value = 1
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            resource_dao = ResourceDAO(mock_db_connection)

            # Execute
            resource_id = resource_dao.create_resource(
                scan_id=1,
                api_version="v1",
                kind="Pod",
                namespace="default",
                name="test-pod",
                resource_data={"apiVersion": "v1"},
                resource_hash="abc123",
            )

            # Assert
            assert resource_id == 1
            mock_base_dao.insert_returning_id.assert_called_once()

    def test_bulk_create_resources(self, mock_db_connection):
        """Test bulk creating resource records."""
        # Setup
        resources_data = [
            {
                "scan_id": 1,
                "api_version": "v1",
                "kind": "Pod",
                "namespace": "default",
                "name": "pod1",
                "resource_data": {"apiVersion": "v1"},
                "resource_hash": "hash1",
                "is_helm_managed": False,
                "helm_release": None,
            },
            {
                "scan_id": 1,
                "api_version": "v1",
                "kind": "Service",
                "namespace": "default",
                "name": "svc1",
                "resource_data": {"apiVersion": "v1"},
                "resource_hash": "hash2",
                "is_helm_managed": True,
                "helm_release": "my-app",
            },
        ]

        with patch("src.model.dao.resource_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.bulk_insert.return_value = 2
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            resource_dao = ResourceDAO(mock_db_connection)

            # Execute
            result = resource_dao.bulk_create_resources(resources_data)

            # Assert
            assert result == 2
            mock_base_dao.bulk_insert.assert_called_once()

    def test_get_resource_history(self, mock_db_connection):
        """Test getting resource history."""
        # Setup
        mock_history = [{"id": 1, "kind": "Pod", "name": "test-pod", "timestamp": datetime.now()}]
        mock_db_connection.fetch_all.return_value = mock_history
        mock_db_connection.dialect = "sqlite"

        with patch("src.model.dao.resource_dao.create_base_dao"):
            resource_dao = ResourceDAO(mock_db_connection)

            # Execute
            result = resource_dao.get_resource_history("Pod", "test-pod", "default", 10)

            # Assert
            assert result == mock_history
            mock_db_connection.fetch_all.assert_called_once()

    def test_get_most_active_resources(self, mock_db_connection):
        """Test getting most active resources."""
        # Setup
        mock_results = [
            {"kind": "Pod", "name": "active-pod", "namespace": "default", "change_count": 5}
        ]
        mock_db_connection.fetch_all.return_value = mock_results
        mock_db_connection.dialect = "sqlite"

        with patch("src.model.dao.resource_dao.create_base_dao"):
            resource_dao = ResourceDAO(mock_db_connection)

            # Execute
            result = resource_dao.get_most_active_resources(30, 10)

            # Assert
            assert len(result) == 1
            assert result[0]["kind"] == "Pod"
            assert result[0]["name"] == "active-pod"
            assert result[0]["namespace"] == "default"
            assert result[0]["change_count"] == 5


class TestChangeDAO:
    """Test ChangeDAO functionality."""

    def test_create_change(self, mock_db_connection):
        """Test creating a change record."""
        # Setup
        with patch("src.model.dao.change_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.insert_returning_id.return_value = 1
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            change_dao = ChangeDAO(mock_db_connection)

            # Execute
            change_id = change_dao.create_change(
                kind="Pod",
                namespace="default",
                name="test-pod",
                change_type="updated",
                old_scan_id=1,
                new_scan_id=2,
                changed_fields={"spec.image": {"old": "nginx:1.20", "new": "nginx:1.21"}},
                diff_summary="Updated image",
            )

            # Assert
            assert change_id == 1
            mock_base_dao.insert_returning_id.assert_called_once()

    def test_get_changes_between_scans(self, mock_db_connection):
        """Test getting changes between scans."""
        # Setup
        mock_changes = [{"id": 1, "kind": "Pod", "name": "test-pod", "change_type": "updated"}]

        with patch("src.model.dao.change_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.find_where.return_value = mock_changes
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            change_dao = ChangeDAO(mock_db_connection)

            # Execute
            result = change_dao.get_changes_between_scans(1, 2)

            # Assert
            assert result == mock_changes
            mock_base_dao.find_where.assert_called_once_with(
                "old_scan_id = ? AND new_scan_id = ?", [1, 2], "timestamp DESC"
            )

    def test_get_recent_changes(self, mock_db_connection):
        """Test getting recent changes."""
        # Setup
        mock_changes = [{"id": 1, "change_type": "created"}, {"id": 2, "change_type": "updated"}]

        with patch("src.model.dao.change_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.find_where.return_value = mock_changes
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            change_dao = ChangeDAO(mock_db_connection)

            # Execute
            result = change_dao.get_recent_changes(days=7, limit=10)

            # Assert
            assert result == mock_changes
            mock_base_dao.find_where.assert_called_once()

    def test_get_change_statistics(self, mock_db_connection):
        """Test getting change statistics."""
        # Setup
        mock_db_connection.fetch_all.side_effect = [
            [{"change_type": "created", "count": 5}, {"change_type": "updated", "count": 3}],
            [{"kind": "Pod", "count": 4}, {"kind": "Service", "count": 2}],
            [{"namespace": "default", "count": 6}],
        ]

        with patch("src.model.dao.change_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.count_all.return_value = 8
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            change_dao = ChangeDAO(mock_db_connection)

            # Execute
            stats = change_dao.get_change_statistics(30)

            # Assert
            assert stats["total_changes"] == 8
            assert stats["changes_by_type"] == {"created": 5, "updated": 3}
            assert stats["most_changed_kinds"] == {"Pod": 4, "Service": 2}
            assert stats["most_active_namespaces"] == {"default": 6}

    def test_bulk_create_changes(self, mock_db_connection):
        """Test bulk creating change records."""
        # Setup
        changes_data = [
            {
                "kind": "Pod",
                "namespace": "default",
                "name": "pod1",
                "change_type": "created",
                "new_scan_id": 1,
            }
        ]

        with patch("src.model.dao.change_dao.create_base_dao") as mock_create_dao:
            mock_base_dao = Mock()
            mock_base_dao.bulk_insert.return_value = 1
            mock_create_dao.return_value = Mock(return_value=mock_base_dao)

            mock_db_connection.dialect = "sqlite"
            change_dao = ChangeDAO(mock_db_connection)

            # Execute
            result = change_dao.bulk_create_changes(changes_data)

            # Assert
            assert result == 1
            mock_base_dao.bulk_insert.assert_called_once()
