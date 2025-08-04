"""Unit tests for Repository layer."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.model.repository.scan_repository import ScanRepository


class TestScanRepository:
    """Test ScanRepository functionality."""

    def test_create_scan_record(self, mock_db_connection):
        """Test creating a scan record."""
        # Setup
        mock_scan_dao = Mock()
        mock_scan_dao.create_scan.return_value = 1

        with patch("src.model.repository.scan_repository.ScanDAO", return_value=mock_scan_dao):
            repository = ScanRepository(mock_db_connection)

            # Execute
            scan_id = repository.create_scan_record(
                cluster_context="test-cluster",
                namespace="default",
                total_resources=100,
                cluster_version="v1.28.0",
                node_count=3,
            )

            # Assert
            assert scan_id == 1
            mock_scan_dao.create_scan.assert_called_once_with(
                cluster_context="test-cluster",
                namespace="default",
                scan_type="full",
                total_resources=100,
                cluster_version="v1.28.0",
                node_count=3,
                cluster_info=None,
            )

    def test_get_scan_by_id(self, mock_db_connection, sample_scan_data):
        """Test getting scan by ID."""
        # Setup
        mock_scan_dao = Mock()
        mock_scan_dao.find_by_id.return_value = sample_scan_data

        with patch("src.model.repository.scan_repository.ScanDAO", return_value=mock_scan_dao):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result = repository.get_scan_by_id(1)

            # Assert
            assert result == sample_scan_data
            mock_scan_dao.find_by_id.assert_called_once_with(1)

    def test_get_recent_scans_with_computed_fields(self, mock_db_connection):
        """Test getting recent scans with computed fields."""
        # Setup
        mock_scans = [
            {
                "id": 1,
                "timestamp": datetime(2024, 1, 1, 12, 0, 0),
                "total_resources": 100,
                "node_count": 4,
            },
            {
                "id": 2,
                "timestamp": datetime(2024, 1, 2, 12, 0, 0),
                "total_resources": 80,
                "node_count": 2,
            },
        ]

        mock_scan_dao = Mock()
        mock_scan_dao.get_recent_scans.return_value = mock_scans

        with patch("src.model.repository.scan_repository.ScanDAO", return_value=mock_scan_dao):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result = repository.get_recent_scans(limit=10, cluster_context="test-cluster")

            # Assert
            assert len(result) == 2

            # Check computed fields
            assert "age_days" in result[0]
            assert "resources_per_node" in result[0]
            assert result[0]["resources_per_node"] == 25.0  # 100/4
            assert result[1]["resources_per_node"] == 40.0  # 80/2

            mock_scan_dao.get_recent_scans.assert_called_once_with(limit=10, context="test-cluster")

    def test_get_previous_scan(self, mock_db_connection, sample_scan_data):
        """Test getting previous scan."""
        # Setup
        current_scan = {
            "id": 2,
            "timestamp": datetime(2024, 1, 2, 12, 0, 0),
            "cluster_context": "test-cluster",
        }

        mock_scan_dao = Mock()
        mock_scan_dao.get_scan_before_timestamp.return_value = sample_scan_data

        with patch("src.model.repository.scan_repository.ScanDAO", return_value=mock_scan_dao):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result = repository.get_previous_scan(current_scan, "test-cluster")

            # Assert
            assert result == sample_scan_data
            mock_scan_dao.get_scan_before_timestamp.assert_called_once_with(
                current_scan["timestamp"], "test-cluster"
            )

    def test_get_scan_statistics_with_business_logic(self, mock_db_connection):
        """Test getting scan statistics with business calculations."""
        # Setup
        mock_stats = {
            "total_scans": 30,
            "date_range": (datetime(2024, 1, 1), datetime(2024, 1, 31)),
            "scans_by_context": {"prod": 20, "staging": 10},
            "cluster_versions": {"v1.28.0": 30},
        }

        mock_scan_dao = Mock()
        mock_scan_dao.get_scan_statistics.return_value = mock_stats

        with patch("src.model.repository.scan_repository.ScanDAO", return_value=mock_scan_dao):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result = repository.get_scan_statistics(30)

            # Assert
            assert result["total_scans"] == 30
            assert result["avg_scans_per_day"] == 1.0  # 30/30
            assert result["scan_frequency"] == "1.0 scans/day"

            mock_scan_dao.get_scan_statistics.assert_called_once_with(30)

    def test_cleanup_old_scans(self, mock_db_connection):
        """Test cleaning up old scans."""
        # Setup
        mock_scan_dao = Mock()
        mock_scan_dao.cleanup_old_scans.return_value = 5

        with patch("src.model.repository.scan_repository.ScanDAO", return_value=mock_scan_dao):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result = repository.cleanup_old_scans(90)

            # Assert
            assert result["deleted_scans"] == 5
            assert result["keep_days"] == 90
            assert "cutoff_date" in result

            mock_scan_dao.cleanup_old_scans.assert_called_once_with(90)

    def test_validate_scan_data_with_errors(self, mock_db_connection):
        """Test scan data validation with errors."""
        # Setup
        with patch("src.model.repository.scan_repository.ScanDAO"):
            repository = ScanRepository(mock_db_connection)

            # Execute - test with negative resources
            result = repository.validate_scan_data(
                cluster_context="test",
                namespace="default",
                total_resources=-5,
                cluster_version="1.28.0",  # Missing 'v' prefix
            )

            # Assert
            assert len(result["errors"]) == 1
            assert "cannot be negative" in result["errors"][0]
            assert len(result["warnings"]) == 1
            assert "should start with 'v'" in result["warnings"][0]

    def test_validate_scan_data_with_warnings(self, mock_db_connection):
        """Test scan data validation with warnings."""
        # Setup
        with patch("src.model.repository.scan_repository.ScanDAO"):
            repository = ScanRepository(mock_db_connection)

            # Execute - test with zero resources and large count
            result = repository.validate_scan_data(
                cluster_context="test",
                namespace="default",
                total_resources=0,  # Will trigger warning
                cluster_version="v1.28.0",
            )

            # Assert
            assert len(result["errors"]) == 0
            assert len(result["warnings"]) == 1
            assert "scan may have failed" in result["warnings"][0]

    def test_validate_scan_data_large_resource_count(self, mock_db_connection):
        """Test scan data validation with large resource count."""
        # Setup
        with patch("src.model.repository.scan_repository.ScanDAO"):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result = repository.validate_scan_data(
                cluster_context="test",
                namespace="default",
                total_resources=15000,  # Large count
                cluster_version="v1.28.0",
            )

            # Assert
            assert len(result["errors"]) == 0
            assert len(result["warnings"]) == 1
            assert "Large resource count" in result["warnings"][0]

    def test_calculate_age_days(self, mock_db_connection):
        """Test age calculation helper method."""
        # Setup
        with patch("src.model.repository.scan_repository.ScanDAO"):
            repository = ScanRepository(mock_db_connection)

            # Execute
            past_date = datetime.utcnow().replace(day=1)  # Beginning of month
            age = repository._calculate_age_days(past_date)

            # Assert
            assert isinstance(age, int)
            assert age >= 0

    def test_calculate_resources_per_node(self, mock_db_connection):
        """Test resources per node calculation."""
        # Setup
        with patch("src.model.repository.scan_repository.ScanDAO"):
            repository = ScanRepository(mock_db_connection)

            # Execute
            result1 = repository._calculate_resources_per_node(100, 4)
            result2 = repository._calculate_resources_per_node(100, 0)  # Edge case

            # Assert
            assert result1 == 25.0
            assert result2 == 0.0

    def test_calculate_scan_frequency(self, mock_db_connection):
        """Test scan frequency calculation."""
        # Setup
        with patch("src.model.repository.scan_repository.ScanDAO"):
            repository = ScanRepository(mock_db_connection)

            # Execute
            freq1 = repository._calculate_scan_frequency(30, 30)  # 1 per day
            freq2 = repository._calculate_scan_frequency(15, 30)  # 0.5 per day
            freq3 = repository._calculate_scan_frequency(0, 30)  # No scans

            # Assert
            assert freq1 == "1.0 scans/day"
            assert "every 2.0 days" in freq2
            assert freq3 == "No scans"
