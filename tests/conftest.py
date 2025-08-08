"""Test configuration and fixtures."""

import pytest
from unittest.mock import AsyncMock, Mock
from datetime import datetime
from typing import Dict, Any, List, Optional

pytest_plugins = ("pytest_asyncio",)

from src.database.connection import DatabaseConnection, AsyncDatabaseConnection


@pytest.fixture
def mock_db_connection():
    """Mock database connection for unit tests."""
    mock_conn = Mock(spec=DatabaseConnection)

    # Mock common methods
    mock_conn.execute = Mock(return_value=1)
    mock_conn.fetch_one = Mock(return_value=None)
    mock_conn.fetch_all = Mock(return_value=[])
    mock_conn.insert_returning_id = Mock(return_value=1)
    mock_conn.bulk_insert = Mock(return_value=0)
    mock_conn.test_connection = Mock(return_value=True)
    mock_conn.get_database_stats = Mock(return_value={})
    mock_conn.get_database_info = Mock(return_value={"url": "sqlite:///test.db"})
    mock_conn.close = Mock()

    return mock_conn


@pytest.fixture
async def mock_async_db_connection():
    """Mock async database connection for unit tests."""
    mock_conn = AsyncMock(spec=AsyncDatabaseConnection)

    # Mock common methods
    mock_conn.connect = AsyncMock()
    mock_conn.disconnect = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=1)
    mock_conn.fetch_one = AsyncMock(return_value=None)
    mock_conn.fetch_all = AsyncMock(return_value=[])
    mock_conn.insert_returning_id = AsyncMock(return_value=1)
    mock_conn.bulk_insert = AsyncMock(return_value=0)
    mock_conn.test_connection = AsyncMock(return_value=True)
    mock_conn.get_database_stats = AsyncMock(return_value={})
    mock_conn.get_database_info = Mock(return_value={"url": "sqlite:///test.db"})

    return mock_conn


@pytest.fixture
def sample_scan_data():
    """Sample scan data for testing."""
    return {
        "id": 1,
        "timestamp": datetime(2024, 1, 1, 12, 0, 0),
        "cluster_context": "test-cluster",
        "namespace": "default",
        "scan_type": "full",
        "total_resources": 100,
        "cluster_version": "v1.28.0",
        "node_count": 3,
        "cluster_info": '{"nodes": [{"name": "node1"}]}',
    }


@pytest.fixture
def sample_resource_data():
    """Sample resource data for testing."""
    return {
        "id": 1,
        "scan_id": 1,
        "api_version": "v1",
        "kind": "Pod",
        "namespace": "default",
        "name": "test-pod",
        "resource_data": '{"apiVersion": "v1", "kind": "Pod"}',
        "resource_hash": "abc123",
        "is_helm_managed": False,
        "helm_release": None,
    }


@pytest.fixture
def sample_change_data():
    """Sample change data for testing."""
    return {
        "id": 1,
        "timestamp": datetime(2024, 1, 1, 12, 0, 0),
        "kind": "Pod",
        "namespace": "default",
        "name": "test-pod",
        "change_type": "created",
        "old_scan_id": None,
        "new_scan_id": 1,
        "changed_fields": '{"spec.containers[0].image": {"old": "nginx:1.20", "new": "nginx:1.21"}}',
        "diff_summary": "Updated container image",
    }


@pytest.fixture
def sample_k8s_resources():
    """Sample K8s resources for testing."""
    from src.model.kubernetes import K8sResource

    return [
        K8sResource(
            api_version="v1",
            kind="Pod",
            metadata={"name": "test-pod", "namespace": "default"},
            spec={"containers": [{"name": "nginx", "image": "nginx:1.21"}]},
            status={"phase": "Running"},
        ),
        K8sResource(
            api_version="v1",
            kind="Service",
            metadata={"name": "test-service", "namespace": "default"},
            spec={"selector": {"app": "test"}, "ports": [{"port": 80}]},
        ),
    ]


@pytest.fixture
def sample_cluster_info():
    """Sample cluster info for testing."""
    from src.model.cluster import ClusterInfo, ClusterVersion, NodeInfo

    return ClusterInfo(
        server_version=ClusterVersion(major="1", minor="28", git_version="v1.28.0"),
        client_version=ClusterVersion(major="1", minor="28", git_version="v1.28.0"),
        nodes=[
            NodeInfo(
                name="node1",
                status="Ready",
                roles="worker",
                version="v1.28.0",
                os="Amazon Linux 2",
                container_runtime="containerd://1.7.0",
            ),
            NodeInfo(
                name="node2",
                status="Ready",
                roles="worker",
                version="v1.28.0",
                os="Amazon Linux 2",
                container_runtime="containerd://1.7.0",
            ),
        ],
    )


class MockTable:
    """Mock database table for testing."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.data = []
        self.next_id = 1

    def insert(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Mock insert operation."""
        record = record.copy()
        record["id"] = self.next_id
        self.next_id += 1
        self.data.append(record)
        return record

    def find_one(self, **filters) -> Optional[Dict[str, Any]]:
        """Mock find one operation."""
        for record in self.data:
            if all(record.get(k) == v for k, v in filters.items()):
                return record
        return None

    def find(self, **filters) -> List[Dict[str, Any]]:
        """Mock find operation."""
        results = []
        for record in self.data:
            if all(record.get(k) == v for k, v in filters.items()):
                results.append(record)
        return results

    def count(self, **filters) -> int:
        """Mock count operation."""
        return len(self.find(**filters))

    def delete(self, **filters) -> int:
        """Mock delete operation."""
        to_delete = self.find(**filters)
        for record in to_delete:
            self.data.remove(record)
        return len(to_delete)

    def clear(self):
        """Clear all data."""
        self.data.clear()
        self.next_id = 1


@pytest.fixture
def mock_database_tables():
    """Mock database tables for testing."""
    return {
        "scan_records": MockTable("scan_records"),
        "resource_records": MockTable("resource_records"),
        "resource_changes": MockTable("resource_changes"),
    }
