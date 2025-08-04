"""Integration tests for database operations."""

import pytest
import tempfile
from pathlib import Path

from src.database.connection import AsyncDatabaseConnection, DatabaseConnection


@pytest.mark.integration
class TestDatabaseIntegration:
    """Integration tests that use a real SQLite database."""

    def setup_method(self):
        """Set up test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.db_url = f"sqlite:///{self.db_path}"

    def teardown_method(self):
        """Clean up test database."""
        if self.db_path.exists():
            self.db_path.unlink()

    @pytest.mark.asyncio
    async def test_async_database_full_cycle(self):
        """Test full async database lifecycle."""
        # Create connection
        db = AsyncDatabaseConnection(self.db_url)

        try:
            # Connect and initialize
            await db.connect()

            # Test connection
            assert await db.test_connection() is True

            # Insert data
            scan_data = {
                "cluster_context": "test",
                "namespace": "default",
                "total_resources": 10,
                "cluster_version": "v1.28.0",
            }

            scan_id = await db.insert_returning_id("scan_records", scan_data)
            assert scan_id is not None

            # Fetch data
            result = await db.fetch_one(
                "SELECT * FROM scan_records WHERE id = :id", {"id": scan_id}
            )

            assert result is not None
            assert result["cluster_context"] == "test"
            assert result["total_resources"] == 10

            # Update data
            affected = await db.execute(
                "UPDATE scan_records SET total_resources = :resources WHERE id = :id",
                {"resources": 20, "id": scan_id},
            )
            assert affected == 1

            # Verify update
            updated = await db.fetch_one(
                "SELECT total_resources FROM scan_records WHERE id = :id", {"id": scan_id}
            )
            assert updated["total_resources"] == 20

            # Test stats
            stats = await db.get_database_stats()
            assert stats["scan_records"] == 1

        finally:
            await db.disconnect()

    def test_sync_database_full_cycle(self):
        """Test full sync database lifecycle."""
        # Create connection
        db = DatabaseConnection(self.db_url)

        try:
            # Test connection
            assert db.test_connection() is True

            # Insert data
            resource_data = {
                "scan_id": 1,
                "api_version": "v1",
                "kind": "Pod",
                "name": "test-pod",
                "resource_data": '{"apiVersion": "v1"}',
                "resource_hash": "abc123",
            }

            resource_id = db.insert_returning_id("resource_records", resource_data)
            assert resource_id is not None

            # Fetch data
            result = db.fetch_one(
                "SELECT * FROM resource_records WHERE id = :id", {"id": resource_id}
            )

            assert result is not None
            assert result["kind"] == "Pod"
            assert result["name"] == "test-pod"

            # Bulk insert
            bulk_data = [
                {
                    "scan_id": 1,
                    "api_version": "v1",
                    "kind": "Service",
                    "name": f"test-service-{i}",
                    "resource_data": '{"apiVersion": "v1"}',
                    "resource_hash": f"hash{i}",
                }
                for i in range(3)
            ]

            count = db.bulk_insert("resource_records", bulk_data)
            assert count == 3

            # Fetch all
            services = db.fetch_all(
                "SELECT * FROM resource_records WHERE kind = :kind", {"kind": "Service"}
            )
            assert len(services) == 3

            # Test stats
            stats = db.get_database_stats()
            assert stats["resource_records"] == 4  # 1 Pod + 3 Services

        finally:
            db.close()

    @pytest.mark.asyncio
    async def test_transaction_rollback(self):
        """Test transaction rollback on error."""
        db = AsyncDatabaseConnection(self.db_url)

        try:
            await db.connect()

            # Insert initial data
            await db.insert_returning_id(
                "scan_records", {"cluster_context": "test", "total_resources": 5}
            )

            # Verify initial state
            count_before = await db.fetch_one("SELECT COUNT(*) as count FROM scan_records")
            assert count_before["count"] == 1

            # Try transaction that should fail
            try:
                async with db.database.transaction():
                    # Insert valid record
                    await db.insert_returning_id(
                        "scan_records", {"cluster_context": "test2", "total_resources": 10}
                    )

                    # Force an error (invalid SQL)
                    await db.database.execute("INVALID SQL STATEMENT")

            except Exception:
                pass  # Expected to fail

            # Verify rollback occurred
            count_after = await db.fetch_one("SELECT COUNT(*) as count FROM scan_records")
            assert count_after["count"] == 1  # Should still be 1, not 2

        finally:
            await db.disconnect()


@pytest.mark.integration
@pytest.mark.slow
class TestPostgreSQLIntegration:
    """Integration tests for PostgreSQL (requires running PostgreSQL)."""

    @pytest.mark.skip(reason="PostgreSQL tests require running PostgreSQL instance")
    @pytest.mark.asyncio
    async def test_postgresql_connection(self):
        """Test PostgreSQL connection (only if available)."""
        # This would require a running PostgreSQL instance
        # Skip for now since we don't want to require PostgreSQL for tests
        pytest.skip("PostgreSQL integration tests require running PostgreSQL instance")
