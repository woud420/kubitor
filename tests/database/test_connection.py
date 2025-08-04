"""Unit tests for database connection layer."""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from pathlib import Path

from src.database.connection import AsyncDatabaseConnection, DatabaseConnection


class TestAsyncDatabaseConnection:
    """Test AsyncDatabaseConnection functionality."""

    @pytest.mark.asyncio
    async def test_init_with_default_url(self):
        """Test initialization with default SQLite URL."""
        with patch("src.database.connection.Database") as mock_database:
            conn = AsyncDatabaseConnection()

            # Assert default SQLite path is used
            expected_path = Path.home() / ".k8s-scanner" / "history.db"
            expected_url = f"sqlite:///{expected_path}"
            assert conn.database_url == expected_url
            mock_database.assert_called_once_with(expected_url)

    @pytest.mark.asyncio
    async def test_init_with_custom_url(self):
        """Test initialization with custom database URL."""
        custom_url = "postgresql://user:pass@localhost/test"

        with patch("src.database.connection.Database") as mock_database:
            conn = AsyncDatabaseConnection(custom_url)

            assert conn.database_url == custom_url
            mock_database.assert_called_once_with(custom_url)

    @pytest.mark.asyncio
    async def test_connect(self):
        """Test database connection."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")

            # Mock schema initialization
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()

            # Assert
            assert conn._connected is True
            mock_database.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Test database disconnection."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")
            conn._connected = True

            await conn.disconnect()

            # Assert
            assert conn._connected is False
            mock_database.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute(self):
        """Test query execution."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database.execute.return_value = 1
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")
            conn._connected = True

            result = await conn.execute("INSERT INTO test VALUES (?)", {"value": "test"})

            # Assert
            assert result == 1
            mock_database.execute.assert_called_once_with(
                "INSERT INTO test VALUES (?)", {"value": "test"}
            )

    @pytest.mark.asyncio
    async def test_fetch_one(self):
        """Test fetching one row."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_row = Mock()
            mock_row.__dict__ = {"id": 1, "name": "test"}

            # Mock the row to support dict() conversion
            def mock_dict():
                return {"id": 1, "name": "test"}

            mock_row.__iter__ = lambda: iter([("id", 1), ("name", "test")])

            mock_database.fetch_one.return_value = mock_row
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")
            conn._connected = True

            with patch("builtins.dict", side_effect=lambda x: {"id": 1, "name": "test"}):
                result = await conn.fetch_one("SELECT * FROM test WHERE id = :id", {"id": 1})

            # Assert
            assert result == {"id": 1, "name": "test"}
            mock_database.fetch_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_all(self):
        """Test fetching all rows."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_rows = [Mock(), Mock()]

            # Mock each row
            for i, row in enumerate(mock_rows):
                row.__iter__ = lambda i=i: iter([("id", i + 1), ("name", f"test{i + 1}")])

            mock_database.fetch_all.return_value = mock_rows
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")
            conn._connected = True

            def mock_dict(row):
                if hasattr(row, "__iter__"):
                    return dict(row)
                return {"id": 1, "name": "test"}

            with patch("builtins.dict", side_effect=mock_dict):
                result = await conn.fetch_all("SELECT * FROM test")

            # Assert
            assert len(result) == 2
            mock_database.fetch_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_returning_id_sqlite(self):
        """Test insert returning ID for SQLite."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database.execute.return_value = 5  # lastrowid
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")
            conn._connected = True

            result = await conn.insert_returning_id("test_table", {"name": "test"})

            # Assert
            assert result == 5
            mock_database.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_returning_id_postgresql(self):
        """Test insert returning ID for PostgreSQL."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database.fetch_one.return_value = {"id": 5}
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("postgresql://test")
            conn._connected = True

            result = await conn.insert_returning_id("test_table", {"name": "test"})

            # Assert
            assert result == 5
            mock_database.fetch_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_bulk_insert(self):
        """Test bulk insert."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")
            conn._connected = True

            # Mock the transaction context manager
            mock_transaction = AsyncMock()
            mock_database.transaction.return_value.__aenter__ = AsyncMock(
                return_value=mock_transaction
            )
            mock_database.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

            # Mock insert_returning_id
            with patch.object(conn, "insert_returning_id", new_callable=AsyncMock) as mock_insert:
                mock_insert.return_value = 1

                records = [{"name": "test1"}, {"name": "test2"}]
                result = await conn.bulk_insert("test_table", records)

            # Assert
            assert result == 2
            assert mock_insert.call_count == 2

    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        """Test successful connection test."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database.fetch_one.return_value = {"result": 1}
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")

            with patch.object(conn, "connect", new_callable=AsyncMock):
                result = await conn.test_connection()

            # Assert
            assert result is True
            mock_database.fetch_one.assert_called_once_with("SELECT 1")

    @pytest.mark.asyncio
    async def test_test_connection_failure(self):
        """Test failed connection test."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database.fetch_one.side_effect = Exception("Connection failed")
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")

            with patch.object(conn, "connect", new_callable=AsyncMock):
                result = await conn.test_connection()

            # Assert
            assert result is False

    @pytest.mark.asyncio
    async def test_get_database_stats(self):
        """Test getting database statistics."""
        with patch("src.database.connection.Database") as mock_database_class:
            mock_database = AsyncMock()
            mock_database.fetch_one.side_effect = [
                {"count": 5},  # scan_records
                {"count": 100},  # resource_records
                {"count": 20},  # resource_changes
            ]
            mock_database_class.return_value = mock_database

            conn = AsyncDatabaseConnection("sqlite:///test.db")

            with patch.object(conn, "connect", new_callable=AsyncMock):
                stats = await conn.get_database_stats()

            # Assert
            assert stats["scan_records"] == 5
            assert stats["resource_records"] == 100
            assert stats["resource_changes"] == 20

    def test_get_database_info(self):
        """Test getting database info."""
        with patch("src.database.connection.Database"):
            conn = AsyncDatabaseConnection("sqlite:///test.db")

            info = conn.get_database_info()

            # Assert
            assert info["url"] == "sqlite:///test.db"
            assert info["connected"] is False
            assert "databases" in info["engine"]


class TestDatabaseConnectionSyncWrapper:
    """Test DatabaseConnection sync wrapper."""

    def test_init(self):
        """Test sync wrapper initialization."""
        with patch("src.database.connection.AsyncDatabaseConnection") as mock_async:
            conn = DatabaseConnection("sqlite:///test.db")

            # Assert
            mock_async.assert_called_once_with("sqlite:///test.db")
            assert conn._loop is None

    def test_execute(self):
        """Test sync execute wrapper."""
        with patch("src.database.connection.AsyncDatabaseConnection") as mock_async_class:
            mock_async = AsyncMock()
            mock_async.execute.return_value = 1
            mock_async_class.return_value = mock_async

            conn = DatabaseConnection("sqlite:///test.db")

            with patch.object(conn, "_get_loop") as mock_get_loop:
                mock_loop = Mock()
                mock_loop.run_until_complete.return_value = 1
                mock_get_loop.return_value = mock_loop

                result = conn.execute("INSERT INTO test VALUES (?)", {"value": "test"})

            # Assert
            assert result == 1
            mock_loop.run_until_complete.assert_called_once()

    def test_get_loop_existing(self):
        """Test getting existing event loop."""
        with patch("src.database.connection.AsyncDatabaseConnection"):
            conn = DatabaseConnection("sqlite:///test.db")

            # Set existing loop
            mock_loop = Mock()
            conn._loop = mock_loop

            result = conn._get_loop()

            # Assert
            assert result is mock_loop

    def test_get_loop_create_new(self):
        """Test creating new event loop."""
        with patch("src.database.connection.AsyncDatabaseConnection"):
            conn = DatabaseConnection("sqlite:///test.db")

            with patch("asyncio.get_event_loop", side_effect=RuntimeError()), patch(
                "asyncio.new_event_loop"
            ) as mock_new_loop, patch("asyncio.set_event_loop") as mock_set_loop:
                mock_loop = Mock()
                mock_new_loop.return_value = mock_loop

                result = conn._get_loop()

                # Assert
                assert result is mock_loop
                mock_new_loop.assert_called_once()
                mock_set_loop.assert_called_once_with(mock_loop)

    def test_close(self):
        """Test closing sync wrapper."""
        with patch("src.database.connection.AsyncDatabaseConnection") as mock_async_class:
            mock_async = AsyncMock()
            mock_async._connected = True
            mock_async_class.return_value = mock_async

            conn = DatabaseConnection("sqlite:///test.db")

            # Set up loop
            mock_loop = Mock()
            conn._loop = mock_loop

            conn.close()

            # Assert
            mock_loop.run_until_complete.assert_called_once()
