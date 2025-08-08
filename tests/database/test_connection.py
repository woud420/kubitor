"""Unit tests for database connection layer using aiosqlite and asyncpg mocks."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from pathlib import Path

from src.database.connection import AsyncDatabaseConnection, DatabaseConnection


class TestAsyncDatabaseConnection:
    """Test AsyncDatabaseConnection functionality."""

    @pytest.mark.asyncio
    async def test_init_with_default_url(self):
        conn = AsyncDatabaseConnection()
        expected_path = Path.home() / ".k8s-scanner" / "history.db"
        assert conn.database_url == f"sqlite:///{expected_path}"

    @pytest.mark.asyncio
    async def test_connect_sqlite(self):
        mock_conn = AsyncMock()
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)) as mock_connect:
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            mock_connect.assert_called_once_with("test.db")
            assert conn._connected is True

    @pytest.mark.asyncio
    async def test_disconnect_sqlite(self):
        mock_conn = AsyncMock()
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)):
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            await conn.disconnect()
            mock_conn.close.assert_called_once()
            assert conn._connected is False

    @pytest.mark.asyncio
    async def test_execute_fetch_sqlite(self):
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = {"id": 1}
        mock_cursor.fetchall.return_value = [{"id": 1}, {"id": 2}]
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)):
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            result = await conn.execute("INSERT", {"a": 1})
            assert result == 1
            one = await conn.fetch_one("SELECT", {"a": 1})
            assert one == {"id": 1}
            all_rows = await conn.fetch_all("SELECT", None)
            assert len(all_rows) == 2
            mock_conn.commit.assert_called()

    @pytest.mark.asyncio
    async def test_insert_returning_id_sqlite(self):
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 5
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)):
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            result = await conn.insert_returning_id("tbl", {"name": "x"})
            assert result == 5

    @pytest.mark.asyncio
    async def test_insert_returning_id_postgresql(self):
        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = {"id": 5}
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            conn = AsyncDatabaseConnection("postgresql://test")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            result = await conn.insert_returning_id("tbl", {"name": "x"})
            assert result == 5
            mock_pool.fetchrow.assert_called()

    @pytest.mark.asyncio
    async def test_bulk_insert_uses_transaction(self):
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 1
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)):
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            result = await conn.bulk_insert("tbl", [{"a": 1}, {"a": 2}])
            assert result == 2
            assert mock_conn.execute.call_count >= 2

    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = {"result": 1}
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)):
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            assert await conn.test_connection() is True

    @pytest.mark.asyncio
    async def test_get_database_stats(self):
        mock_cursor = AsyncMock()
        # fetch_one will be called three times
        mock_cursor.fetchone.side_effect = [
            {"count": 1},
            {"count": 2},
            {"count": 3},
        ]
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = mock_cursor
        with patch("aiosqlite.connect", new=AsyncMock(return_value=mock_conn)):
            conn = AsyncDatabaseConnection("sqlite:///test.db")
            with patch.object(conn, "initialize_schema", new_callable=AsyncMock):
                await conn.connect()
            stats = await conn.get_database_stats()
            assert stats["scan_records"] == 1
            assert stats["resource_records"] == 2
            assert stats["resource_changes"] == 3

    def test_get_database_info(self):
        conn = AsyncDatabaseConnection("sqlite:///test.db")
        info = conn.get_database_info()
        assert info["engine"] == "aiosqlite"


class TestDatabaseConnectionSyncWrapper:
    """Test DatabaseConnection sync wrapper."""

    def test_execute_sync(self):
        async_conn = AsyncMock()
        async_conn.execute.return_value = 1
        with patch("src.database.connection.AsyncDatabaseConnection", return_value=async_conn):
            conn = DatabaseConnection("sqlite:///test.db")
            loop = Mock()
            loop.run_until_complete.return_value = 1
            with patch.object(conn, "_get_loop", return_value=loop):
                result = conn.execute("INSERT", {})
        assert result == 1
        loop.run_until_complete.assert_called_once()
