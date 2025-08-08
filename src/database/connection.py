"""Async database connection using aiosqlite or asyncpg."""

import asyncio
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from databases import Database

import aiosqlite
import asyncpg

from ..utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseDialect(Enum):
    """Enum for database dialects."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    UNKNOWN = "unknown"


DIALECT_DETECTORS: Dict[str, Callable[[str], bool]] = {
    "sqlite": lambda url: url.startswith("sqlite"),
    "postgresql": lambda url: url.startswith("postgresql"),
}


def detect_database_dialect(database_url: str) -> str:
    """Detect database dialect from URL using enum + dictionary pattern."""
    for dialect_name, detector_func in DIALECT_DETECTORS.items():
        if detector_func(database_url):
            return dialect_name
    return DatabaseDialect.UNKNOWN.value


def _convert_postgres(query: str, values: Optional[Dict[str, Any]]) -> Tuple[str, List[Any]]:
    """Convert :named parameters to asyncpg positional parameters."""
    if not values:
        return query, []

    new_query = ""
    params: List[Any] = []
    idx = 1
    i = 0
    while i < len(query):
        if query[i] == ":":
            j = i + 1
            while j < len(query) and (query[j].isalnum() or query[j] == "_"):
                j += 1
            name = query[i + 1 : j]
            new_query += f"${idx}"
            params.append(values.get(name))
            idx += 1
            i = j
        else:
            new_query += query[i]
            i += 1
    return new_query, params


class AsyncDatabaseConnection:
    """Async database connection manager."""

    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
            db_path = Path.home() / ".k8s-scanner" / "history.db"
            db_path.parent.mkdir(exist_ok=True)
            database_url = f"sqlite:///{db_path}"

        self.database_url = database_url
        self.dialect = detect_database_dialect(database_url)
        self._connected = False
        self._conn: Optional[aiosqlite.Connection] = None
        self._pool: Optional[asyncpg.pool.Pool] = None
        self._in_transaction = 0
        self._tx_conn: Optional[asyncpg.Connection] = None
        self.database = self  # expose database-like interface

        logger.info(f"Database initialized: {database_url}")

    async def connect(self):
        """Connect to database."""
        if self._connected:
            return

        if self.dialect == DatabaseDialect.SQLITE.value:
            path = self.database_url.replace("sqlite:///", "")
            self._conn = await aiosqlite.connect(path)
            self._conn.row_factory = aiosqlite.Row
        elif self.dialect == DatabaseDialect.POSTGRESQL.value:
            self._pool = await asyncpg.create_pool(self.database_url)
        else:
            raise ValueError(f"Unsupported database URL: {self.database_url}")

        self._connected = True
        await self.initialize_schema()
        logger.info("Database connected")

    async def disconnect(self):
        """Disconnect from database."""
        if not self._connected:
            return

        if self.dialect == DatabaseDialect.SQLITE.value and self._conn:
            await self._conn.close()
        elif self.dialect == DatabaseDialect.POSTGRESQL.value and self._pool:
            await self._pool.close()

        self._connected = False
        logger.info("Database disconnected")

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection context manager."""
        async with self.transaction():
            yield self

    @asynccontextmanager
    async def transaction(self):
        """Transaction context manager."""
        await self.connect()
        if self.dialect == DatabaseDialect.SQLITE.value and self._conn:
            await self._conn.execute("BEGIN")
            self._in_transaction += 1
            try:
                yield self
                await self._conn.commit()
            except Exception:
                await self._conn.rollback()
                raise
            finally:
                self._in_transaction -= 1
        elif self.dialect == DatabaseDialect.POSTGRESQL.value and self._pool:
            conn = await self._pool.acquire()
            tx = conn.transaction()
            await tx.start()
            self._in_transaction += 1
            prev_conn = self._tx_conn
            self._tx_conn = conn
            try:
                yield self
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
            finally:
                self._in_transaction -= 1
                self._tx_conn = prev_conn
                await self._pool.release(conn)
        else:
            yield self

    async def initialize_schema(self):
        """Initialize database schema."""
        schema_initializers = {
            DatabaseDialect.SQLITE.value: self._create_sqlite_schema,
            DatabaseDialect.POSTGRESQL.value: self._create_postgresql_schema,
        }
        initializer_func = schema_initializers.get(self.dialect)
        if initializer_func:
            await initializer_func()
        else:
            logger.warning(f"No schema initializer for dialect: {self.dialect}")
        logger.info("Database schema initialized")

    async def _create_sqlite_schema(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS scan_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                cluster_context TEXT,
                namespace TEXT,
                scan_type TEXT DEFAULT 'full',
                total_resources INTEGER DEFAULT 0,
                cluster_version TEXT,
                node_count INTEGER,
                cluster_info TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS resource_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL,
                api_version TEXT NOT NULL,
                kind TEXT NOT NULL,
                namespace TEXT,
                name TEXT NOT NULL,
                resource_data TEXT NOT NULL,
                resource_hash TEXT,
                is_helm_managed BOOLEAN DEFAULT FALSE,
                helm_release TEXT,
                labels TEXT,
                annotations TEXT,
                FOREIGN KEY (scan_id) REFERENCES scan_records (id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS resource_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                kind TEXT NOT NULL,
                namespace TEXT,
                name TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_scan_id INTEGER,
                new_scan_id INTEGER,
                changed_fields TEXT,
                diff_summary TEXT,
                FOREIGN KEY (old_scan_id) REFERENCES scan_records (id) ON DELETE CASCADE,
                FOREIGN KEY (new_scan_id) REFERENCES scan_records (id) ON DELETE CASCADE
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_scan_timestamp ON scan_records (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_scan_context ON scan_records (cluster_context)",
            "CREATE INDEX IF NOT EXISTS idx_resource_scan ON resource_records (scan_id)",
            "CREATE INDEX IF NOT EXISTS idx_resource_kind ON resource_records (kind)",
            "CREATE INDEX IF NOT EXISTS idx_resource_name ON resource_records (kind, name, namespace)",
            "CREATE INDEX IF NOT EXISTS idx_resource_hash ON resource_records (resource_hash)",
            "CREATE INDEX IF NOT EXISTS idx_change_timestamp ON resource_changes (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_change_resource ON resource_changes (kind, name, namespace)",
        ]

        for query in queries:
            await self.execute(query)

    async def _create_postgresql_schema(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS scan_records (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                cluster_context VARCHAR(255),
                namespace VARCHAR(255),
                scan_type VARCHAR(50) DEFAULT 'full',
                total_resources INTEGER DEFAULT 0,
                cluster_version VARCHAR(50),
                node_count INTEGER,
                cluster_info JSONB
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS resource_records (
                id SERIAL PRIMARY KEY,
                scan_id INTEGER NOT NULL,
                api_version VARCHAR(100) NOT NULL,
                kind VARCHAR(100) NOT NULL,
                namespace VARCHAR(255),
                name VARCHAR(255) NOT NULL,
                resource_data JSONB NOT NULL,
                resource_hash VARCHAR(64),
                is_helm_managed BOOLEAN DEFAULT FALSE,
                helm_release VARCHAR(255),
                labels JSONB,
                annotations JSONB,
                FOREIGN KEY (scan_id) REFERENCES scan_records (id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS resource_changes (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                kind VARCHAR(100) NOT NULL,
                namespace VARCHAR(255),
                name VARCHAR(255) NOT NULL,
                change_type VARCHAR(20) NOT NULL,
                old_scan_id INTEGER,
                new_scan_id INTEGER,
                changed_fields JSONB,
                diff_summary TEXT,
                FOREIGN KEY (old_scan_id) REFERENCES scan_records (id) ON DELETE CASCADE,
                FOREIGN KEY (new_scan_id) REFERENCES scan_records (id) ON DELETE CASCADE
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_scan_timestamp ON scan_records (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_scan_context ON scan_records (cluster_context)",
            "CREATE INDEX IF NOT EXISTS idx_resource_scan ON resource_records (scan_id)",
            "CREATE INDEX IF NOT EXISTS idx_resource_kind ON resource_records (kind)",
            "CREATE INDEX IF NOT EXISTS idx_resource_name ON resource_records (kind, name, namespace)",
            "CREATE INDEX IF NOT EXISTS idx_resource_hash ON resource_records (resource_hash)",
            "CREATE INDEX IF NOT EXISTS idx_change_timestamp ON resource_changes (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_change_resource ON resource_changes (kind, name, namespace)",
        ]

        for query in queries:
            await self.execute(query)

    async def execute(self, query: str, values: Optional[Dict[str, Any]] = None) -> int:
        """Execute a query and return affected rows."""
        await self.connect()
        if self.dialect == DatabaseDialect.SQLITE.value and self._conn:
            cursor = await self._conn.execute(query, values or {})
            if self._in_transaction == 0:
                await self._conn.commit()
            return cursor.rowcount
        elif self.dialect == DatabaseDialect.POSTGRESQL.value and self._pool:
            pg_query, params = _convert_postgres(query, values)
            conn = self._tx_conn or self._pool
            result = await conn.execute(pg_query, *params)
            try:
                return int(result.split()[-1])
            except Exception:
                return 0
        else:
            raise RuntimeError("Database not connected")

    async def fetch_one(self, query: str, values: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Fetch one row as dictionary."""
        await self.connect()
        if self.dialect == DatabaseDialect.SQLITE.value and self._conn:
            cursor = await self._conn.execute(query, values or {})
            row = await cursor.fetchone()
            return dict(row) if row else None
        elif self.dialect == DatabaseDialect.POSTGRESQL.value and self._pool:
            pg_query, params = _convert_postgres(query, values)
            conn = self._tx_conn or self._pool
            row = await conn.fetchrow(pg_query, *params)
            return dict(row) if row else None
        return None

    async def fetch_all(self, query: str, values: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch all rows as list of dictionaries."""
        await self.connect()
        if self.dialect == DatabaseDialect.SQLITE.value and self._conn:
            cursor = await self._conn.execute(query, values or {})
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        elif self.dialect == DatabaseDialect.POSTGRESQL.value and self._pool:
            pg_query, params = _convert_postgres(query, values)
            conn = self._tx_conn or self._pool
            rows = await conn.fetch(pg_query, *params)
            return [dict(r) for r in rows]
        return []

    async def insert_returning_id(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """Insert and return the new record ID."""
        await self.connect()
        columns = list(data.keys())
        placeholders = [f":{col}" for col in columns]
        if self.dialect == DatabaseDialect.SQLITE.value and self._conn:
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            cursor = await self._conn.execute(query, data)
            if self._in_transaction == 0:
                await self._conn.commit()
            return cursor.lastrowid
        elif self.dialect == DatabaseDialect.POSTGRESQL.value and self._pool:
            query = (
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) RETURNING id"
            )
            pg_query, params = _convert_postgres(query, data)
            conn = self._tx_conn or self._pool
            row = await conn.fetchrow(pg_query, *params)
            return row["id"] if row else None
        return None

    async def bulk_insert(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Bulk insert records."""
        if not records:
            return 0
        await self.connect()
        async with self.transaction():
            count = 0
            for record in records:
                await self.insert_returning_id(table, record)
                count += 1
            return count

    async def test_connection(self) -> bool:
        """Test database connection."""
        try:
            await self.fetch_one("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        await self.connect()
        stats: Dict[str, Any] = {}
        for table in ["scan_records", "resource_records", "resource_changes"]:
            try:
                result = await self.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
                stats[table] = result["count"] if result else 0
            except Exception:
                stats[table] = 0
        return stats

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information."""
        engine = "aiosqlite" if self.dialect == DatabaseDialect.SQLITE.value else "asyncpg"
        return {
            "url": self.database_url,
            "connected": self._connected,
            "engine": engine,
        }


class DatabaseConnection:
    """Sync wrapper around async database connection."""

    def __init__(self, database_url: Optional[str] = None):
        self.async_db = AsyncDatabaseConnection(database_url)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.dialect = self.async_db.dialect

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            try:
                self._loop = asyncio.get_event_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def execute(self, query: str, values: Optional[Dict[str, Any]] = None) -> int:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.execute(query, values))

    def fetch_one(self, query: str, values: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.fetch_one(query, values))

    def fetch_all(self, query: str, values: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.fetch_all(query, values))

    def insert_returning_id(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.insert_returning_id(table, data))

    def bulk_insert(self, table: str, records: List[Dict[str, Any]]) -> int:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.bulk_insert(table, records))

    def test_connection(self) -> bool:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.test_connection())

    def get_database_stats(self) -> Dict[str, Any]:
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.get_database_stats())

    def get_database_info(self) -> Dict[str, Any]:
        return self.async_db.get_database_info()

    def close(self):
        if self._loop and self.async_db._connected:
            self._loop.run_until_complete(self.async_db.disconnect())
