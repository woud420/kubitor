"""Async database connection using databases library."""

import asyncio
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from databases import Database

from ..utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseDialect(Enum):
    """Enum for database dialects."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    UNKNOWN = "unknown"


# Dictionary mapping URL patterns to dialect detection functions
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


class AsyncDatabaseConnection:
    """Async database connection manager using databases library."""

    def __init__(self, database_url: Optional[str] = None):
        """Initialize connection manager."""
        if database_url is None:
            # Default to SQLite in user's home directory
            db_path = Path.home() / ".k8s-scanner" / "history.db"
            db_path.parent.mkdir(exist_ok=True)
            database_url = f"sqlite:///{db_path}"

        self.database_url = database_url
        self.database = Database(database_url)
        self._connected = False

        # Determine dialect from URL using enum + dictionary pattern
        self.dialect = detect_database_dialect(database_url)

        logger.info(f"Database initialized: {database_url}")

    async def connect(self):
        """Connect to database."""
        if not self._connected:
            await self.database.connect()
            self._connected = True
            await self.initialize_schema()
            logger.info("Database connected")

    async def disconnect(self):
        """Disconnect from database."""
        if self._connected:
            await self.database.disconnect()
            self._connected = False
            logger.info("Database disconnected")

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection context manager."""
        if not self._connected:
            await self.connect()

        async with self.database.transaction():
            yield self.database

    async def initialize_schema(self):
        """Initialize database schema using enum + dictionary pattern."""
        # Dictionary mapping dialects to schema initialization functions
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
        """Create SQLite schema."""
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
            # Indexes
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
            await self.database.execute(query)

    async def _create_postgresql_schema(self):
        """Create PostgreSQL schema."""
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
            # Indexes
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
            await self.database.execute(query)

    async def execute(self, query: str, values: Optional[Dict[str, Any]] = None) -> int:
        """Execute a query and return affected rows."""
        if not self._connected:
            await self.connect()

        result = await self.database.execute(query, values or {})
        return result

    async def fetch_one(
        self, query: str, values: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch one row as dictionary."""
        if not self._connected:
            await self.connect()

        result = await self.database.fetch_one(query, values or {})
        return dict(result) if result else None

    async def fetch_all(
        self, query: str, values: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows as list of dictionaries."""
        if not self._connected:
            await self.connect()

        results = await self.database.fetch_all(query, values or {})
        return [dict(row) for row in results]

    async def insert_returning_id(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """Insert and return the new record ID."""
        if not self._connected:
            await self.connect()

        columns = list(data.keys())
        placeholders = [f":{col}" for col in columns]

        if self.database_url.startswith("sqlite"):
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            result = await self.database.execute(query, data)
            return result  # SQLite returns lastrowid
        else:  # PostgreSQL
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) RETURNING id"
            result = await self.database.fetch_one(query, data)
            return result["id"] if result else None

    async def bulk_insert(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Bulk insert records."""
        if not records:
            return 0

        if not self._connected:
            await self.connect()

        # Use transaction for bulk insert
        async with self.database.transaction():
            count = 0
            for record in records:
                await self.insert_returning_id(table, record)
                count += 1

        return count

    async def test_connection(self) -> bool:
        """Test database connection."""
        try:
            if not self._connected:
                await self.connect()
            await self.database.fetch_one("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        if not self._connected:
            await self.connect()

        stats = {}
        tables = ["scan_records", "resource_records", "resource_changes"]

        for table in tables:
            try:
                result = await self.database.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
                stats[table] = result["count"] if result else 0
            except Exception:
                stats[table] = 0

        return stats

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information."""
        return {
            "url": self.database_url,
            "connected": self._connected,
            "engine": "databases" + (" + asyncio" if self._connected else ""),
        }


# Sync wrapper for CLI compatibility
class DatabaseConnection:
    """Sync wrapper around async database connection."""

    def __init__(self, database_url: Optional[str] = None):
        self.async_db = AsyncDatabaseConnection(database_url)
        self._loop = None

        # Expose dialect from async connection
        self.dialect = self.async_db.dialect

    def _get_loop(self):
        """Get or create event loop."""
        if self._loop is None:
            try:
                self._loop = asyncio.get_event_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def execute(self, query: str, values: Optional[Dict[str, Any]] = None) -> int:
        """Execute a query and return affected rows."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.execute(query, values))

    def fetch_one(
        self, query: str, values: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch one row as dictionary."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.fetch_one(query, values))

    def fetch_all(
        self, query: str, values: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows as list of dictionaries."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.fetch_all(query, values))

    def insert_returning_id(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """Insert and return the new record ID."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.insert_returning_id(table, data))

    def bulk_insert(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Bulk insert records."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.bulk_insert(table, records))

    def test_connection(self) -> bool:
        """Test database connection."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.test_connection())

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        loop = self._get_loop()
        return loop.run_until_complete(self.async_db.get_database_stats())

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information."""
        return self.async_db.get_database_info()

    def close(self):
        """Close database connection."""
        if self._loop and self.async_db._connected:
            self._loop.run_until_complete(self.async_db.disconnect())
