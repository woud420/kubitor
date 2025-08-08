"""DAO for scan_records table operations using composition."""

import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Type, Callable
from enum import Enum

from .base_dao import BaseDAO, BaseSQLiteDAO, BasePostgreSQLDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseDialect(Enum):
    """Enum for database dialects."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class ScanDAOSQLite(BaseSQLiteDAO):
    """SQLite-specific DAO for scan_records table."""

    def get_table_name(self) -> str:
        return "scan_records"


class ScanDAOPostgreSQL(BasePostgreSQLDAO):
    """PostgreSQL-specific DAO for scan_records table."""

    def get_table_name(self) -> str:
        return "scan_records"


# Dictionary mapping dialect enum to DAO class constructor
SCAN_DAO_REGISTRY: Dict[DatabaseDialect, Callable[[DatabaseConnection], BaseDAO]] = {
    DatabaseDialect.SQLITE: lambda conn: ScanDAOSQLite(conn),
    DatabaseDialect.POSTGRESQL: lambda conn: ScanDAOPostgreSQL(conn),
}


def create_base_dao(dialect: DatabaseDialect) -> Callable[[DatabaseConnection], BaseDAO]:
    """Return the DAO constructor for the given dialect.

    This indirection allows tests to patch the factory to supply mocked DAOs.
    """

    return SCAN_DAO_REGISTRY[dialect]


class ScanDAO:
    """Data Access Object for scan_records table with dialect dispatch."""

    def __init__(self, db_connection: DatabaseConnection):
        """Initialise the DAO and resolve the database dialect.

        The real ``DatabaseConnection`` object exposes a ``dialect`` attribute, but
        the tests provide a ``MagicMock`` without it.  To make the DAO resilient we
        attempt to derive the dialect from multiple sources in the following order:

        * ``db_connection.dialect`` if it exists and is a string
        * ``db_connection.database_url``
        * ``db_connection.get_database_info()['url']``
        * default to ``sqlite``

        This mirrors the behaviour of a real connection while keeping the tests
        simple.
        """

        self.db = db_connection

        try:
            dialect = db_connection.dialect
        except AttributeError:
            dialect = None

        if not isinstance(dialect, str):
            try:
                url = db_connection.database_url
            except AttributeError:
                url = None
            if not isinstance(url, str) and hasattr(db_connection, "get_database_info"):
                info = db_connection.get_database_info()
                url = info.get("url") if isinstance(info, dict) else None
            dialect = url.split(":", 1)[0] if isinstance(url, str) else "sqlite"

        try:
            dialect_enum = DatabaseDialect(dialect)
        except ValueError:
            raise ValueError(f"Unsupported database dialect: {dialect}")

        dao_constructor = create_base_dao(dialect_enum)
        self._base_dao = dao_constructor(db_connection)
        self.dialect = dialect_enum.value

    def create_scan(
        self,
        cluster_context: Optional[str] = None,
        namespace: Optional[str] = None,
        scan_type: str = "full",
        total_resources: int = 0,
        cluster_version: Optional[str] = None,
        node_count: Optional[int] = None,
        cluster_info: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Create a new scan record and return its ID."""
        columns = [
            "cluster_context",
            "namespace",
            "scan_type",
            "total_resources",
            "cluster_version",
            "node_count",
            "cluster_info",
        ]

        if self.dialect == "sqlite":
            values = [
                cluster_context,
                namespace,
                scan_type,
                total_resources,
                cluster_version,
                node_count,
                json.dumps(cluster_info) if cluster_info else None,
            ]
        else:  # postgresql
            values = [
                cluster_context,
                namespace,
                scan_type,
                total_resources,
                cluster_version,
                node_count,
                cluster_info,  # PostgreSQL handles JSON natively
            ]

        return self._base_dao.insert_returning_id(columns, values)

    def find_by_id(self, scan_id: int) -> Optional[Dict[str, Any]]:
        """Find scan by ID."""
        return self._base_dao.find_by_id(scan_id)

    def get_recent_scans(
        self, limit: int = 10, context: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get recent scans, optionally filtered by context."""
        if context:
            where_clause = "cluster_context = ?"
            params = [context]
            if self.dialect == "postgresql":
                where_clause = "cluster_context = %s"
            return self._base_dao.find_where(where_clause, params, "timestamp DESC", limit)
        else:
            query = "SELECT * FROM scan_records ORDER BY timestamp DESC"
            if limit:
                query += f" LIMIT {limit}"
            return self.db.fetch_all(query)

    def get_scans_in_date_range(
        self, start_date: datetime, end_date: datetime, context: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get scans within a date range."""
        if self.dialect == "sqlite":
            where_clause = "timestamp >= ? AND timestamp <= ?"
            params = [start_date, end_date]
            if context:
                where_clause += " AND cluster_context = ?"
                params.append(context)
        else:  # postgresql
            where_clause = "timestamp >= %s AND timestamp <= %s"
            params = [start_date, end_date]
            if context:
                where_clause += " AND cluster_context = %s"
                params.append(context)

        return self._base_dao.find_where(where_clause, params, "timestamp DESC")

    def get_latest_scan(self, context: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get the most recent scan."""
        scans = self.get_recent_scans(limit=1, context=context)
        return scans[0] if scans else None

    def get_scan_before_timestamp(
        self, timestamp: datetime, context: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent scan before a given timestamp."""
        if self.dialect == "sqlite":
            where_clause = "timestamp < ?"
            params = [timestamp]
            if context:
                where_clause += " AND cluster_context = ?"
                params.append(context)
        else:  # postgresql
            where_clause = "timestamp < %s"
            params = [timestamp]
            if context:
                where_clause += " AND cluster_context = %s"
                params.append(context)

        results = self._base_dao.find_where(where_clause, params, "timestamp DESC", 1)
        return results[0] if results else None

    def get_scan_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Get scan statistics for the last N days."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        # Total scans
        where_clause = "timestamp >= ?"
        params = [cutoff_date]
        if self.dialect == "postgresql":
            where_clause = "timestamp >= %s"

        total_scans = self._base_dao.count_all(where_clause, params)

        # Date range
        if self.dialect == "sqlite":
            date_query = """
                SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date 
                FROM scan_records WHERE timestamp >= ?
            """
        else:
            date_query = """
                SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date 
                FROM scan_records WHERE timestamp >= %s
            """

        date_result = self.db.fetch_one(date_query, [cutoff_date])
        date_range = (
            date_result["min_date"]
            if date_result and date_result["min_date"]
            else datetime.utcnow(),
            date_result["max_date"]
            if date_result and date_result["max_date"]
            else datetime.utcnow(),
        )

        # Scans by context
        if self.dialect == "sqlite":
            context_query = """
                SELECT cluster_context, COUNT(*) as count 
                FROM scan_records 
                WHERE timestamp >= ? 
                GROUP BY cluster_context 
                ORDER BY count DESC
            """
        else:
            context_query = """
                SELECT cluster_context, COUNT(*) as count 
                FROM scan_records 
                WHERE timestamp >= %s 
                GROUP BY cluster_context 
                ORDER BY count DESC
            """

        context_results = self.db.fetch_all(context_query, [cutoff_date])
        scans_by_context = {
            (row["cluster_context"] or "default"): row["count"] for row in context_results
        }

        # Cluster versions
        if self.dialect == "sqlite":
            version_query = """
                SELECT cluster_version, COUNT(*) as count 
                FROM scan_records 
                WHERE timestamp >= ? 
                GROUP BY cluster_version
            """
        else:
            version_query = """
                SELECT cluster_version, COUNT(*) as count 
                FROM scan_records 
                WHERE timestamp >= %s 
                GROUP BY cluster_version
            """

        version_results = self.db.fetch_all(version_query, [cutoff_date])
        cluster_versions = {
            (row["cluster_version"] or "unknown"): row["count"] for row in version_results
        }

        return {
            "total_scans": total_scans,
            "date_range": date_range,
            "scans_by_context": scans_by_context,
            "cluster_versions": cluster_versions,
        }

    def cleanup_old_scans(self, keep_days: int = 90) -> int:
        """Delete scans older than specified days."""
        cutoff_date = datetime.utcnow() - timedelta(days=keep_days)

        where_clause = "timestamp < ?"
        params = [cutoff_date]
        if self.dialect == "postgresql":
            where_clause = "timestamp < %s"

        deleted_count = self._base_dao.delete_where(where_clause, params)
        logger.info(f"Cleaned up {deleted_count} old scan records")
        return deleted_count
