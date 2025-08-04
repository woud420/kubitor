"""DAO for resource_changes table operations using composition."""

import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Callable
from enum import Enum

from .base_dao import BaseDAO, BaseSQLiteDAO, BasePostgreSQLDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseDialect(Enum):
    """Enum for database dialects."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class ChangeDAOSQLite(BaseSQLiteDAO):
    """SQLite-specific DAO for resource_changes table."""

    def get_table_name(self) -> str:
        return "resource_changes"


class ChangeDAOPostgreSQL(BasePostgreSQLDAO):
    """PostgreSQL-specific DAO for resource_changes table."""

    def get_table_name(self) -> str:
        return "resource_changes"


# Dictionary mapping dialect enum to DAO class constructor
CHANGE_DAO_REGISTRY: Dict[DatabaseDialect, Callable[[DatabaseConnection], BaseDAO]] = {
    DatabaseDialect.SQLITE: lambda conn: ChangeDAOSQLite(conn),
    DatabaseDialect.POSTGRESQL: lambda conn: ChangeDAOPostgreSQL(conn),
}


class ChangeDAO:
    """Data Access Object for resource_changes table with dialect dispatch."""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        # Use enum + dictionary pattern instead of if/elif
        try:
            dialect_enum = DatabaseDialect(db_connection.dialect)
        except ValueError:
            raise ValueError(f"Unsupported database dialect: {db_connection.dialect}")

        dao_constructor = CHANGE_DAO_REGISTRY.get(dialect_enum)
        if dao_constructor is None:
            raise ValueError(f"No ChangeDAO implementation for dialect: {dialect_enum}")

        self._base_dao = dao_constructor(db_connection)

    def create_change(
        self,
        kind: str,
        namespace: Optional[str],
        name: str,
        change_type: str,
        old_scan_id: Optional[int] = None,
        new_scan_id: Optional[int] = None,
        changed_fields: Optional[Dict[str, Any]] = None,
        diff_summary: Optional[str] = None,
    ) -> int:
        """Create a new change record and return its ID."""
        columns = [
            "kind",
            "namespace",
            "name",
            "change_type",
            "old_scan_id",
            "new_scan_id",
            "changed_fields",
            "diff_summary",
        ]

        if self.db.dialect == "sqlite":
            values = [
                kind,
                namespace,
                name,
                change_type,
                old_scan_id,
                new_scan_id,
                json.dumps(changed_fields) if changed_fields else None,
                diff_summary,
            ]
        else:  # postgresql
            values = [
                kind,
                namespace,
                name,
                change_type,
                old_scan_id,
                new_scan_id,
                changed_fields,
                diff_summary,
            ]

        return self._base_dao.insert_returning_id(columns, values)

    def find_by_id(self, change_id: int) -> Optional[Dict[str, Any]]:
        """Find change by ID."""
        return self._base_dao.find_by_id(change_id)

    def get_changes_between_scans(self, scan1_id: int, scan2_id: int) -> List[Dict[str, Any]]:
        """Get changes between two scans."""
        if self.db.dialect == "sqlite":
            where_clause = "old_scan_id = ? AND new_scan_id = ?"
        else:
            where_clause = "old_scan_id = %s AND new_scan_id = %s"

        params = [scan1_id, scan2_id]
        return self._base_dao.find_where(where_clause, params, "timestamp DESC")

    def get_recent_changes(
        self, days: int = 7, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get recent changes within the last N days."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        if self.db.dialect == "sqlite":
            where_clause = "timestamp >= ?"
        else:
            where_clause = "timestamp >= %s"

        params = [cutoff_date]
        return self._base_dao.find_where(where_clause, params, "timestamp DESC", limit)

    def get_changes_by_resource(
        self, kind: str, name: str, namespace: Optional[str] = None, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get changes for a specific resource."""
        if self.db.dialect == "sqlite":
            where_clause = "kind = ? AND name = ?"
            params = [kind, name]
            if namespace is not None:
                where_clause += " AND namespace = ?"
                params.append(namespace)
            else:
                where_clause += " AND namespace IS NULL"
        else:
            where_clause = "kind = %s AND name = %s"
            params = [kind, name]
            if namespace is not None:
                where_clause += " AND namespace = %s"
                params.append(namespace)
            else:
                where_clause += " AND namespace IS NULL"

        return self._base_dao.find_where(where_clause, params, "timestamp DESC", limit)

    def get_changes_by_type(
        self, change_type: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get changes by type (created, updated, deleted)."""
        if self.db.dialect == "sqlite":
            where_clause = "change_type = ?"
        else:
            where_clause = "change_type = %s"

        params = [change_type]
        return self._base_dao.find_where(where_clause, params, "timestamp DESC", limit)

    def get_change_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Get change statistics for the last N days."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        # Total changes
        if self.db.dialect == "sqlite":
            where_clause = "timestamp >= ?"
            params = [cutoff_date]
        else:
            where_clause = "timestamp >= %s"
            params = [cutoff_date]

        total_changes = self._base_dao.count_all(where_clause, params)

        # Changes by type
        if self.db.dialect == "sqlite":
            type_query = """
                SELECT change_type, COUNT(*) as count 
                FROM resource_changes 
                WHERE timestamp >= ? 
                GROUP BY change_type
            """
        else:
            type_query = """
                SELECT change_type, COUNT(*) as count 
                FROM resource_changes 
                WHERE timestamp >= %s 
                GROUP BY change_type
            """

        type_results = self.db.fetch_all(type_query, [cutoff_date])
        changes_by_type = {row["change_type"]: row["count"] for row in type_results}

        # Changes by resource kind
        if self.db.dialect == "sqlite":
            kind_query = """
                SELECT kind, COUNT(*) as count 
                FROM resource_changes 
                WHERE timestamp >= ? 
                GROUP BY kind 
                ORDER BY count DESC 
                LIMIT 10
            """
        else:
            kind_query = """
                SELECT kind, COUNT(*) as count 
                FROM resource_changes 
                WHERE timestamp >= %s 
                GROUP BY kind 
                ORDER BY count DESC 
                LIMIT 10
            """

        kind_results = self.db.fetch_all(kind_query, [cutoff_date])
        most_changed_kinds = {row["kind"]: row["count"] for row in kind_results}

        # Changes by namespace
        if self.db.dialect == "sqlite":
            namespace_query = """
                SELECT namespace, COUNT(*) as count 
                FROM resource_changes 
                WHERE timestamp >= ? 
                GROUP BY namespace 
                ORDER BY count DESC 
                LIMIT 10
            """
        else:
            namespace_query = """
                SELECT namespace, COUNT(*) as count 
                FROM resource_changes 
                WHERE timestamp >= %s 
                GROUP BY namespace 
                ORDER BY count DESC 
                LIMIT 10
            """

        namespace_results = self.db.fetch_all(namespace_query, [cutoff_date])
        most_active_namespaces = {
            (row["namespace"] or "cluster-scoped"): row["count"] for row in namespace_results
        }

        return {
            "total_changes": total_changes,
            "changes_by_type": changes_by_type,
            "most_changed_kinds": most_changed_kinds,
            "most_active_namespaces": most_active_namespaces,
        }

    def bulk_create_changes(self, changes_data: List[Dict[str, Any]]) -> int:
        """Bulk create change records."""
        if not changes_data:
            return 0

        columns = [
            "kind",
            "namespace",
            "name",
            "change_type",
            "old_scan_id",
            "new_scan_id",
            "changed_fields",
            "diff_summary",
        ]

        if self.db.dialect == "sqlite":
            values_list = [
                [
                    item["kind"],
                    item["namespace"],
                    item["name"],
                    item["change_type"],
                    item.get("old_scan_id"),
                    item.get("new_scan_id"),
                    json.dumps(item.get("changed_fields")) if item.get("changed_fields") else None,
                    item.get("diff_summary"),
                ]
                for item in changes_data
            ]
        else:  # postgresql
            values_list = [
                [
                    item["kind"],
                    item["namespace"],
                    item["name"],
                    item["change_type"],
                    item.get("old_scan_id"),
                    item.get("new_scan_id"),
                    item.get("changed_fields"),
                    item.get("diff_summary"),
                ]
                for item in changes_data
            ]

        return self._base_dao.bulk_insert(columns, values_list)

    def cleanup_old_changes(self, keep_days: int = 90) -> int:
        """Delete changes older than specified days."""
        cutoff_date = datetime.utcnow() - timedelta(days=keep_days)

        if self.db.dialect == "sqlite":
            where_clause = "timestamp < ?"
        else:
            where_clause = "timestamp < %s"

        params = [cutoff_date]
        deleted_count = self._base_dao.delete_where(where_clause, params)
        logger.info(f"Cleaned up {deleted_count} old change records")
        return deleted_count
