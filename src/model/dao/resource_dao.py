"""DAO for resource_records table operations using direct SQL."""

import json
from typing import List, Optional, Dict, Any, Tuple, Callable
from enum import Enum

from .base_dao import BaseDAO, BaseSQLiteDAO, BasePostgreSQLDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseDialect(Enum):
    """Enum for database dialects."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class ResourceDAOSQLite(BaseSQLiteDAO):
    """SQLite-specific DAO for resource_records table."""

    def get_table_name(self) -> str:
        return "resource_records"


class ResourceDAOPostgreSQL(BasePostgreSQLDAO):
    """PostgreSQL-specific DAO for resource_records table."""

    def get_table_name(self) -> str:
        return "resource_records"


# Dictionary mapping dialect enum to DAO class constructor
RESOURCE_DAO_REGISTRY: Dict[DatabaseDialect, Callable[[DatabaseConnection], BaseDAO]] = {
    DatabaseDialect.SQLITE: lambda conn: ResourceDAOSQLite(conn),
    DatabaseDialect.POSTGRESQL: lambda conn: ResourceDAOPostgreSQL(conn),
}


class ResourceDAO:
    """Data Access Object for resource_records table with dialect dispatch."""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        # Use enum + dictionary pattern instead of if/elif
        try:
            dialect_enum = DatabaseDialect(db_connection.dialect)
        except ValueError:
            raise ValueError(f"Unsupported database dialect: {db_connection.dialect}")

        dao_constructor = RESOURCE_DAO_REGISTRY.get(dialect_enum)
        if dao_constructor is None:
            raise ValueError(f"No ResourceDAO implementation for dialect: {dialect_enum}")

        self._base_dao = dao_constructor(db_connection)

    def create_resource(
        self,
        scan_id: int,
        api_version: str,
        kind: str,
        namespace: Optional[str],
        name: str,
        resource_data: Dict[str, Any],
        resource_hash: str,
        is_helm_managed: bool = False,
        helm_release: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
    ) -> int:
        """Create a new resource record and return its ID."""
        if self.db.dialect == "sqlite":
            query = """
                INSERT INTO resource_records 
                (scan_id, api_version, kind, namespace, name, resource_data, resource_hash, is_helm_managed, helm_release, labels, annotations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = [
                scan_id,
                api_version,
                kind,
                namespace,
                name,
                json.dumps(resource_data),
                resource_hash,
                is_helm_managed,
                helm_release,
                json.dumps(labels) if labels else None,
                json.dumps(annotations) if annotations else None,
            ]
        else:  # postgresql
            query = """
                INSERT INTO resource_records 
                (scan_id, api_version, kind, namespace, name, resource_data, resource_hash, is_helm_managed, helm_release, labels, annotations)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            params = [
                scan_id,
                api_version,
                kind,
                namespace,
                name,
                resource_data,
                resource_hash,
                is_helm_managed,
                helm_release,  # PostgreSQL handles JSON natively
                labels,
                annotations,
            ]

        return self.db.insert_returning_id(query, params)

    def bulk_create_resources(self, resources_data: List[Dict[str, Any]]) -> int:
        """Bulk create resource records."""
        if not resources_data:
            return 0

        if self.db.dialect == "sqlite":
            query = """
                INSERT INTO resource_records 
                (scan_id, api_version, kind, namespace, name, resource_data, resource_hash, is_helm_managed, helm_release, labels, annotations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            # Convert data for SQLite
            converted_data = []
            for item in resources_data:
                # Handle backward compatibility - some tests may not have labels/annotations
                labels = item.get("labels", {})
                annotations = item.get("annotations", {})

                converted_item = [
                    item["scan_id"],
                    item["api_version"],
                    item["kind"],
                    item["namespace"],
                    item["name"],
                    json.dumps(item["resource_data"])
                    if isinstance(item["resource_data"], dict)
                    else item["resource_data"],
                    item["resource_hash"],
                    item.get("is_helm_managed", False),
                    item.get("helm_release"),
                    json.dumps(labels) if labels else None,
                    json.dumps(annotations) if annotations else None,
                ]
                converted_data.append(converted_item)
        else:  # postgresql
            query = """
                INSERT INTO resource_records 
                (scan_id, api_version, kind, namespace, name, resource_data, resource_hash, is_helm_managed, helm_release, labels, annotations)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # PostgreSQL can handle JSON directly
            converted_data = []
            for item in resources_data:
                # Handle backward compatibility
                labels = item.get("labels", {})
                annotations = item.get("annotations", {})

                converted_item = [
                    item["scan_id"],
                    item["api_version"],
                    item["kind"],
                    item["namespace"],
                    item["name"],
                    item["resource_data"],
                    item["resource_hash"],
                    item.get("is_helm_managed", False),
                    item.get("helm_release"),
                    labels,
                    annotations,
                ]
                converted_data.append(converted_item)

        # Execute bulk insert
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, converted_data)
            return len(resources_data)

    def get_resources_by_scan(self, scan_id: int) -> List[Dict[str, Any]]:
        """Get all resources for a specific scan."""
        where_clause = "scan_id = ?"
        params = [scan_id]

        if self.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")

        return self.find_where(where_clause, params)

    def get_resource_history(
        self, kind: str, name: str, namespace: Optional[str] = None, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get history of a specific resource with scan timestamps."""
        if self.db.dialect == "sqlite":
            query = """
                SELECT r.*, s.timestamp 
                FROM resource_records r
                JOIN scan_records s ON r.scan_id = s.id
                WHERE r.kind = ? AND r.name = ?
            """
            params = [kind, name]

            if namespace is not None:
                query += " AND r.namespace = ?"
                params.append(namespace)
            else:
                query += " AND r.namespace IS NULL"

            query += " ORDER BY s.timestamp DESC LIMIT ?"
            params.append(limit)
        else:  # postgresql
            query = """
                SELECT r.*, s.timestamp 
                FROM resource_records r
                JOIN scan_records s ON r.scan_id = s.id
                WHERE r.kind = %s AND r.name = %s
            """
            params = [kind, name]

            if namespace is not None:
                query += " AND r.namespace = %s"
                params.append(namespace)
            else:
                query += " AND r.namespace IS NULL"

            query += " ORDER BY s.timestamp DESC LIMIT %s"
            params.append(limit)

        return self.db.fetch_all(query, params)

    def find_resources_by_type(
        self, kind: str, scan_id: Optional[int] = None, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Find resources by type, optionally filtered by scan and namespace."""
        where_clause = "kind = ?"
        params = [kind]

        if scan_id:
            where_clause += " AND scan_id = ?"
            params.append(scan_id)

        if namespace is not None:
            where_clause += " AND namespace = ?"
            params.append(namespace)

        if self.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")

        return self.find_where(where_clause, params)

    def get_resource_counts_by_type(self, scan_id: int) -> Dict[str, int]:
        """Get resource counts by type for a specific scan."""
        if self.db.dialect == "sqlite":
            query = """
                SELECT kind, COUNT(*) as count 
                FROM resource_records 
                WHERE scan_id = ? 
                GROUP BY kind
            """
        else:
            query = """
                SELECT kind, COUNT(*) as count 
                FROM resource_records 
                WHERE scan_id = %s 
                GROUP BY kind
            """

        results = self.db.fetch_all(query, [scan_id])
        return {row["kind"]: row["count"] for row in results}

    def get_resource_counts_by_namespace(self, scan_id: int) -> Dict[str, int]:
        """Get resource counts by namespace for a specific scan."""
        if self.db.dialect == "sqlite":
            query = """
                SELECT namespace, COUNT(*) as count 
                FROM resource_records 
                WHERE scan_id = ? 
                GROUP BY namespace
            """
        else:
            query = """
                SELECT namespace, COUNT(*) as count 
                FROM resource_records 
                WHERE scan_id = %s 
                GROUP BY namespace
            """

        results = self.db.fetch_all(query, [scan_id])
        return {(row["namespace"] or "cluster-scoped"): row["count"] for row in results}

    def get_helm_managed_count(self, scan_id: int) -> int:
        """Get count of Helm-managed resources for a scan."""
        where_clause = "scan_id = ? AND is_helm_managed = ?"
        params = [scan_id, True]

        if self.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")

        return self.count_all(where_clause, params)

    def get_non_helm_managed_count(self, scan_id: int) -> int:
        """Get count of non-Helm-managed resources for a scan."""
        where_clause = "scan_id = ? AND is_helm_managed = ?"
        params = [scan_id, False]

        if self.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")

        return self.count_all(where_clause, params)

    def get_most_active_resources(self, days: int = 30, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most frequently changing resources."""
        from datetime import datetime, timedelta

        cutoff_date = datetime.utcnow() - timedelta(days=days)

        if self.db.dialect == "sqlite":
            query = """
                SELECT r.kind, r.name, r.namespace, COUNT(DISTINCT r.resource_hash) as change_count
                FROM resource_records r
                JOIN scan_records s ON r.scan_id = s.id
                WHERE s.timestamp >= ?
                GROUP BY r.kind, r.name, r.namespace
                ORDER BY change_count DESC
                LIMIT ?
            """
        else:
            query = """
                SELECT r.kind, r.name, r.namespace, COUNT(DISTINCT r.resource_hash) as change_count
                FROM resource_records r
                JOIN scan_records s ON r.scan_id = s.id
                WHERE s.timestamp >= %s
                GROUP BY r.kind, r.name, r.namespace
                ORDER BY change_count DESC
                LIMIT %s
            """

        results = self.db.fetch_all(query, [cutoff_date, limit])
        return [
            {
                "kind": row["kind"],
                "name": row["name"],
                "namespace": row["namespace"] or "cluster-scoped",
                "change_count": row["change_count"],
            }
            for row in results
        ]

    def get_namespace_activity(self, days: int = 30) -> Dict[str, int]:
        """Get activity by namespace (resource count)."""
        from datetime import datetime, timedelta

        cutoff_date = datetime.utcnow() - timedelta(days=days)

        if self.db.dialect == "sqlite":
            query = """
                SELECT r.namespace, COUNT(*) as count
                FROM resource_records r
                JOIN scan_records s ON r.scan_id = s.id
                WHERE s.timestamp >= ?
                GROUP BY r.namespace
                ORDER BY count DESC
            """
        else:
            query = """
                SELECT r.namespace, COUNT(*) as count
                FROM resource_records r
                JOIN scan_records s ON r.scan_id = s.id
                WHERE s.timestamp >= %s
                GROUP BY r.namespace
                ORDER BY count DESC
            """

        results = self.db.fetch_all(query, [cutoff_date])
        return {(row["namespace"] or "cluster-scoped"): row["count"] for row in results}

    def find_resources_by_annotation(
        self,
        annotation_key: str,
        annotation_value: Optional[str] = None,
        scan_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Find resources by annotation key and optionally value."""
        if self.db.dialect == "sqlite":
            if annotation_value is None:
                # Check if annotation key exists
                query = """
                    SELECT * FROM resource_records
                    WHERE annotations IS NOT NULL 
                    AND json_extract(annotations, '$.' || ? ) IS NOT NULL
                """
                params = [annotation_key]
            else:
                # Check if annotation key has specific value
                query = """
                    SELECT * FROM resource_records
                    WHERE annotations IS NOT NULL 
                    AND json_extract(annotations, '$.' || ? ) = ?
                """
                params = [annotation_key, annotation_value]

            if scan_id:
                query += " AND scan_id = ?"
                params.append(scan_id)
        else:  # postgresql
            if annotation_value is None:
                # Check if annotation key exists
                query = """
                    SELECT * FROM resource_records
                    WHERE annotations ? %s
                """
                params = [annotation_key]
            else:
                # Check if annotation key has specific value
                query = """
                    SELECT * FROM resource_records
                    WHERE annotations @> %s::jsonb
                """
                params = [json.dumps({annotation_key: annotation_value})]

            if scan_id:
                query += " AND scan_id = %s"
                params.append(scan_id)

        return self.db.fetch_all(query, params)

    def get_resources_with_annotations(
        self, annotation_filters: List[Dict[str, Any]], scan_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get resources matching multiple annotation filters.

        annotation_filters: List of dicts with keys:
            - key: annotation key
            - operator: 'equals', 'contains', 'exists', etc.
            - value: annotation value (optional for 'exists')
        """
        # This would need more complex SQL generation based on filters
        # For now, return empty list - would implement based on specific database features
        logger.warning("Complex annotation filtering not yet implemented in DAO")
        return []

    def find_resources_by_hash(self, resource_hash: str) -> List[Dict[str, Any]]:
        """Find resources with the same hash (identical content)."""
        where_clause = "resource_hash = ?"
        params = [resource_hash]

        if self.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")

        return self.find_where(where_clause, params)

    def cleanup_resources_by_scan_ids(self, scan_ids: List[int]) -> int:
        """Delete all resources for given scan IDs."""
        if not scan_ids:
            return 0

        placeholders = self._build_placeholders(len(scan_ids))
        where_clause = f"scan_id IN ({placeholders})"

        deleted_count = self.delete_where(where_clause, scan_ids)
        logger.info(f"Cleaned up {deleted_count} resource records")
        return deleted_count
