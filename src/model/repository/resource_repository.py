"""Repository for resource-related business operations."""

from typing import List, Optional, Dict, Any

from ..dao.resource_dao import ResourceDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class ResourceRepository:
    """Repository for resource-related business operations."""

    def __init__(self, db_connection: DatabaseConnection):
        self.resource_dao = ResourceDAO(db_connection)

    def create_resource_record(
        self,
        scan_id: int,
        api_version: str,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        resource_data: Optional[Dict[str, Any]] = None,
        resource_hash: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
    ) -> int:
        """Create a new resource record."""
        logger.debug(f"Creating resource record: {kind}/{name}")

        resource_id = self.resource_dao.create_resource(
            scan_id=scan_id,
            api_version=api_version,
            kind=kind,
            name=name,
            namespace=namespace,
            resource_data=resource_data or {},
            resource_hash=resource_hash or "",
            labels=labels,
            annotations=annotations,
        )

        logger.debug(f"Created resource record with ID: {resource_id}")
        return resource_id

    def get_resource_by_id(self, resource_id: int) -> Optional[Dict[str, Any]]:
        """Get resource by ID."""
        return self.resource_dao._base_dao.find_by_id(resource_id)

    def get_resources_by_scan(self, scan_id: int) -> List[Dict[str, Any]]:
        """Get all resources for a scan."""
        return self.resource_dao.get_resources_by_scan(scan_id)

    def get_resources_by_kind(
        self, kind: str, namespace: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get resources by kind."""
        return self.resource_dao.find_resources_by_type(kind, None, namespace)

    def bulk_create_resources(self, resources: List[Dict[str, Any]]) -> int:
        """Bulk create multiple resource records."""
        logger.info(f"Bulk creating {len(resources)} resource records")

        count = self.resource_dao.bulk_create_resources(resources)

        logger.info(f"Successfully created {count} resource records")
        return count

    def update_resource_data(
        self, resource_id: int, resource_data: str, resource_hash: str
    ) -> bool:
        """Update resource data and hash."""
        # Use base DAO update_where method
        set_clause = "resource_data = ?, resource_hash = ?"
        where_clause = "id = ?"
        params = [resource_data, resource_hash, resource_id]
        if self.resource_dao.db.dialect == "postgresql":
            set_clause = set_clause.replace("?", "%s")
            where_clause = where_clause.replace("?", "%s")
        rows_affected = self.resource_dao._base_dao.update_where(set_clause, where_clause, params)
        return rows_affected > 0

    def find_resource_by_key(
        self, api_version: str, kind: str, name: str, namespace: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Find resource by unique key."""
        # Use base DAO find_where method
        where_clause = "api_version = ? AND kind = ? AND name = ?"
        params: List[Any] = [api_version, kind, name]
        if namespace is not None:
            where_clause += " AND namespace = ?"
            params.append(namespace)
        else:
            where_clause += " AND namespace IS NULL"
        if self.resource_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
        results = self.resource_dao._base_dao.find_where(where_clause, params, limit=1)
        return results[0] if results else None

    def delete_resources_by_scan(self, scan_id: int) -> int:
        """Delete all resources for a scan."""
        # Use base DAO delete_where method
        where_clause = "scan_id = ?"
        params = [str(scan_id)]
        if self.resource_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
        count = self.resource_dao._base_dao.delete_where(where_clause, params)
        logger.info(f"Deleted {count} resources for scan {scan_id}")
        return count

    def get_resource_count_by_kind(self) -> Dict[str, int]:
        """Get count of resources by kind."""
        # Use database query to get count by kind
        query = "SELECT kind, COUNT(*) as count FROM resource_records GROUP BY kind"
        results = self.resource_dao.db.fetch_all(query)
        return {row["kind"]: row["count"] for row in results}

    def get_resource_history(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get resource history timeline."""
        logger.debug(f"Getting history for {kind}/{name} in {namespace}")
        
        # Build where clause
        where_clause = "kind = ? AND name = ?"
        params: List[Any] = [kind, name]
        
        if namespace is not None:
            where_clause += " AND namespace = ?"
            params.append(namespace)
        else:
            where_clause += " AND namespace IS NULL"
            
        if self.resource_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
            
        results = self.resource_dao._base_dao.find_where(
            where_clause, params, "created_at DESC", limit
        )
        
        # Format results with additional fields
        history = []
        for resource in results:
            history.append({
                "timestamp": resource.get("created_at"),
                "scan_id": resource.get("scan_id"),
                "resource_hash": resource.get("resource_hash"),
                "resource_data": resource.get("resource_data"),
                "labels": resource.get("labels"),
                "annotations": resource.get("annotations")
            })
            
        return history
