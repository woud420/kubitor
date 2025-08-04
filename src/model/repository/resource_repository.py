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
        resource_data: Optional[str] = None,
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
            resource_data=resource_data,
            resource_hash=resource_hash,
            labels=labels,
            annotations=annotations,
        )

        logger.debug(f"Created resource record with ID: {resource_id}")
        return resource_id

    def get_resource_by_id(self, resource_id: int) -> Optional[Dict[str, Any]]:
        """Get resource by ID."""
        return self.resource_dao.find_by_id(resource_id)

    def get_resources_by_scan(self, scan_id: int) -> List[Dict[str, Any]]:
        """Get all resources for a scan."""
        return self.resource_dao.find_by_scan_id(scan_id)

    def get_resources_by_kind(
        self, kind: str, namespace: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get resources by kind."""
        return self.resource_dao.find_by_kind(kind, namespace, limit)

    def bulk_create_resources(self, resources: List[Dict[str, Any]]) -> int:
        """Bulk create multiple resource records."""
        logger.info(f"Bulk creating {len(resources)} resource records")

        count = self.resource_dao.bulk_insert(resources)

        logger.info(f"Successfully created {count} resource records")
        return count

    def update_resource_data(
        self, resource_id: int, resource_data: str, resource_hash: str
    ) -> bool:
        """Update resource data and hash."""
        return self.resource_dao.update_resource_data(resource_id, resource_data, resource_hash)

    def find_resource_by_key(
        self, api_version: str, kind: str, name: str, namespace: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Find resource by unique key."""
        return self.resource_dao.find_by_unique_key(api_version, kind, name, namespace)

    def delete_resources_by_scan(self, scan_id: int) -> int:
        """Delete all resources for a scan."""
        count = self.resource_dao.delete_by_scan_id(scan_id)
        logger.info(f"Deleted {count} resources for scan {scan_id}")
        return count

    def get_resource_count_by_kind(self) -> Dict[str, int]:
        """Get count of resources by kind."""
        return self.resource_dao.get_count_by_kind()
