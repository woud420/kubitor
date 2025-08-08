"""Scan API service for managing K8s cluster scans."""

import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path

from ..database.connection import DatabaseConnection
from ..model.repository.scan_repository import ScanRepository
from ..model.repository.resource_repository import ResourceRepository
from ..model.repository.change_repository import ChangeRepository
from ..model.kubernetes import K8sResource
from ..model.cluster import ClusterInfo
from ..k8s import K8sClient, ResourceScanner
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ScanService:
    """High-level service for managing K8s cluster scans."""

    def __init__(self, database_url: Optional[str] = None):
        """Initialize scan service."""
        if database_url is None:
            # Default to SQLite in user's home directory
            db_path = Path.home() / ".k8s-scanner" / "history.db"
            db_path.parent.mkdir(exist_ok=True)
            database_url = f"sqlite:///{db_path}"

        self.db_connection = DatabaseConnection(database_url)
        self.scan_repo = ScanRepository(self.db_connection)
        self.resource_repo = ResourceRepository(self.db_connection)
        self.change_repo = ChangeRepository(self.db_connection)

    def perform_scan(
        self,
        context: Optional[str] = None,
        namespace: Optional[str] = None,
        resource_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Perform a complete cluster scan and store results."""
        logger.info(f"Starting scan for context: {context}, namespace: {namespace}")

        try:
            # Initialize K8s client and scanner
            k8s_client = K8sClient(context=context)
            scanner = ResourceScanner(k8s_client)

            # Get cluster info
            cluster_info = k8s_client.get_cluster_info()

            # Scan resources
            if namespace:
                resources = scanner.scan_namespace(namespace, resource_types)
            else:
                resources = scanner.scan_all_namespaces(resource_types)

            # Create scan record
            scan_id = self.scan_repo.create_scan_record(
                cluster_context=context,
                namespace=namespace,
                scan_type="namespace" if namespace else "cluster",
                total_resources=len(resources),
                cluster_version=cluster_info.server_version.git_version
                if cluster_info.server_version
                else None,
                node_count=len(cluster_info.nodes),
                cluster_info=cluster_info.dict(),
            )

            # Store resources
            resource_records = []
            for resource in resources:
                resource_data = resource.dict()
                resource_hash = self._calculate_resource_hash(resource_data)

                resource_record = {
                    "scan_id": scan_id,
                    "api_version": resource.api_version,
                    "kind": resource.kind,
                    "name": resource.name,
                    "namespace": resource.namespace,
                    "resource_data": json.dumps(resource_data),
                    "resource_hash": resource_hash,
                    "labels": resource.labels,
                    "annotations": resource.annotations,
                }
                resource_records.append(resource_record)

            # Bulk insert resources
            if resource_records:
                self.resource_repo.bulk_create_resources(resource_records)

            # Detect changes if this isn't the first scan
            changes = self._detect_changes(scan_id, context, namespace)

            scan_result = {
                "scan_id": scan_id,
                "timestamp": datetime.utcnow().isoformat(),
                "cluster_context": context,
                "namespace": namespace,
                "total_resources": len(resources),
                "cluster_version": cluster_info.server_version.git_version
                if cluster_info.server_version
                else None,
                "node_count": len(cluster_info.nodes),
                "changes_detected": len(changes),
                "changes": changes[:10] if changes else [],  # Include first 10 changes
            }

            logger.info(f"Scan completed successfully: {scan_id}")
            return scan_result

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            raise

    def get_scan_history(
        self,
        context: Optional[str] = None,
        namespace: Optional[str] = None,
        days: int = 30,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get scan history."""
        logger.info(f"Retrieving scan history for context: {context}, namespace: {namespace}")

        # Use the actual repository interface
        return self.scan_repo.get_recent_scans(
            cluster_context=context, namespace=namespace, days=days, limit=limit
        )

    def get_scan_details(self, scan_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific scan."""
        scan = self.scan_repo.get_scan_by_id(scan_id)
        if not scan:
            return None

        # Get resources for this scan
        resources = self.resource_repo.get_resources_by_scan(scan_id)

        # Get changes detected in this scan
        changes = []
        for resource in resources:
            resource_changes = self.change_repo.get_changes_by_resource(resource["id"])
            changes.extend(resource_changes)

        scan["resources"] = resources
        scan["changes"] = changes
        scan["resource_count"] = len(resources)
        scan["change_count"] = len(changes)

        return scan

    def cleanup_old_scans(self, keep_days: int = 90) -> Dict[str, int]:
        """Clean up old scan data."""
        logger.info(f"Cleaning up scans older than {keep_days} days")

        deleted_scans = self.scan_repo.cleanup_old_scans(keep_days)

        # Note: Resource and change cleanup is handled by foreign key constraints
        # when scans are deleted

        cleanup_result = {
            "deleted_scans": deleted_scans,
            "cutoff_date": (datetime.utcnow() - timedelta(days=keep_days)).isoformat(),
        }

        logger.info(f"Cleanup completed: {cleanup_result}")
        return cleanup_result

    def get_resource_history(
        self,
        api_version: str,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get history of a specific resource."""
        # Find all instances of this resource across scans
        resource_key = f"{api_version}/{kind}/{namespace or ''}/{name}"
        logger.info(f"Getting history for resource: {resource_key}")

        # This would need to be implemented in the repository layer
        # For now, return empty list
        return []

    def _detect_changes(
        self, current_scan_id: int, context: Optional[str] = None, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Detect changes between current scan and previous scan."""
        # Find the most recent previous scan for the same context/namespace
        previous_scans = self.scan_repo.get_recent_scans(
            cluster_context=context,
            namespace=namespace,
            days=30,
            limit=2,  # Current + previous
        )

        if len(previous_scans) < 2:
            logger.info("No previous scan found for change detection")
            return []

        # Get the previous scan (second in the list since they're ordered by recency)
        previous_scan_id = None
        for scan in previous_scans:
            if scan["id"] != current_scan_id:
                previous_scan_id = scan["id"]
                break

        if not previous_scan_id:
            return []

        # Get resources from both scans
        current_resources = self.resource_repo.get_resources_by_scan(current_scan_id)
        previous_resources = self.resource_repo.get_resources_by_scan(previous_scan_id)

        # Create lookup maps
        def make_resource_key(resource):
            return f"{resource['api_version']}/{resource['kind']}/{resource.get('namespace', '')}/{resource['name']}"

        current_map = {make_resource_key(r): r for r in current_resources}
        previous_map = {make_resource_key(r): r for r in previous_resources}

        changes = []

        # Find added resources
        for key, resource in current_map.items():
            if key not in previous_map:
                change_id = self.change_repo.create_change_record(
                    resource_id=resource["id"],
                    change_type="created",
                    change_summary=f"Resource {resource['kind']}/{resource['name']} was created",
                )
                changes.append(
                    {
                        "id": change_id,
                        "type": "created",
                        "resource": resource,
                        "summary": f"Resource {resource['kind']}/{resource['name']} was created",
                    }
                )

        # Find removed resources
        for key, resource in previous_map.items():
            if key not in current_map:
                # Create a placeholder change record for deleted resource
                # Note: We don't have the resource_id from current scan, so we'll use a different approach
                changes.append(
                    {
                        "type": "deleted",
                        "resource": resource,
                        "summary": f"Resource {resource['kind']}/{resource['name']} was deleted",
                    }
                )

        # Find modified resources
        for key in set(current_map.keys()) & set(previous_map.keys()):
            current_resource = current_map[key]
            previous_resource = previous_map[key]

            if current_resource.get("resource_hash") != previous_resource.get("resource_hash"):
                change_id = self.change_repo.create_change_record(
                    resource_id=current_resource["id"],
                    change_type="updated",
                    change_summary=f"Resource {current_resource['kind']}/{current_resource['name']} was updated",
                )
                changes.append(
                    {
                        "id": change_id,
                        "type": "updated",
                        "resource": current_resource,
                        "previous_resource": previous_resource,
                        "summary": f"Resource {current_resource['kind']}/{current_resource['name']} was updated",
                    }
                )

        logger.info(f"Detected {len(changes)} changes")
        return changes

    def _calculate_resource_hash(self, resource_data: Dict[str, Any]) -> str:
        """Calculate hash of resource data for change detection."""
        # Remove fields that change frequently but aren't meaningful
        filtered_data = resource_data.copy()

        # Remove metadata fields that change on every update
        if "metadata" in filtered_data:
            metadata = filtered_data["metadata"].copy()
            metadata.pop("resourceVersion", None)
            metadata.pop("generation", None)
            metadata.pop("managedFields", None)
            metadata.pop("creationTimestamp", None)
            filtered_data["metadata"] = metadata

        # Remove status field as it changes frequently
        filtered_data.pop("status", None)

        # Calculate hash
        resource_json = json.dumps(filtered_data, sort_keys=True)
        return hashlib.sha256(resource_json.encode()).hexdigest()

    def close(self):
        """Close database connection."""
        if self.db_connection:
            self.db_connection.close()
