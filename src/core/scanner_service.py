"""Enhanced scanner service with database integration."""

from typing import List, Optional, Tuple
from datetime import datetime

from ..model.kubernetes import K8sResource
from ..model.cluster import ClusterInfo
from ..model.database import ScanRecordResponse, ResourceChangeResponse
from ..k8s import K8sClient, ResourceScanner
from ..database import DatabaseService
from ..utils.logger import get_logger

logger = get_logger(__name__)


class EnhancedScannerService:
    """Enhanced scanner service with historical tracking."""

    def __init__(
        self,
        client: K8sClient,
        database_service: Optional[DatabaseService] = None,
        include_types: Optional[List[str]] = None,
        exclude_types: Optional[List[str]] = None,
    ):
        self.client = client
        self.database_service = database_service or DatabaseService()
        self.scanner = ResourceScanner(
            client=client, include_types=include_types, exclude_types=exclude_types
        )

    def perform_scan_with_history(
        self, store_in_db: bool = True, detect_changes: bool = True
    ) -> Tuple[
        List[K8sResource], ClusterInfo, Optional[int], Optional[List[ResourceChangeResponse]]
    ]:
        """Perform scan and optionally store in database with change detection."""
        logger.info("Starting enhanced scan with historical tracking")

        # Perform the scan
        resources = self.scanner.scan()
        cluster_info = self._get_cluster_info()

        scan_id = None
        changes = None

        if store_in_db and self.database_service:
            # Store scan in database
            scan_id = self.database_service.store_scan(
                resources=resources,
                cluster_info=cluster_info,
                context=self.client.context,
                namespace=self.client.namespace,
            )

            # Detect changes if requested
            if detect_changes:
                changes = self.database_service.detect_changes(scan_id)
                if changes:
                    logger.info(f"Detected {len(changes)} changes since last scan")
                else:
                    logger.info("No changes detected since last scan")

        logger.info(f"Enhanced scan completed. Found {len(resources)} resources")
        return resources, cluster_info, scan_id, changes

    def get_scan_history(self, limit: int = 10) -> List[ScanRecordResponse]:
        """Get recent scan history."""
        if not self.database_service:
            return []
        return self.database_service.get_recent_scans(limit)

    def get_resource_history(
        self, kind: str, name: str, namespace: Optional[str] = None, limit: int = 10
    ) -> List[dict]:
        """Get history of a specific resource."""
        if not self.database_service:
            return []
        return self.database_service.get_resource_history(kind, name, namespace, limit)

    def compare_scans(self, scan1_id: int, scan2_id: int) -> List[ResourceChangeResponse]:
        """Compare two specific scans."""
        if not self.database_service:
            return []
        return self.database_service.detect_changes(scan2_id, scan1_id)

    def get_historical_summary(self, days: int = 30) -> dict:
        """Get historical summary."""
        if not self.database_service:
            return {}

        summary = self.database_service.get_historical_summary(days)
        return summary.dict()

    def cleanup_old_data(self, keep_days: int = 90) -> int:
        """Clean up old scan data."""
        if not self.database_service:
            return 0
        return self.database_service.cleanup_old_scans(keep_days)

    def _get_cluster_info(self) -> ClusterInfo:
        """Get cluster information."""
        from ..core.reporter import ClusterReporter

        reporter = ClusterReporter(self.client)
        return reporter._get_cluster_info()
