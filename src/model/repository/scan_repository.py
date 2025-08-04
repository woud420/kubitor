"""Repository for scan-related business operations."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from ..dao.scan_dao import ScanDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class ScanRepository:
    """Repository for scan-related business operations."""

    def __init__(self, db_connection: DatabaseConnection):
        self.scan_dao = ScanDAO(db_connection)

    def create_scan_record(
        self,
        cluster_context: Optional[str] = None,
        namespace: Optional[str] = None,
        scan_type: str = "full",
        total_resources: int = 0,
        cluster_version: Optional[str] = None,
        node_count: Optional[int] = None,
        cluster_info: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Create a new scan record."""
        logger.info(f"Creating scan record for context: {cluster_context}, namespace: {namespace}")

        scan_id = self.scan_dao.create_scan(
            cluster_context=cluster_context,
            namespace=namespace,
            scan_type=scan_type,
            total_resources=total_resources,
            cluster_version=cluster_version,
            node_count=node_count,
            cluster_info=cluster_info,
        )

        logger.info(f"Created scan record with ID: {scan_id}")
        return scan_id

    def get_scan_by_id(self, scan_id: int) -> Optional[Dict[str, Any]]:
        """Get scan by ID."""
        return self.scan_dao.find_by_id(scan_id)

    def get_recent_scans(
        self, limit: int = 10, cluster_context: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get recent scans with business logic."""
        scans = self.scan_dao.get_recent_scans(limit=limit, context=cluster_context)

        # Add computed fields or transformations
        for scan in scans:
            scan["age_days"] = self._calculate_age_days(scan.get("timestamp"))
            scan["resources_per_node"] = self._calculate_resources_per_node(
                scan.get("total_resources", 0), scan.get("node_count", 1)
            )

        return scans

    def get_scans_in_date_range(
        self, start_date: datetime, end_date: datetime, cluster_context: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get scans within a date range."""
        return self.scan_dao.get_scans_in_date_range(start_date, end_date, cluster_context)

    def get_latest_scan_for_context(
        self, cluster_context: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent scan for a given context."""
        return self.scan_dao.get_latest_scan(cluster_context)

    def get_previous_scan(
        self, current_scan: Dict[str, Any], cluster_context: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get the scan that occurred before the current scan."""
        timestamp = current_scan.get("timestamp")
        if not timestamp:
            return None

        return self.scan_dao.get_scan_before_timestamp(timestamp, cluster_context)

    def get_scan_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive scan statistics."""
        stats = self.scan_dao.get_scan_statistics(days)

        # Add business calculations
        stats["avg_scans_per_day"] = stats["total_scans"] / max(days, 1)
        stats["scan_frequency"] = self._calculate_scan_frequency(stats["total_scans"], days)

        return stats

    def cleanup_old_scans(self, keep_days: int = 90) -> Dict[str, Any]:
        """Clean up old scans and return summary."""
        logger.info(f"Cleaning up scans older than {keep_days} days")

        deleted_count = self.scan_dao.cleanup_old_scans(keep_days)

        return {
            "deleted_scans": deleted_count,
            "keep_days": keep_days,
            "cutoff_date": datetime.utcnow() - timedelta(days=keep_days),
        }

    def validate_scan_data(
        self,
        cluster_context: Optional[str],
        namespace: Optional[str],
        total_resources: int,
        cluster_version: Optional[str],
    ) -> Dict[str, List[str]]:
        """Validate scan data and return any errors."""
        errors = []
        warnings = []

        # Validate total_resources
        if total_resources < 0:
            errors.append("total_resources cannot be negative")
        elif total_resources == 0:
            warnings.append("total_resources is 0 - scan may have failed")

        # Validate cluster_version format
        if cluster_version and not cluster_version.startswith("v"):
            warnings.append("cluster_version should start with 'v' (e.g., v1.28.0)")

        # Check for reasonable resource counts
        if total_resources > 10000:
            warnings.append(f"Large resource count ({total_resources}) - scan may take longer")

        return {"errors": errors, "warnings": warnings}

    def _calculate_age_days(self, timestamp: Optional[datetime]) -> Optional[int]:
        """Calculate age in days from timestamp."""
        if not timestamp:
            return None

        if isinstance(timestamp, str):
            # Parse string timestamp if needed
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except:
                return None

        age = datetime.utcnow() - timestamp.replace(tzinfo=None)
        return age.days

    def _calculate_resources_per_node(self, total_resources: int, node_count: int) -> float:
        """Calculate average resources per node."""
        if node_count <= 0:
            return 0.0
        return round(total_resources / node_count, 2)

    def _calculate_scan_frequency(self, total_scans: int, days: int) -> str:
        """Calculate human-readable scan frequency."""
        if total_scans == 0:
            return "No scans"

        avg_per_day = total_scans / max(days, 1)

        if avg_per_day >= 1:
            return f"{avg_per_day:.1f} scans/day"
        else:
            days_per_scan = 1 / avg_per_day
            return f"1 scan every {days_per_scan:.1f} days"
