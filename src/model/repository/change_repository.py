"""Repository for change-related business operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone

from ..dao.change_dao import ChangeDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class ChangeRepository:
    """Repository for change-related business operations."""

    def __init__(self, db_connection: DatabaseConnection):
        self.change_dao = ChangeDAO(db_connection)

    def create_change_record(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        change_type: str = "updated",
        old_scan_id: Optional[int] = None,
        new_scan_id: Optional[int] = None,
        changed_fields: Optional[Dict[str, Any]] = None,
        diff_summary: Optional[str] = None,
        # Legacy parameters - kept for compatibility
        resource_id: Optional[int] = None,
        field_path: Optional[str] = None,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
        change_summary: Optional[str] = None,
    ) -> int:
        """Create a new change record."""
        # Use diff_summary if change_summary is provided (legacy)
        if change_summary and not diff_summary:
            diff_summary = change_summary
            
        logger.debug(f"Creating change record for {kind}/{name}: {change_type}")

        change_id = self.change_dao.create_change(
            kind=kind,
            namespace=namespace,
            name=name,
            change_type=change_type,
            old_scan_id=old_scan_id,
            new_scan_id=new_scan_id,
            changed_fields=changed_fields,
            diff_summary=diff_summary,
        )

        logger.debug(f"Created change record with ID: {change_id}")
        return change_id

    def get_change_by_id(self, change_id: int) -> Optional[Dict[str, Any]]:
        """Get change by ID."""
        return self.change_dao.find_by_id(change_id)

    def get_changes_by_resource(self, resource_id: int) -> List[Dict[str, Any]]:
        """Get all changes for a resource."""
        # Use base DAO find_where method since find_by_resource_id doesn't exist
        where_clause = "kind = ? AND name = ?"
        params = [f"resource_{resource_id}", f"resource_{resource_id}"]
        if self.change_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
        return self.change_dao._base_dao.find_where(where_clause, params)

    def get_changes_by_type(
        self, change_type: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get changes by type."""
        # Use base DAO find_where method
        where_clause = "change_type = ?"
        params = [change_type]
        if self.change_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
        return self.change_dao._base_dao.find_where(where_clause, params, "timestamp DESC", limit)

    def get_recent_changes(
        self, hours: int = 24, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get recent changes within specified hours."""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        # Use base DAO find_where method
        where_clause = "timestamp >= ?"
        params = [since.isoformat()]
        if self.change_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
        return self.change_dao._base_dao.find_where(where_clause, params, "timestamp DESC", limit)

    def bulk_create_changes(self, changes: List[Dict[str, Any]]) -> int:
        """Bulk create multiple change records."""
        logger.info(f"Bulk creating {len(changes)} change records")

        count = self.change_dao.bulk_create_changes(changes)

        logger.info(f"Successfully created {count} change records")
        return count

    def get_change_summary_by_resource(self, resource_id: int) -> Dict[str, Any]:
        """Get change summary for a resource."""
        changes = self.get_changes_by_resource(resource_id)

        summary: Dict[str, Any] = {
            "total_changes": len(changes),
            "change_types": {},
            "latest_change": None,
            "first_change": None,
        }

        if changes:
            # Group by change type
            for change in changes:
                change_type = change.get("change_type", "unknown")
                summary["change_types"][change_type] = (
                    summary["change_types"].get(change_type, 0) + 1
                )

            # Sort by timestamp
            sorted_changes = sorted(changes, key=lambda x: x.get("detected_at", datetime.min))
            summary["first_change"] = sorted_changes[0]
            summary["latest_change"] = sorted_changes[-1]

        return summary

    def delete_changes_by_resource(self, resource_id: int) -> int:
        """Delete all changes for a resource."""
        # Use base DAO delete_where method since delete_by_resource_id doesn't exist
        where_clause = "kind = ? AND name = ?"
        params = [f"resource_{resource_id}", f"resource_{resource_id}"]
        if self.change_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
        count = self.change_dao._base_dao.delete_where(where_clause, params)
        logger.info(f"Deleted {count} changes for resource {resource_id}")
        return count

    def get_change_statistics(self) -> Dict[str, Any]:
        """Get change statistics."""
        all_changes = self.change_dao._base_dao.find_all()

        stats = {
            "total_changes": len(all_changes),
            "change_types": {},
            "changes_by_hour": {},
            "most_changed_resources": {},
        }

        # Analyze changes
        resource_changes: Dict[str, int] = {}
        for change in all_changes:
            # Count by type
            change_type = change.get("change_type", "unknown")
            stats["change_types"][change_type] = stats["change_types"].get(change_type, 0) + 1  # type: ignore

            # Count by resource
            resource_id = change.get("resource_id")
            if resource_id:
                resource_changes[resource_id] = resource_changes.get(resource_id, 0) + 1

            # Count by hour (simplified)
            detected_at = change.get("detected_at")
            if detected_at:
                hour_key = (
                    detected_at.strftime("%Y-%m-%d %H:00")
                    if isinstance(detected_at, datetime)
                    else str(detected_at)[:13]
                )
                stats["changes_by_hour"][hour_key] = stats["changes_by_hour"].get(hour_key, 0) + 1  # type: ignore

        # Get most changed resources (top 10)
        sorted_resources = sorted(resource_changes.items(), key=lambda x: x[1], reverse=True)[:10]
        stats["most_changed_resources"] = dict(sorted_resources)

        return stats

    def get_changes_between_scans(
        self, scan1_id: int, scan2_id: int
    ) -> List[Dict[str, Any]]:
        """Get changes between two specific scans."""
        logger.debug(f"Getting changes between scans {scan1_id} and {scan2_id}")
        
        # Use base DAO to find changes for either scan
        where_clause = "old_scan_id = ? OR new_scan_id = ? OR old_scan_id = ? OR new_scan_id = ?"
        params = [scan1_id, scan1_id, scan2_id, scan2_id]
        
        if self.change_dao.db.dialect == "postgresql":
            where_clause = where_clause.replace("?", "%s")
            
        changes = self.change_dao._base_dao.find_where(
            where_clause, params, "detected_at DESC"
        )
        
        # Filter to only changes between the specific scans
        filtered_changes = []
        for change in changes:
            old_scan_id = change.get('old_scan_id')
            new_scan_id = change.get('new_scan_id')
            
            # Include if change is between the two scans
            if ((old_scan_id == scan1_id and new_scan_id == scan2_id) or
                (old_scan_id == scan2_id and new_scan_id == scan1_id)):
                filtered_changes.append(change)
                
        return filtered_changes
