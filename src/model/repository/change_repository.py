"""Repository for change-related business operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

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
        resource_id: int,
        change_type: str,
        field_path: Optional[str] = None,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
        change_summary: Optional[str] = None,
    ) -> int:
        """Create a new change record."""
        logger.debug(f"Creating change record for resource {resource_id}: {change_type}")

        change_id = self.change_dao.create_change(
            resource_id=resource_id,
            change_type=change_type,
            field_path=field_path,
            old_value=old_value,
            new_value=new_value,
            change_summary=change_summary,
        )

        logger.debug(f"Created change record with ID: {change_id}")
        return change_id

    def get_change_by_id(self, change_id: int) -> Optional[Dict[str, Any]]:
        """Get change by ID."""
        return self.change_dao.find_by_id(change_id)

    def get_changes_by_resource(self, resource_id: int) -> List[Dict[str, Any]]:
        """Get all changes for a resource."""
        return self.change_dao.find_by_resource_id(resource_id)

    def get_changes_by_type(
        self, change_type: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get changes by type."""
        return self.change_dao.find_by_change_type(change_type, limit)

    def get_recent_changes(
        self, hours: int = 24, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get recent changes within specified hours."""
        since = datetime.utcnow() - timedelta(hours=hours)
        return self.change_dao.find_since_date(since, limit)

    def bulk_create_changes(self, changes: List[Dict[str, Any]]) -> int:
        """Bulk create multiple change records."""
        logger.info(f"Bulk creating {len(changes)} change records")

        count = self.change_dao.bulk_insert(changes)

        logger.info(f"Successfully created {count} change records")
        return count

    def get_change_summary_by_resource(self, resource_id: int) -> Dict[str, Any]:
        """Get change summary for a resource."""
        changes = self.get_changes_by_resource(resource_id)

        summary = {
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
        count = self.change_dao.delete_by_resource_id(resource_id)
        logger.info(f"Deleted {count} changes for resource {resource_id}")
        return count

    def get_change_statistics(self) -> Dict[str, Any]:
        """Get change statistics."""
        all_changes = self.change_dao.find_all()

        stats = {
            "total_changes": len(all_changes),
            "change_types": {},
            "changes_by_hour": {},
            "most_changed_resources": {},
        }

        # Analyze changes
        resource_changes = {}
        for change in all_changes:
            # Count by type
            change_type = change.get("change_type", "unknown")
            stats["change_types"][change_type] = stats["change_types"].get(change_type, 0) + 1

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
                stats["changes_by_hour"][hour_key] = stats["changes_by_hour"].get(hour_key, 0) + 1

        # Get most changed resources (top 10)
        sorted_resources = sorted(resource_changes.items(), key=lambda x: x[1], reverse=True)[:10]
        stats["most_changed_resources"] = dict(sorted_resources)

        return stats
