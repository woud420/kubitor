"""History service for historical data operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..database.connection import DatabaseConnection
from ..model.repository.scan_repository import ScanRepository
from ..model.repository.resource_repository import ResourceRepository
from ..model.repository.change_repository import ChangeRepository
from ..model.repository.historical_repository import HistoricalRepository
from ..model.database import ScanRecordResponse, ResourceChangeResponse, HistoricalSummary
from ..utils.logger import get_logger

logger = get_logger(__name__)


class HistoryService:
    """Service for managing historical data and analysis."""
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize history service with repositories."""
        if database_url is None:
            # Default to SQLite in user's home directory
            db_path = Path.home() / ".k8s-scanner" / "history.db"
            db_path.parent.mkdir(exist_ok=True)
            database_url = f"sqlite:///{db_path}"
        
        self.db_connection = DatabaseConnection(database_url)
        self.scan_repo = ScanRepository(self.db_connection)
        self.resource_repo = ResourceRepository(self.db_connection)
        self.change_repo = ChangeRepository(self.db_connection)
        self.historical_repo = HistoricalRepository(self.db_connection)
    
    def get_recent_changes(
        self,
        days: int = 7,
        limit: Optional[int] = None
    ) -> List[ResourceChangeResponse]:
        """Get recent resource changes within the specified days."""
        logger.info(f"Getting changes from last {days} days")
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        scans = self.scan_repo.get_scans_in_range(
            start_date=cutoff_date,
            end_date=datetime.now(timezone.utc)
        )
        
        if len(scans) < 2:
            logger.warning(f"Need at least 2 scans for change detection, found {len(scans)}")
            return []
        
        # Get changes between most recent scans
        latest_scan = scans[0]
        previous_scan = scans[1]
        
        changes = self.change_repo.get_changes_between_scans(
            previous_scan.id,
            latest_scan.id
        )
        
        if limit:
            return changes[:limit]
        return changes
    
    def get_resource_history(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get historical timeline for a specific resource."""
        logger.info(f"Getting history for {kind}/{name} in namespace {namespace}")
        
        return self.resource_repo.get_resource_history(
            kind=kind,
            name=name,
            namespace=namespace,
            limit=limit
        )
    
    def get_resource_timeline(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get timeline with change detection for a specific resource."""
        history = self.get_resource_history(kind, name, namespace, limit)
        
        timeline = []
        for i, record in enumerate(history):
            timeline_entry = {
                "timestamp": record["timestamp"],
                "scan_id": record["scan_id"],
                "resource_hash": record["resource_hash"],
                "changed": False
            }
            
            # Detect changes from previous version
            if i < len(history) - 1:
                prev_record = history[i + 1]
                if record["resource_hash"] != prev_record["resource_hash"]:
                    timeline_entry["changed"] = True
            
            timeline.append(timeline_entry)
        
        return timeline
    
    def get_historical_summary(self, days: int = 30) -> HistoricalSummary:
        """Get comprehensive historical summary statistics."""
        logger.info(f"Generating historical summary for last {days} days")
        return self.historical_repo.get_summary(days)
    
    def cleanup_old_scans(self, days_to_keep: int = 90) -> int:
        """Clean up scan data older than specified days."""
        logger.info(f"Cleaning up scans older than {days_to_keep} days")
        
        deleted_count = self.scan_repo.cleanup_old_scans(days_to_keep)
        logger.info(f"Deleted {deleted_count} old scans")
        
        return deleted_count
    
    def get_scans_in_range(
        self,
        start_date: datetime,
        end_date: datetime,
        context: Optional[str] = None
    ) -> List[ScanRecordResponse]:
        """Get all scans within a date range."""
        return self.scan_repo.get_scans_in_range(
            start_date=start_date,
            end_date=end_date,
            context=context
        )
    
    def compare_scans(
        self,
        scan1_id: int,
        scan2_id: int
    ) -> List[ResourceChangeResponse]:
        """Compare two specific scans and return differences."""
        logger.info(f"Comparing scan {scan1_id} with scan {scan2_id}")
        
        return self.change_repo.get_changes_between_scans(scan1_id, scan2_id)
    
    def analyze_drift(
        self,
        context: Optional[str] = None,
        days: int = 7
    ) -> Dict[str, Any]:
        """Analyze configuration drift over time."""
        logger.info(f"Analyzing drift for context {context} over {days} days")
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        scans = self.scan_repo.get_scans_in_range(
            start_date=cutoff_date,
            end_date=datetime.now(timezone.utc),
            context=context
        )
        
        if len(scans) < 2:
            return {
                "error": "Need at least 2 scans for drift analysis",
                "available_scans": len(scans)
            }
        
        # Compare first and last scan
        oldest_scan = scans[-1]
        newest_scan = scans[0]
        
        changes = self.change_repo.get_changes_between_scans(
            oldest_scan.id,
            newest_scan.id
        )
        
        # Analyze changes
        drift_analysis = {
            "time_range": {
                "start": oldest_scan.timestamp,
                "end": newest_scan.timestamp,
                "days": (newest_scan.timestamp - oldest_scan.timestamp).days
            },
            "total_changes": len(changes),
            "changes_by_type": self._categorize_changes(changes),
            "most_active_resources": self._get_most_active_resources(changes),
            "stability_score": self._calculate_stability_score(
                changes, newest_scan.total_resources
            )
        }
        
        return drift_analysis
    
    def _categorize_changes(self, changes: List[ResourceChangeResponse]) -> Dict[str, int]:
        """Categorize changes by type."""
        by_type = {"created": 0, "modified": 0, "deleted": 0}
        for change in changes:
            change_type = change.change_type
            if change_type in by_type:
                by_type[change_type] += 1
        return by_type
    
    def _get_most_active_resources(
        self,
        changes: List[ResourceChangeResponse]
    ) -> Dict[str, int]:
        """Get most frequently changed resources."""
        by_resource = {}
        for change in changes:
            resource_key = f"{change.kind}/{change.name}"
            by_resource[resource_key] = by_resource.get(resource_key, 0) + 1
        
        # Return top 10
        sorted_resources = sorted(by_resource.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_resources[:10])
    
    def _calculate_stability_score(
        self,
        changes: List[ResourceChangeResponse],
        total_resources: int
    ) -> float:
        """Calculate stability score (0-100, higher is more stable)."""
        if total_resources == 0:
            return 100.0
        
        change_ratio = len(changes) / total_resources
        stability_score = max(0, 100 - (change_ratio * 100))
        return round(stability_score, 2)
    
    def close(self):
        """Close database connection."""
        self.db_connection.close()