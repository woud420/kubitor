"""Analysis API service for historical analysis and reporting."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Callable
from pathlib import Path
from enum import Enum

from ..database.connection import DatabaseConnection
from ..model.repository.historical_repository import HistoricalRepository
from ..model.repository.scan_repository import ScanRepository
from ..model.repository.resource_repository import ResourceRepository
from ..model.repository.change_repository import ChangeRepository
from ..utils.logger import get_logger

logger = get_logger(__name__)


class HealthStatus(Enum):
    """Enum for cluster health status."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"


def _determine_healthy_status(score: int) -> str:
    """Determine healthy status."""
    return HealthStatus.HEALTHY.value


def _determine_warning_status(score: int) -> str:
    """Determine warning status."""
    return HealthStatus.WARNING.value


def _determine_critical_status(score: int) -> str:
    """Determine critical status."""
    return HealthStatus.CRITICAL.value


# Dictionary mapping score ranges to status determination functions
HEALTH_STATUS_RULES: List[tuple[Callable[[int], bool], Callable[[int], str]]] = [
    (lambda score: score >= 80, _determine_healthy_status),
    (lambda score: score >= 60, _determine_warning_status),
    (lambda score: True, _determine_critical_status),  # Default case
]


def determine_health_status(health_score: int) -> str:
    """Determine health status using enum + dictionary pattern."""
    for condition, status_func in HEALTH_STATUS_RULES:
        if condition(health_score):
            return status_func(health_score)
    return HealthStatus.CRITICAL.value  # Fallback


class AnalysisService:
    """High-level service for historical analysis and reporting."""

    def __init__(self, database_url: Optional[str] = None):
        """Initialize analysis service."""
        if database_url is None:
            # Default to SQLite in user's home directory
            db_path = Path.home() / ".k8s-scanner" / "history.db"
            db_path.parent.mkdir(exist_ok=True)
            database_url = f"sqlite:///{db_path}"

        self.db_connection = DatabaseConnection(database_url)
        self.historical_repo = HistoricalRepository(self.db_connection)
        self.scan_repo = ScanRepository(self.db_connection)
        self.resource_repo = ResourceRepository(self.db_connection)
        self.change_repo = ChangeRepository(self.db_connection)

    def analyze_cluster_evolution(self, cluster_context: str, days: int = 30) -> Dict[str, Any]:
        """Analyze how a cluster has evolved over time."""
        logger.info(f"Analyzing cluster evolution for {cluster_context} over {days} days")

        try:
            evolution = self.historical_repo.get_cluster_evolution(cluster_context, days)
            return evolution
        except Exception as e:
            logger.error(f"Failed to analyze cluster evolution: {e}")
            return {"error": str(e)}

    def compare_scans(self, scan_id_1: int, scan_id_2: int) -> Dict[str, Any]:
        """Compare two scans and identify differences."""
        logger.info(f"Comparing scans {scan_id_1} and {scan_id_2}")

        try:
            comparison = self.historical_repo.compare_scans(scan_id_1, scan_id_2)
            return comparison
        except Exception as e:
            logger.error(f"Failed to compare scans: {e}")
            return {"error": str(e)}

    def get_resource_timeline(
        self,
        api_version: str,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        days: int = 30,
    ) -> Dict[str, Any]:
        """Get timeline for a specific resource."""
        resource_key = f"{api_version}/{kind}/{namespace or ''}/{name}"
        logger.info(f"Getting timeline for resource: {resource_key}")

        try:
            timeline = self.historical_repo.get_resource_timeline(
                api_version, kind, name, namespace, days
            )
            return timeline
        except Exception as e:
            logger.error(f"Failed to get resource timeline: {e}")
            return {"error": str(e)}

    def analyze_drift(
        self, cluster_context: str, baseline_scan_id: Optional[int] = None, days: int = 7
    ) -> Dict[str, Any]:
        """Analyze configuration drift from baseline."""
        logger.info(f"Analyzing drift for {cluster_context}")

        try:
            drift_analysis = self.historical_repo.get_drift_analysis(
                cluster_context, baseline_scan_id, days
            )
            return drift_analysis
        except Exception as e:
            logger.error(f"Failed to analyze drift: {e}")
            return {"error": str(e)}

    def get_change_summary(
        self, cluster_context: Optional[str] = None, namespace: Optional[str] = None, days: int = 7
    ) -> Dict[str, Any]:
        """Get summary of changes over time period."""
        logger.info(f"Getting change summary for {days} days")

        try:
            # Get recent changes
            recent_changes = self.change_repo.get_recent_changes(hours=days * 24, limit=100)

            # Get change statistics
            stats = self.change_repo.get_change_statistics()

            # Filter by context/namespace if specified
            if cluster_context or namespace:
                # This would need additional filtering logic in the repository layer
                # For now, return all changes
                pass

            summary = {
                "period_days": days,
                "total_changes": len(recent_changes),
                "recent_changes": recent_changes[:20],  # First 20 changes
                "statistics": stats,
                "cluster_context": cluster_context,
                "namespace": namespace,
            }

            return summary
        except Exception as e:
            logger.error(f"Failed to get change summary: {e}")
            return {"error": str(e)}

    def get_resource_statistics(
        self, cluster_context: Optional[str] = None, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get statistics about resources."""
        logger.info("Getting resource statistics")

        try:
            # Get resource count by kind
            resource_counts = self.resource_repo.get_resource_count_by_kind()

            # Get recent scans for additional context
            recent_scans = self.scan_repo.get_recent_scans(
                cluster_context=cluster_context, namespace=namespace, days=30, limit=10
            )

            statistics = {
                "resource_counts_by_kind": resource_counts,
                "recent_scans": len(recent_scans),
                "cluster_context": cluster_context,
                "namespace": namespace,
                "last_scan": recent_scans[0] if recent_scans else None,
            }

            return statistics
        except Exception as e:
            logger.error(f"Failed to get resource statistics: {e}")
            return {"error": str(e)}

    def get_cluster_health_report(self, cluster_context: str, days: int = 7) -> Dict[str, Any]:
        """Generate a comprehensive cluster health report."""
        logger.info(f"Generating health report for {cluster_context}")

        try:
            # Get recent scans
            recent_scans = self.scan_repo.get_recent_scans(
                cluster_context=cluster_context, days=days, limit=50
            )

            if not recent_scans:
                return {
                    "cluster_context": cluster_context,
                    "status": "no_data",
                    "message": "No recent scan data available",
                }

            # Calculate health metrics
            latest_scan = recent_scans[0]
            scan_frequency = len(recent_scans) / days if days > 0 else 0

            # Get change analysis
            drift_analysis = self.analyze_drift(cluster_context, days=days)
            change_summary = self.get_change_summary(cluster_context=cluster_context, days=days)

            # Health score calculation (simple heuristic)
            health_score = 100

            # Reduce score for high drift
            if isinstance(drift_analysis, dict) and "summary" in drift_analysis:
                drift_events = drift_analysis["summary"].get("total_drift_events", 0)
                if drift_events > 10:
                    health_score -= min(30, drift_events * 2)

            # Reduce score for many changes
            if isinstance(change_summary, dict):
                total_changes = change_summary.get("total_changes", 0)
                if total_changes > 50:
                    health_score -= min(20, (total_changes - 50) // 10)

            # Reduce score for infrequent scanning
            if scan_frequency < 0.5:  # Less than once every 2 days
                health_score -= 20

            health_score = max(0, health_score)

            # Determine status using enum + dictionary pattern
            status = determine_health_status(health_score)

            report = {
                "cluster_context": cluster_context,
                "generated_at": datetime.utcnow().isoformat(),
                "period_days": days,
                "status": status,
                "health_score": health_score,
                "metrics": {
                    "total_scans": len(recent_scans),
                    "scan_frequency_per_day": round(scan_frequency, 2),
                    "latest_scan_resources": latest_scan.get("total_resources", 0),
                    "latest_scan_date": latest_scan.get("created_at"),
                },
                "drift_analysis": drift_analysis,
                "change_summary": change_summary,
                "recommendations": self._generate_recommendations(
                    health_score, scan_frequency, drift_analysis, change_summary
                ),
            }

            return report
        except Exception as e:
            logger.error(f"Failed to generate health report: {e}")
            return {"error": str(e)}

    def _generate_recommendations(
        self,
        health_score: int,
        scan_frequency: float,
        drift_analysis: Dict[str, Any],
        change_summary: Dict[str, Any],
    ) -> List[str]:
        """Generate recommendations based on analysis using enum + dictionary pattern."""
        recommendations = []

        # Health score recommendations
        health_recommendations = {
            lambda score: score
            < 60: "Cluster health is critical - investigate recent changes and drift",
            lambda score: score < 80: "Cluster health needs attention - review configuration drift",
        }

        for condition, recommendation in health_recommendations.items():
            if condition(health_score):
                recommendations.append(recommendation)
                break

        if scan_frequency < 0.5:
            recommendations.append("Increase scanning frequency for better monitoring coverage")

        if isinstance(drift_analysis, dict) and "summary" in drift_analysis:
            drift_events = drift_analysis["summary"].get("total_drift_events", 0)
            if drift_events > 20:
                recommendations.append(
                    "High configuration drift detected - review change management processes"
                )

            unstable_resources = drift_analysis["summary"].get("most_unstable_resources", [])
            if unstable_resources:
                recommendations.append(
                    f"Focus on stabilizing frequently changing resources: {', '.join([r[0] for r in unstable_resources[:3]])}"
                )

        if isinstance(change_summary, dict):
            total_changes = change_summary.get("total_changes", 0)
            if total_changes > 100:
                recommendations.append(
                    "High change volume - consider implementing change freezes or approval processes"
                )

        if not recommendations:
            recommendations.append("Cluster appears healthy - continue regular monitoring")

        return recommendations

    def close(self):
        """Close database connection."""
        if self.db_connection:
            self.db_connection.close()
