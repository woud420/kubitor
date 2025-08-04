"""Repository for historical analysis and reporting."""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta

from ..dao.scan_dao import ScanDAO
from ..dao.resource_dao import ResourceDAO
from ..dao.change_dao import ChangeDAO
from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class HistoricalRepository:
    """Repository for historical analysis and cross-table operations."""

    def __init__(self, db_connection: DatabaseConnection):
        self.scan_dao = ScanDAO(db_connection)
        self.resource_dao = ResourceDAO(db_connection)
        self.change_dao = ChangeDAO(db_connection)
        self.db = db_connection

    def get_cluster_evolution(self, cluster_context: str, days: int = 30) -> Dict[str, Any]:
        """Get cluster evolution over time."""
        since = datetime.utcnow() - timedelta(days=days)

        # Get scans for the cluster
        scans = self.scan_dao.find_by_cluster_context(cluster_context, since)

        evolution = {
            "cluster_context": cluster_context,
            "period_days": days,
            "scan_count": len(scans),
            "resource_evolution": [],
            "summary": {
                "initial_resources": 0,
                "final_resources": 0,
                "peak_resources": 0,
                "total_changes": 0,
            },
        }

        if not scans:
            return evolution

        # Sort scans by timestamp
        sorted_scans = sorted(scans, key=lambda x: x.get("created_at", datetime.min))

        # Build timeline
        for scan in sorted_scans:
            scan_id = scan["id"]
            resources = self.resource_dao.find_by_scan_id(scan_id)
            changes = self.change_dao.find_by_scan_resources(scan_id)

            point = {
                "scan_id": scan_id,
                "timestamp": scan.get("created_at"),
                "resource_count": len(resources),
                "change_count": len(changes),
                "scan_type": scan.get("scan_type", "full"),
                "cluster_version": scan.get("cluster_version"),
                "node_count": scan.get("node_count"),
            }
            evolution["resource_evolution"].append(point)

        # Calculate summary
        if evolution["resource_evolution"]:
            resource_counts = [p["resource_count"] for p in evolution["resource_evolution"]]
            evolution["summary"]["initial_resources"] = resource_counts[0]
            evolution["summary"]["final_resources"] = resource_counts[-1]
            evolution["summary"]["peak_resources"] = max(resource_counts)
            evolution["summary"]["total_changes"] = sum(
                p["change_count"] for p in evolution["resource_evolution"]
            )

        return evolution

    def compare_scans(self, scan_id_1: int, scan_id_2: int) -> Dict[str, Any]:
        """Compare two scans and identify differences."""
        logger.info(f"Comparing scans {scan_id_1} and {scan_id_2}")

        # Get scan details
        scan1 = self.scan_dao.find_by_id(scan_id_1)
        scan2 = self.scan_dao.find_by_id(scan_id_2)

        if not scan1 or not scan2:
            raise ValueError("One or both scans not found")

        # Get resources for both scans
        resources1 = self.resource_dao.find_by_scan_id(scan_id_1)
        resources2 = self.resource_dao.find_by_scan_id(scan_id_2)

        # Create resource maps for comparison
        def make_resource_key(resource):
            return f"{resource['api_version']}/{resource['kind']}/{resource.get('namespace', '')}/{resource['name']}"

        resources1_map = {make_resource_key(r): r for r in resources1}
        resources2_map = {make_resource_key(r): r for r in resources2}

        # Find differences
        added = []
        removed = []
        modified = []
        unchanged = []

        all_keys = set(resources1_map.keys()) | set(resources2_map.keys())

        for key in all_keys:
            if key in resources1_map and key in resources2_map:
                r1, r2 = resources1_map[key], resources2_map[key]
                if r1.get("resource_hash") != r2.get("resource_hash"):
                    modified.append({"key": key, "scan1_resource": r1, "scan2_resource": r2})
                else:
                    unchanged.append(key)
            elif key in resources1_map:
                removed.append(resources1_map[key])
            else:
                added.append(resources2_map[key])

        comparison = {
            "scan1": {
                "id": scan_id_1,
                "timestamp": scan1.get("created_at"),
                "resource_count": len(resources1),
            },
            "scan2": {
                "id": scan_id_2,
                "timestamp": scan2.get("created_at"),
                "resource_count": len(resources2),
            },
            "differences": {
                "added": added,
                "removed": removed,
                "modified": modified,
                "unchanged_count": len(unchanged),
            },
            "summary": {
                "total_added": len(added),
                "total_removed": len(removed),
                "total_modified": len(modified),
                "total_unchanged": len(unchanged),
                "net_change": len(resources2) - len(resources1),
            },
        }

        return comparison

    def get_resource_timeline(
        self,
        api_version: str,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        days: int = 30,
    ) -> Dict[str, Any]:
        """Get timeline for a specific resource."""
        since = datetime.utcnow() - timedelta(days=days)

        # Find all instances of this resource
        resources = self.resource_dao.find_resource_history(
            api_version, kind, name, namespace, since
        )

        timeline = {
            "resource_key": f"{api_version}/{kind}/{namespace or ''}/{name}",
            "period_days": days,
            "timeline": [],
            "summary": {
                "first_seen": None,
                "last_seen": None,
                "total_versions": len(resources),
                "total_changes": 0,
            },
        }

        if not resources:
            return timeline

        # Sort by scan timestamp
        sorted_resources = sorted(resources, key=lambda x: x.get("created_at", datetime.min))

        # Build timeline with changes
        for resource in sorted_resources:
            resource_id = resource["id"]
            changes = self.change_dao.find_by_resource_id(resource_id)

            timeline_entry = {
                "scan_id": resource.get("scan_id"),
                "resource_id": resource_id,
                "timestamp": resource.get("created_at"),
                "resource_hash": resource.get("resource_hash"),
                "changes": changes,
                "change_count": len(changes),
            }
            timeline["timeline"].append(timeline_entry)

        # Update summary
        timeline["summary"]["first_seen"] = sorted_resources[0].get("created_at")
        timeline["summary"]["last_seen"] = sorted_resources[-1].get("created_at")
        timeline["summary"]["total_changes"] = sum(
            entry["change_count"] for entry in timeline["timeline"]
        )

        return timeline

    def get_drift_analysis(
        self, cluster_context: str, baseline_scan_id: Optional[int] = None, days: int = 7
    ) -> Dict[str, Any]:
        """Analyze configuration drift from baseline."""
        # If no baseline specified, use the oldest scan in the period
        since = datetime.utcnow() - timedelta(days=days)
        scans = self.scan_dao.find_by_cluster_context(cluster_context, since)

        if not scans:
            return {"error": "No scans found for cluster in specified period"}

        sorted_scans = sorted(scans, key=lambda x: x.get("created_at", datetime.min))

        if baseline_scan_id is None:
            baseline_scan = sorted_scans[0]
            baseline_scan_id = baseline_scan["id"]
        else:
            baseline_scan = self.scan_dao.find_by_id(baseline_scan_id)
            if not baseline_scan:
                return {"error": "Baseline scan not found"}

        # Compare each subsequent scan with baseline
        drift_analysis = {
            "cluster_context": cluster_context,
            "baseline_scan": {"id": baseline_scan_id, "timestamp": baseline_scan.get("created_at")},
            "period_days": days,
            "drift_points": [],
            "summary": {
                "total_drift_events": 0,
                "resources_with_drift": set(),
                "most_unstable_resources": [],
            },
        }

        # Analyze drift for each scan after baseline
        subsequent_scans = [s for s in sorted_scans if s["id"] != baseline_scan_id]

        for scan in subsequent_scans:
            comparison = self.compare_scans(baseline_scan_id, scan["id"])

            drift_point = {
                "scan_id": scan["id"],
                "timestamp": scan.get("created_at"),
                "drift_score": len(comparison["differences"]["added"])
                + len(comparison["differences"]["removed"])
                + len(comparison["differences"]["modified"]),
                "changes": comparison["differences"],
            }

            drift_analysis["drift_points"].append(drift_point)

            # Track resources with drift
            for resource in (
                comparison["differences"]["added"] + comparison["differences"]["removed"]
            ):
                key = f"{resource['api_version']}/{resource['kind']}/{resource.get('namespace', '')}/{resource['name']}"
                drift_analysis["summary"]["resources_with_drift"].add(key)

            for change in comparison["differences"]["modified"]:
                drift_analysis["summary"]["resources_with_drift"].add(change["key"])

        # Convert set to list for JSON serialization
        drift_analysis["summary"]["resources_with_drift"] = list(
            drift_analysis["summary"]["resources_with_drift"]
        )
        drift_analysis["summary"]["total_drift_events"] = len(drift_analysis["drift_points"])

        # Find most unstable resources (resources that change most frequently)
        resource_change_counts = {}
        for point in drift_analysis["drift_points"]:
            for change in point["changes"]["modified"]:
                key = change["key"]
                resource_change_counts[key] = resource_change_counts.get(key, 0) + 1

        drift_analysis["summary"]["most_unstable_resources"] = sorted(
            resource_change_counts.items(), key=lambda x: x[1], reverse=True
        )[:10]

        return drift_analysis
