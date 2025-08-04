"""Historical analysis and comparison utilities."""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ..model.database import ResourceChangeResponse, ScanRecordResponse, HistoricalSummary
from ..database import DatabaseService
from ..utils.logger import get_logger

logger = get_logger(__name__)
console = Console()


class HistoricalAnalyzer:
    """Provides historical analysis and comparison capabilities."""

    def __init__(self, database_service: Optional[DatabaseService] = None):
        self.database_service = database_service or DatabaseService()

    def analyze_drift(self, days: int = 7) -> Dict[str, Any]:
        """Analyze configuration drift over the last N days."""
        logger.info(f"Analyzing drift over the last {days} days")

        cutoff_date = datetime.utcnow() - timedelta(days=days)
        scans = self.database_service.get_scans_in_range(cutoff_date, datetime.utcnow())

        if len(scans) < 2:
            return {
                "error": "Need at least 2 scans for drift analysis",
                "available_scans": len(scans),
            }

        # Compare first and last scan
        oldest_scan = scans[-1]
        newest_scan = scans[0]

        changes = self.database_service.detect_changes(newest_scan.id, oldest_scan.id)

        # Categorize changes
        drift_analysis = {
            "time_range": {
                "start": oldest_scan.timestamp,
                "end": newest_scan.timestamp,
                "days": (newest_scan.timestamp - oldest_scan.timestamp).days,
            },
            "total_changes": len(changes),
            "changes_by_type": self._categorize_changes(changes),
            "changes_by_namespace": self._group_changes_by_namespace(changes),
            "most_active_resources": self._get_most_active_resources(changes),
            "stability_score": self._calculate_stability_score(
                changes, newest_scan.total_resources
            ),
        }

        return drift_analysis

    def generate_change_report(self, changes: List[ResourceChangeResponse]) -> str:
        """Generate a human-readable change report."""
        if not changes:
            return "No changes detected."

        lines = []
        lines.append("RESOURCE CHANGES REPORT")
        lines.append("=" * 50)
        lines.append(f"Total changes: {len(changes)}")
        lines.append("")

        # Group by change type
        by_type = self._categorize_changes(changes)
        for change_type, count in by_type.items():
            lines.append(f"{change_type.upper()}: {count}")
        lines.append("")

        # Show recent changes
        lines.append("RECENT CHANGES:")
        lines.append("-" * 30)

        for change in changes[:20]:  # Show first 20
            timestamp = change.timestamp.strftime("%Y-%m-%d %H:%M")
            lines.append(f"[{timestamp}] {change.change_type.upper()}: {change.kind}/{change.name}")
            if change.namespace:
                lines[-1] += f" (namespace: {change.namespace})"
            if change.diff_summary:
                lines.append(f"  └─ {change.diff_summary}")

        if len(changes) > 20:
            lines.append(f"... and {len(changes) - 20} more changes")

        return "\n".join(lines)

    def print_scan_history_table(self, scans: List[ScanRecordResponse]):
        """Print scan history as a formatted table."""
        if not scans:
            console.print("[yellow]No scan history found[/yellow]")
            return

        table = Table(title="Scan History", show_header=True)
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Timestamp", style="green")
        table.add_column("Context", style="blue")
        table.add_column("Namespace", style="magenta")
        table.add_column("Resources", justify="right", style="yellow")
        table.add_column("K8s Version", style="white")

        for scan in scans:
            table.add_row(
                str(scan.id),
                scan.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                scan.cluster_context or "default",
                scan.namespace or "all",
                str(scan.total_resources),
                scan.cluster_version or "unknown",
            )

        console.print(table)

    def print_changes_table(self, changes: List[ResourceChangeResponse]):
        """Print changes as a formatted table."""
        if not changes:
            console.print("[yellow]No changes found[/yellow]")
            return

        table = Table(title="Resource Changes", show_header=True)
        table.add_column("Timestamp", style="green")
        table.add_column("Type", style="cyan", no_wrap=True)
        table.add_column("Kind", style="blue", no_wrap=True)
        table.add_column("Name", style="white")
        table.add_column("Namespace", style="magenta")
        table.add_column("Change", style="yellow")

        for change in changes:
            # Color code change types
            change_color = {"created": "green", "updated": "yellow", "deleted": "red"}.get(
                change.change_type, "white"
            )

            change_text = Text(change.change_type.upper(), style=change_color)

            table.add_row(
                change.timestamp.strftime("%Y-%m-%d %H:%M"),
                change_text,
                change.kind,
                change.name,
                change.namespace or "cluster-scoped",
                change.diff_summary or "No details",
            )

        console.print(table)

    def print_historical_summary(self, summary: HistoricalSummary):
        """Print historical summary."""
        console.print(
            f"\n[bold]Historical Summary ({summary.date_range[0]} to {summary.date_range[1]})[/bold]"
        )
        console.print(f"Total Scans: [cyan]{summary.total_scans}[/cyan]")

        # Most active namespaces
        if summary.most_active_namespaces:
            console.print("\n[bold]Most Active Namespaces:[/bold]")
            for ns, count in list(summary.most_active_namespaces.items())[:5]:
                console.print(f"  {ns}: [yellow]{count}[/yellow] resources")

        # Most changed resources
        if summary.most_changed_resources:
            console.print("\n[bold]Most Changed Resource Types:[/bold]")
            for kind, count in list(summary.most_changed_resources.items())[:5]:
                console.print(f"  {kind}: [red]{count}[/red] changes")

        # Cluster versions
        if summary.cluster_versions:
            console.print("\n[bold]Cluster Versions:[/bold]")
            for version, count in summary.cluster_versions.items():
                console.print(f"  {version}: [green]{count}[/green] scans")

    def get_resource_timeline(
        self, kind: str, name: str, namespace: Optional[str] = None, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get timeline of changes for a specific resource."""
        history = self.database_service.get_resource_history(kind, name, namespace, limit)

        timeline = []
        for i, record in enumerate(history):
            timeline_entry = {
                "timestamp": record["timestamp"],
                "scan_id": record["scan_id"],
                "resource_hash": record["resource_hash"],
            }

            # Detect changes from previous version
            if i < len(history) - 1:
                prev_record = history[i + 1]
                if record["resource_hash"] != prev_record["resource_hash"]:
                    timeline_entry["changed"] = True
                    # Could add more detailed diff here
                else:
                    timeline_entry["changed"] = False

            timeline.append(timeline_entry)

        return timeline

    def _categorize_changes(self, changes: List[ResourceChangeResponse]) -> Dict[str, int]:
        """Categorize changes by type."""
        by_type = {"created": 0, "updated": 0, "deleted": 0}
        for change in changes:
            by_type[change.change_type] += 1
        return by_type

    def _group_changes_by_namespace(self, changes: List[ResourceChangeResponse]) -> Dict[str, int]:
        """Group changes by namespace."""
        by_namespace = {}
        for change in changes:
            ns = change.namespace or "cluster-scoped"
            by_namespace[ns] = by_namespace.get(ns, 0) + 1
        return dict(sorted(by_namespace.items(), key=lambda x: x[1], reverse=True))

    def _get_most_active_resources(self, changes: List[ResourceChangeResponse]) -> Dict[str, int]:
        """Get most frequently changed resources."""
        by_resource = {}
        for change in changes:
            resource_key = f"{change.kind}/{change.name}"
            by_resource[resource_key] = by_resource.get(resource_key, 0) + 1

        # Return top 10
        sorted_resources = sorted(by_resource.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_resources[:10])

    def _calculate_stability_score(
        self, changes: List[ResourceChangeResponse], total_resources: int
    ) -> float:
        """Calculate stability score (0-100, higher is more stable)."""
        if total_resources == 0:
            return 100.0

        # Simple stability score: fewer changes relative to total resources = more stable
        change_ratio = len(changes) / total_resources
        stability_score = max(0, 100 - (change_ratio * 100))
        return round(stability_score, 2)
