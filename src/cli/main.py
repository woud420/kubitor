"""Main CLI interface using Typer."""

from pathlib import Path
from typing import Optional, List

import typer
from rich import print
from rich.console import Console
from rich.table import Table

from ..core import ResourceOrganizer, ClusterReporter
from ..k8s import K8sClient, ResourceScanner
from ..model.export import ExportFormat, OrganizeBy
from ..model.report import ReportFormat
from ..model.annotations import parse_annotation_filters, AnnotationConfig
from ..database import DatabaseConnection
from ..api import ScanService, AnalysisService
from ..utils.logger import get_logger

# Create CLI app
app = typer.Typer(
    name="k8s-scanner",
    help="Extract Kubernetes resources and generate cluster reports",
    add_completion=True,
)

console = Console()
logger = get_logger(__name__)


from enum import Enum
from typing import Dict, Callable


class ChangeType(Enum):
    """Enum for change types."""

    CREATED = "created"
    DELETED = "deleted"
    UPDATED = "updated"
    UNKNOWN = "unknown"


def _format_created_change(change_type: str) -> str:
    """Format created change type."""
    return f"[green]{change_type.upper()}[/green]"


def _format_deleted_change(change_type: str) -> str:
    """Format deleted change type."""
    return f"[red]{change_type.upper()}[/red]"


def _format_updated_change(change_type: str) -> str:
    """Format updated change type."""
    return f"[yellow]{change_type.upper()}[/yellow]"


def _format_unknown_change(change_type: str) -> str:
    """Format unknown change type."""
    return change_type.upper()


# Dictionary mapping change types to formatting functions
CHANGE_TYPE_FORMATTERS: Dict[ChangeType, Callable[[str], str]] = {
    ChangeType.CREATED: _format_created_change,
    ChangeType.DELETED: _format_deleted_change,
    ChangeType.UPDATED: _format_updated_change,
    ChangeType.UNKNOWN: _format_unknown_change,
}


def _print_changes_table(changes: List[Dict]) -> None:
    """Print changes in a formatted table."""
    if not changes:
        return

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Type", style="cyan")
    table.add_column("Resource", style="green")
    table.add_column("Summary", style="white")

    for change in changes:
        change_type_str = change.get("type", "unknown")
        resource = change.get("resource", {})
        summary = change.get("summary", "No summary available")

        # Format resource name
        resource_name = f"{resource.get('kind', 'Unknown')}/{resource.get('name', 'Unknown')}"
        if resource.get("namespace"):
            resource_name = f"{resource['namespace']}/{resource_name}"

        # Get change type enum
        try:
            change_type_enum = ChangeType(change_type_str)
        except ValueError:
            change_type_enum = ChangeType.UNKNOWN

        # Use dictionary mapping to get formatter function
        formatter = CHANGE_TYPE_FORMATTERS.get(change_type_enum, _format_unknown_change)
        type_display = formatter(change_type_str)

        table.add_row(type_display, resource_name, summary)

    console.print(table)


@app.command()
def scan(
    namespace: Optional[str] = typer.Option(
        None, "--namespace", "-n", help="Kubernetes namespace to scan (default: all namespaces)"
    ),
    output: Path = typer.Option(
        "./k8s-resources", "--output", "-o", help="Output directory for files"
    ),
    context: Optional[str] = typer.Option(
        None, "--context", "-c", help="Kubernetes context to use"
    ),
    organize_by: OrganizeBy = typer.Option(
        OrganizeBy.SERVICE, "--organize-by", help="How to organize the output files"
    ),
    organize_annotation_key: Optional[str] = typer.Option(
        None,
        "--organize-annotation-key",
        help="Specific annotation key to use when organizing by annotation",
    ),
    exclude_types: List[str] = typer.Option(
        [], "--exclude-type", "-e", help="Resource types to exclude (can be used multiple times)"
    ),
    include_types: List[str] = typer.Option(
        [],
        "--include-type",
        "-i",
        help="Only include these resource types (can be used multiple times)",
    ),
    annotation_filters: List[str] = typer.Option(
        [],
        "--annotation-filter",
        "-a",
        help="Filter by annotations (format: key:operator:value, e.g., 'team:equals:platform')",
    ),
    annotation_config: Optional[Path] = typer.Option(
        None, "--annotation-config", help="Path to company-specific annotation configuration file"
    ),
    format: ExportFormat = typer.Option(
        ExportFormat.YAML, "--format", "-f", help="Output format for resource files"
    ),
    store_history: bool = typer.Option(
        True,
        "--store-history/--no-store-history",
        help="Store scan results in database for historical tracking",
    ),
    detect_changes: bool = typer.Option(
        True,
        "--detect-changes/--no-detect-changes",
        help="Detect and report changes since last scan",
    ),
):
    """Scan cluster and extract resources to files."""
    try:
        with console.status("[bold green]Scanning Kubernetes cluster..."):
            # Log scan parameters
            if context:
                console.print(f"Using context: [cyan]{context}[/cyan]")
            if namespace:
                console.print(f"Namespace: [cyan]{namespace}[/cyan]")
            else:
                console.print("Scanning [cyan]all namespaces[/cyan]")

            # Create K8s client and scanner for file export
            client = K8sClient(context=context, namespace=namespace)
            scanner = ResourceScanner(client)

            # Determine resource types to scan
            resource_types = None
            if include_types:
                resource_types = include_types
            elif exclude_types:
                # Get all resources and filter out excluded types
                all_resources = (
                    scanner.scan_all_namespaces()
                    if not namespace
                    else scanner.scan_namespace(namespace)
                )
                included_types = set(r.kind for r in all_resources) - set(exclude_types)
                resource_types = list(included_types)

            # Scan resources for file export
            if namespace:
                resources = scanner.scan_namespace(namespace, resource_types)
            else:
                resources = scanner.scan_all_namespaces(resource_types)

            # Apply annotation filtering if specified
            if annotation_filters or annotation_config:
                # Parse annotation filters
                filters = parse_annotation_filters(annotation_filters)

                # Load annotation config if provided
                config = None
                if annotation_config:
                    config = AnnotationConfig(annotation_config)

                # Filter resources based on annotations
                filtered_resources = []
                for resource in resources:
                    annotations = resource.annotations

                    # Check CLI filters
                    if filters:
                        if all(f.matches(annotations) for f in filters):
                            # Validate against config if provided
                            if config:
                                validation = config.validate_annotations(annotations)
                                if validation["errors"]:
                                    console.print(
                                        f"[red]Resource {resource.kind}/{resource.name} has annotation errors:[/red]"
                                    )
                                    for error in validation["errors"]:
                                        console.print(f"  - {error}")
                                    continue
                                if validation["warnings"]:
                                    console.print(
                                        f"[yellow]Resource {resource.kind}/{resource.name} warnings:[/yellow]"
                                    )
                                    for warning in validation["warnings"]:
                                        console.print(f"  - {warning}")
                            filtered_resources.append(resource)
                    # If no CLI filters, just validate against config
                    elif config:
                        validation = config.validate_annotations(annotations)
                        if not validation["errors"]:
                            filtered_resources.append(resource)
                        else:
                            console.print(
                                f"[red]Excluding {resource.kind}/{resource.name} due to annotation errors[/red]"
                            )

                resources = filtered_resources
                console.print(
                    f"After annotation filtering: [green]{len(resources)}[/green] resources"
                )

            if not resources:
                console.print("[yellow]No resources found matching the criteria[/yellow]")
                raise typer.Exit()

            console.print(f"Found [green]{len(resources)}[/green] resources")

            # Store in database and detect changes if requested
            scan_result = None
            if store_history:
                console.print("Storing scan results in database...")
                scan_service = ScanService()
                try:
                    scan_result = scan_service.perform_scan(
                        context=context, namespace=namespace, resource_types=resource_types
                    )

                    if detect_changes and scan_result.get("changes_detected", 0) > 0:
                        changes = scan_result.get("changes", [])
                        console.print(
                            f"\n[yellow]Detected {len(changes)} changes since last scan:[/yellow]"
                        )
                        _print_changes_table(changes)

                        if scan_result.get("changes_detected", 0) > len(changes):
                            console.print(
                                f"... and {scan_result['changes_detected'] - len(changes)} more changes"
                            )

                    console.print(f"Scan stored with ID: [green]{scan_result['scan_id']}[/green]")
                finally:
                    scan_service.close()

            # Organize and save files
            organizer = ResourceOrganizer(
                output_dir=str(output),
                organize_by=organize_by,
                export_format=format,
                annotation_key=organize_annotation_key,
            )

            console.print(f"Writing resources to [cyan]{output}[/cyan]...")
            organizer.organize_and_save(resources)

            success_msg = "[green]✓[/green] Resources have been saved successfully!"
            if store_history and scan_id:
                success_msg += f" (Scan ID: {scan_id})"
            console.print(success_msg)

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def report(
    namespace: Optional[str] = typer.Option(
        None, "--namespace", "-n", help="Kubernetes namespace to scan (default: all namespaces)"
    ),
    output: Path = typer.Option(
        "./k8s-resources", "--output", "-o", help="Output directory for report"
    ),
    context: Optional[str] = typer.Option(
        None, "--context", "-c", help="Kubernetes context to use"
    ),
    format: ReportFormat = typer.Option(
        ReportFormat.TEXT, "--format", "-f", help="Format for the cluster report"
    ),
    exclude_types: List[str] = typer.Option(
        [], "--exclude-type", "-e", help="Resource types to exclude from statistics"
    ),
    include_types: List[str] = typer.Option(
        [], "--include-type", "-i", help="Only include these resource types in statistics"
    ),
):
    """Generate a comprehensive cluster report."""
    try:
        with console.status("[bold green]Generating cluster report..."):
            # Create client
            client = K8sClient(context=context, namespace=namespace)

            # Create scanner for resource statistics
            scanner = ResourceScanner(
                client=client, include_types=include_types, exclude_types=exclude_types
            )

            # Scan resources
            resources = scanner.scan()

            # Generate report
            reporter = ClusterReporter(client)
            report_content = reporter.generate_report(resources, format)

            # Determine filename
            extension_map = {
                ReportFormat.TEXT: "txt",
                ReportFormat.JSON: "json",
                ReportFormat.YAML: "yaml",
            }
            filename = f"cluster-report.{extension_map[format]}"

            # Save report
            output.mkdir(parents=True, exist_ok=True)
            report_path = output / filename

            with open(report_path, "w") as f:
                f.write(report_content)

            console.print(f"[green]✓[/green] Report saved to: [cyan]{report_path}[/cyan]")

            # Always print report content to console
            console.print("\n" + report_content)

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def upgrade_path(
    current: str = typer.Argument(..., help="Current Kubernetes version (e.g., v1.25.0)"),
    target: Optional[str] = typer.Argument(None, help="Target version (default: next minor)"),
):
    """Show upgrade path and recommendations between versions."""
    try:
        from ..upgrade import UpgradeAdvisor

        advisor = UpgradeAdvisor()
        suggestions = advisor.get_suggestions(current, target)

        # Create a nice table
        table = Table(title="Kubernetes Upgrade Path", show_header=True)
        table.add_column("Aspect", style="cyan", no_wrap=True)
        table.add_column("Details", style="white")

        table.add_row("Current Version", suggestions.current_version)
        table.add_row("Target Version", suggestions.suggested_next_version)

        # Upgrade notes
        if suggestions.upgrade_notes:
            notes = "\n".join(f"• {note}" for note in suggestions.upgrade_notes[:5])
            table.add_row("Upgrade Notes", notes)

        # API deprecations
        if suggestions.api_deprecations:
            deprecations = "\n".join(f"⚠️  {dep}" for dep in suggestions.api_deprecations[:5])
            table.add_row("API Deprecations", deprecations)

        # Required actions
        if suggestions.required_actions:
            actions = "\n".join(f"➤ {action}" for action in suggestions.required_actions[:5])
            table.add_row("Required Actions", actions)

        console.print(table)

        # General recommendations
        if suggestions.general_recommendations:
            console.print("\n[bold]General Recommendations:[/bold]")
            for rec in suggestions.general_recommendations:
                console.print(f"  • {rec}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command("iac-drift")
def iac_drift(
    iac_path: str = typer.Argument(..., help="Path to IaC directory"),
    cluster_path: str = typer.Argument(
        "./k8s-resources", help="Path to cluster export directory (default: ./k8s-resources)"
    ),
    hide_system: bool = typer.Option(
        False, "--hide-system", help="Hide EKS/K8s system resources from analysis"
    ),
):
    """Analyze drift between Infrastructure as Code and running cluster."""
    try:
        from .drift import DriftAnalyzer

        analyzer = DriftAnalyzer(iac_path, cluster_path, hide_system=hide_system)
        analyzer.print_drift_report()

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def history(
    limit: int = typer.Option(10, "--limit", "-l", help="Number of recent scans to show"),
    context: Optional[str] = typer.Option(
        None, "--context", "-c", help="Filter by cluster context"
    ),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="Filter by namespace"),
):
    """Show scan history."""
    try:
        # Use ScanService to get history
        scan_service = ScanService()
        try:
            scans = scan_service.get_scan_history(
                context=context,
                namespace=namespace,
                days=90,  # Look back 90 days
                limit=limit,
            )

            if not scans:
                console.print("[yellow]No scan history found[/yellow]")
                return

            # Display scan history table
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("ID", style="cyan")
            table.add_column("Date", style="green")
            table.add_column("Context", style="blue")
            table.add_column("Namespace", style="yellow")
            table.add_column("Resources", style="white")
            table.add_column("Age", style="dim")

            for scan in scans:
                scan_id = str(scan.get("id", ""))
                timestamp = scan.get("created_at", "")
                if timestamp:
                    try:
                        from datetime import datetime

                        if isinstance(timestamp, str):
                            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        else:
                            dt = timestamp
                        date_str = dt.strftime("%Y-%m-%d %H:%M")
                        age_str = scan.get("age_display", "")
                    except:
                        date_str = str(timestamp)
                        age_str = ""
                else:
                    date_str = "Unknown"
                    age_str = ""

                table.add_row(
                    scan_id,
                    date_str,
                    scan.get("cluster_context") or "default",
                    scan.get("namespace") or "all",
                    str(scan.get("total_resources", 0)),
                    age_str,
                )

            console.print(table)
        finally:
            scan_service.close()

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def changes(
    days: int = typer.Option(7, "--days", "-d", help="Number of days to look back for changes"),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum number of changes to show"),
):
    """Show recent resource changes."""
    try:
        database_service = DatabaseService()
        analyzer = HistoricalAnalyzer(database_service)

        # Get recent scans and find changes
        from datetime import datetime, timedelta

        cutoff_date = datetime.utcnow() - timedelta(days=days)
        scans = database_service.get_scans_in_range(cutoff_date, datetime.utcnow())

        if len(scans) < 2:
            console.print(
                f"[yellow]Need at least 2 scans in the last {days} days for change detection[/yellow]"
            )
            console.print(f"Found {len(scans)} scans")
            raise typer.Exit()

        # Get changes between most recent scans
        latest_scan = scans[0]
        previous_scan = scans[1] if len(scans) > 1 else scans[-1]

        changes = database_service.detect_changes(latest_scan.id, previous_scan.id)

        if not changes:
            console.print("[green]No changes detected[/green]")
        else:
            console.print(f"Found [yellow]{len(changes)}[/yellow] changes")
            analyzer.print_changes_table(changes[:limit])

            if len(changes) > limit:
                console.print(
                    f"... and {len(changes) - limit} more changes (use --limit to see more)"
                )

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def compare(
    scan1_id: int = typer.Argument(..., help="First scan ID"),
    scan2_id: int = typer.Argument(..., help="Second scan ID"),
):
    """Compare two specific scans."""
    try:
        database_service = DatabaseService()
        changes = database_service.detect_changes(scan2_id, scan1_id)

        if not changes:
            console.print("[green]No differences found between scans[/green]")
        else:
            console.print(f"Found [yellow]{len(changes)}[/yellow] differences")
            analyzer = HistoricalAnalyzer(database_service)
            analyzer.print_changes_table(changes)

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def resource_history(
    kind: str = typer.Argument(..., help="Resource kind (e.g., Deployment, Service)"),
    name: str = typer.Argument(..., help="Resource name"),
    namespace: Optional[str] = typer.Option(
        None, "--namespace", "-n", help="Resource namespace (omit for cluster-scoped resources)"
    ),
    limit: int = typer.Option(10, "--limit", "-l", help="Number of historical versions to show"),
):
    """Show history of a specific resource."""
    try:
        database_service = DatabaseService()
        analyzer = HistoricalAnalyzer(database_service)

        timeline = analyzer.get_resource_timeline(kind, name, namespace, limit)

        if not timeline:
            console.print(f"[yellow]No history found for {kind}/{name}[/yellow]")
        else:
            console.print(f"History for [cyan]{kind}/{name}[/cyan]:")
            if namespace:
                console.print(f"Namespace: [magenta]{namespace}[/magenta]")

            for entry in timeline:
                timestamp = entry["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                changed_indicator = " (changed)" if entry.get("changed") else ""
                console.print(f"  {timestamp} - Scan {entry['scan_id']}{changed_indicator}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def summary(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to summarize"),
):
    """Show historical summary and statistics."""
    try:
        database_service = DatabaseService()
        analyzer = HistoricalAnalyzer(database_service)

        summary = database_service.get_historical_summary(days)
        analyzer.print_historical_summary(summary)

        # Also show drift analysis
        drift_analysis = analyzer.analyze_drift(days)
        if "error" not in drift_analysis:
            console.print(f"\n[bold]Drift Analysis (last {days} days):[/bold]")
            console.print(f"Total changes: [yellow]{drift_analysis['total_changes']}[/yellow]")
            console.print(
                f"Stability score: [green]{drift_analysis['stability_score']:.1f}/100[/green]"
            )

            if drift_analysis["most_active_resources"]:
                console.print("\n[bold]Most Active Resources:[/bold]")
                for resource, count in list(drift_analysis["most_active_resources"].items())[:5]:
                    console.print(f"  {resource}: [red]{count}[/red] changes")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def cleanup(
    days: int = typer.Option(90, "--keep-days", help="Keep scan data for this many days"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted without actually deleting"
    ),
):
    """Clean up old scan data."""
    try:
        database_service = DatabaseService()

        if dry_run:
            from datetime import datetime, timedelta

            cutoff_date = datetime.utcnow() - timedelta(days=days)
            scans = database_service.get_scans_in_range(datetime.min, cutoff_date)
            console.print(
                f"[yellow]Would delete {len(scans)} scans older than {days} days[/yellow]"
            )
        else:
            deleted_count = database_service.cleanup_old_scans(days)
            console.print(f"[green]✓[/green] Cleaned up {deleted_count} old scans")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def db_info():
    """Show database information and statistics."""
    try:
        # Use default SQLite database
        from pathlib import Path

        db_path = Path.home() / ".k8s-scanner" / "history.db"
        db_path.parent.mkdir(exist_ok=True)
        database_url = f"sqlite:///{db_path}"

        db = DatabaseConnection(database_url)

        console.print("[bold]Database Information:[/bold]")
        console.print(f"URL: [cyan]{database_url}[/cyan]")
        console.print(f"Type: [cyan]SQLite[/cyan]")
        console.print(f"Path: [cyan]{db_path}[/cyan]")

        # Test connection and get stats
        if db.test_connection():
            console.print("\n[green]✓[/green] Database connection successful")

            stats = db.get_database_stats()
            console.print("\n[bold]Database Statistics:[/bold]")
            console.print(f"Scan Records: [yellow]{stats.get('scan_records', 0)}[/yellow]")
            console.print(f"Resource Records: [yellow]{stats.get('resource_records', 0)}[/yellow]")
            console.print(f"Change Records: [yellow]{stats.get('resource_changes', 0)}[/yellow]")
        else:
            console.print("\n[red]✗[/red] Database connection failed")

        db.close()

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def drift(
    context: str = typer.Argument(help="Cluster context to analyze"),
    days: int = typer.Option(7, "--days", "-d", help="Number of days to analyze"),
    baseline_scan: Optional[int] = typer.Option(
        None, "--baseline", "-b", help="Baseline scan ID (default: oldest in period)"
    ),
):
    """Analyze configuration drift in a cluster."""
    try:
        analysis_service = AnalysisService()
        try:
            console.print(
                f"Analyzing drift for context: [cyan]{context}[/cyan] over [yellow]{days}[/yellow] days"
            )

            drift_analysis = analysis_service.analyze_drift(
                cluster_context=context, baseline_scan_id=baseline_scan, days=days
            )

            if "error" in drift_analysis:
                console.print(f"[red]Error:[/red] {drift_analysis['error']}")
                return

            # Display drift summary
            summary = drift_analysis.get("summary", {})
            console.print(f"\n[bold]Drift Analysis Summary:[/bold]")
            console.print(
                f"Total drift events: [yellow]{summary.get('total_drift_events', 0)}[/yellow]"
            )
            console.print(
                f"Resources with drift: [yellow]{len(summary.get('resources_with_drift', []))}[/yellow]"
            )

            # Show most unstable resources
            unstable = summary.get("most_unstable_resources", [])
            if unstable:
                console.print(f"\n[bold]Most Unstable Resources:[/bold]")
                for resource, count in unstable[:5]:
                    console.print(f"  • {resource}: [red]{count}[/red] changes")

            # Show drift timeline
            drift_points = drift_analysis.get("drift_points", [])
            if drift_points:
                console.print(f"\n[bold]Drift Timeline:[/bold]")
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Scan ID", style="cyan")
                table.add_column("Date", style="green")
                table.add_column("Drift Score", style="yellow")
                table.add_column("Changes", style="white")

                for point in drift_points[-10:]:  # Show last 10 points
                    scan_id = str(point.get("scan_id", ""))
                    timestamp = point.get("timestamp", "")
                    if timestamp:
                        try:
                            from datetime import datetime

                            if isinstance(timestamp, str):
                                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                            else:
                                dt = timestamp
                            date_str = dt.strftime("%Y-%m-%d %H:%M")
                        except:
                            date_str = str(timestamp)
                    else:
                        date_str = "Unknown"

                    drift_score = point.get("drift_score", 0)
                    changes = point.get("changes", {})
                    change_summary = f"A:{len(changes.get('added', []))} R:{len(changes.get('removed', []))} M:{len(changes.get('modified', []))}"

                    table.add_row(scan_id, date_str, str(drift_score), change_summary)

                console.print(table)
        finally:
            analysis_service.close()
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def compare(
    scan1: int = typer.Argument(help="First scan ID"),
    scan2: int = typer.Argument(help="Second scan ID"),
):
    """Compare two scans and show differences."""
    try:
        analysis_service = AnalysisService()
        try:
            console.print(f"Comparing scan [cyan]{scan1}[/cyan] with scan [cyan]{scan2}[/cyan]")

            comparison = analysis_service.compare_scans(scan1, scan2)

            if "error" in comparison:
                console.print(f"[red]Error:[/red] {comparison['error']}")
                return

            # Display comparison summary
            summary = comparison.get("summary", {})
            console.print(f"\n[bold]Comparison Summary:[/bold]")
            console.print(f"Added resources: [green]{summary.get('total_added', 0)}[/green]")
            console.print(f"Removed resources: [red]{summary.get('total_removed', 0)}[/red]")
            console.print(
                f"Modified resources: [yellow]{summary.get('total_modified', 0)}[/yellow]"
            )
            console.print(f"Unchanged resources: [dim]{summary.get('total_unchanged', 0)}[/dim]")
            console.print(f"Net change: [cyan]{summary.get('net_change', 0)}[/cyan]")

            # Show detailed changes
            differences = comparison.get("differences", {})

            # Show added resources
            added = differences.get("added", [])
            if added:
                console.print(f"\n[bold green]Added Resources ({len(added)}):[/bold green]")
                for resource in added[:10]:  # Show first 10
                    name = resource.get("name", "Unknown")
                    kind = resource.get("kind", "Unknown")
                    namespace = resource.get("namespace", "")
                    if namespace:
                        console.print(f"  • [green]+[/green] {kind}/{name} in {namespace}")
                    else:
                        console.print(f"  • [green]+[/green] {kind}/{name}")
                if len(added) > 10:
                    console.print(f"  ... and {len(added) - 10} more")

            # Show removed resources
            removed = differences.get("removed", [])
            if removed:
                console.print(f"\n[bold red]Removed Resources ({len(removed)}):[/bold red]")
                for resource in removed[:10]:  # Show first 10
                    name = resource.get("name", "Unknown")
                    kind = resource.get("kind", "Unknown")
                    namespace = resource.get("namespace", "")
                    if namespace:
                        console.print(f"  • [red]-[/red] {kind}/{name} in {namespace}")
                    else:
                        console.print(f"  • [red]-[/red] {kind}/{name}")
                if len(removed) > 10:
                    console.print(f"  ... and {len(removed) - 10} more")

            # Show modified resources
            modified = differences.get("modified", [])
            if modified:
                console.print(f"\n[bold yellow]Modified Resources ({len(modified)}):[/bold yellow]")
                for change in modified[:10]:  # Show first 10
                    resource = change.get("scan2_resource", {})
                    name = resource.get("name", "Unknown")
                    kind = resource.get("kind", "Unknown")
                    namespace = resource.get("namespace", "")
                    if namespace:
                        console.print(f"  • [yellow]~[/yellow] {kind}/{name} in {namespace}")
                    else:
                        console.print(f"  • [yellow]~[/yellow] {kind}/{name}")
                if len(modified) > 10:
                    console.print(f"  ... and {len(modified) - 10} more")
        finally:
            analysis_service.close()
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def health(
    context: str = typer.Argument(help="Cluster context to analyze"),
    days: int = typer.Option(7, "--days", "-d", help="Number of days to analyze"),
):
    """Generate a cluster health report."""
    try:
        analysis_service = AnalysisService()
        try:
            console.print(f"Generating health report for context: [cyan]{context}[/cyan]")

            report = analysis_service.get_cluster_health_report(context, days)

            if "error" in report:
                console.print(f"[red]Error:[/red] {report['error']}")
                return

            # Display health status
            status = report.get("status", "unknown")
            health_score = report.get("health_score", 0)

            status_colors = {"healthy": "green", "warning": "yellow", "critical": "red"}
            color = status_colors.get(status, "white")

            console.print(f"\n[bold]Cluster Health Report:[/bold]")
            console.print(f"Status: [{color}]{status.upper()}[/{color}]")
            console.print(f"Health Score: [{color}]{health_score}/100[/{color}]")
            console.print(f"Generated: {report.get('generated_at', 'Unknown')}")

            # Display metrics
            metrics = report.get("metrics", {})
            console.print(f"\n[bold]Metrics:[/bold]")
            console.print(f"Total scans: [cyan]{metrics.get('total_scans', 0)}[/cyan]")
            console.print(
                f"Scan frequency: [cyan]{metrics.get('scan_frequency_per_day', 0)}[/cyan] per day"
            )
            console.print(
                f"Latest scan resources: [cyan]{metrics.get('latest_scan_resources', 0)}[/cyan]"
            )

            # Display recommendations
            recommendations = report.get("recommendations", [])
            if recommendations:
                console.print(f"\n[bold]Recommendations:[/bold]")
                for i, rec in enumerate(recommendations, 1):
                    console.print(f"{i}. {rec}")
        finally:
            analysis_service.close()
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def version():
    """Show version information."""
    console.print("[bold]k8s-scanner[/bold] version 0.3.0")
    console.print("A Kubernetes resource extraction and analysis tool with historical tracking")


if __name__ == "__main__":
    app()
