"""Resource organization logic."""

from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Any, Callable, Optional, Set

from ..model.kubernetes import K8sResource
from ..model.export import OrganizeBy, ExportFormat
from ..exporters import YamlExporter, JsonExporter
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ResourceOrganizer:
    """Organizes and exports Kubernetes resources."""

    def __init__(
        self,
        output_dir: str,
        organize_by: OrganizeBy,
        export_format: ExportFormat,
        annotation_key: Optional[str] = None,
    ) -> None:
        if not isinstance(output_dir, str) or not output_dir.strip():
            raise ValueError("Output directory must be a non-empty string")
        if not isinstance(organize_by, OrganizeBy):
            raise TypeError(f"organize_by must be OrganizeBy enum, got {type(organize_by).__name__}")
        if not isinstance(export_format, ExportFormat):
            raise TypeError(f"export_format must be ExportFormat enum, got {type(export_format).__name__}")
        if annotation_key is not None and not isinstance(annotation_key, str):
            raise TypeError(f"annotation_key must be str or None, got {type(annotation_key).__name__}")
            
        self.output_dir = Path(output_dir)
        self.organize_by = organize_by
        self.export_format = export_format
        self.annotation_key = annotation_key

        # Dictionary mapping export formats to exporter classes
        exporter_registry = {
            ExportFormat.YAML: lambda: YamlExporter(self.output_dir),
            ExportFormat.JSON: lambda: JsonExporter(self.output_dir),
        }

        # Select exporter using dictionary pattern
        exporter_factory = exporter_registry.get(
            export_format, lambda: JsonExporter(self.output_dir)
        )
        self.exporter = exporter_factory()

    def organize_and_save(self, resources: List[K8sResource]) -> None:
        """Organize resources and save to files."""
        if not isinstance(resources, list):
            raise TypeError(f"resources must be a list, got {type(resources).__name__}")
        
        # Filter out any invalid resources
        valid_resources = []
        for resource in resources:
            if isinstance(resource, K8sResource):
                valid_resources.append(resource)
            else:
                logger.warning(f"Skipping invalid resource of type {type(resource).__name__}")
        
        if not valid_resources:
            logger.warning("No valid resources to organize")
            return
            
        logger.info(f"Organizing {len(valid_resources)} resources by {self.organize_by}")

        # Organize resources
        organized = self._organize_resources(valid_resources)

        # Export each group
        for group_name, group_resources in organized.items():
            try:
                self.exporter.export(group_resources, group_name)
            except Exception as e:
                logger.error(f"Failed to export group '{group_name}': {e}")

        # Create summary
        self._create_summary(valid_resources, organized)

    def _organize_resources(self, resources: List[K8sResource]) -> Dict[str, List[K8sResource]]:
        """Organize resources based on strategy using enum + dictionary pattern."""
        # Dictionary mapping organization strategies to methods
        organization_strategies = {
            OrganizeBy.SERVICE: self._organize_by_service,
            OrganizeBy.NAMESPACE: self._organize_by_namespace,
            OrganizeBy.TYPE: self._organize_by_type,
            OrganizeBy.ANNOTATION: self._organize_by_annotation,
        }

        organizer_func = organization_strategies.get(self.organize_by, self._organize_by_type)
        return organizer_func(resources)

    def _organize_by_service(self, resources: List[K8sResource]) -> Dict[str, List[K8sResource]]:
        """Organize by service labels."""
        organized = defaultdict(list)

        for resource in resources:
            services = self._get_service_labels(resource)
            for service in services:
                organized[service].append(resource)

        return dict(organized)

    def _organize_by_namespace(self, resources: List[K8sResource]) -> Dict[str, List[K8sResource]]:
        """Organize by namespace."""
        organized = defaultdict(list)

        for resource in resources:
            namespace = resource.namespace or "cluster-scoped"
            organized[namespace].append(resource)

        return dict(organized)

    def _organize_by_type(self, resources: List[K8sResource]) -> Dict[str, List[K8sResource]]:
        """Organize by resource type."""
        organized = defaultdict(list)

        for resource in resources:
            organized[resource.kind].append(resource)

        return dict(organized)

    def _organize_by_annotation(self, resources: List[K8sResource]) -> Dict[str, List[K8sResource]]:
        """Organize by annotation key and value.

        Uses the specified annotation key or finds the first annotation key that varies across resources.
        Falls back to 'uncategorized' if no suitable annotation found.
        """
        organized = defaultdict(list)

        # Use specified annotation key if provided
        if self.annotation_key:
            organizing_key = self.annotation_key
            logger.info(f"Using specified annotation key for organization: {organizing_key}")
        else:
            # Find annotation keys that vary across resources
            all_annotation_keys: Set[str] = set()
            for resource in resources:
                all_annotation_keys.update(resource.annotations.keys())

            # Common annotation keys for organization (in priority order)
            priority_keys = [
                "environment",
                "env",
                "team",
                "owner",
                "app",
                "application",
                "cost-center",
                "cost_center",
                "project",
                "tier",
                "component",
                "version",
            ]

            # Find the first key that exists and has multiple values
            organizing_key = None
            for key in priority_keys:
                if key in all_annotation_keys:
                    # Check if this key has multiple values
                    values = set()
                    for resource in resources:
                        if key in resource.annotations:
                            values.add(resource.annotations[key])
                    if len(values) > 1:
                        organizing_key = key
                        break

            # If no priority key found, use any key with multiple values
            if not organizing_key:
                for key in all_annotation_keys:
                    values = set()
                    for resource in resources:
                        if key in resource.annotations:
                            values.add(resource.annotations[key])
                    if len(values) > 1:
                        organizing_key = key
                        break

        # Organize resources
        if organizing_key:
            logger.info(f"Organizing resources by annotation: {organizing_key}")
            for resource in resources:
                value = resource.annotations.get(organizing_key, "no-annotation")
                group_name = f"{organizing_key}={value}"
                organized[group_name].append(resource)
        else:
            # No suitable annotation found, put all in uncategorized
            logger.warning("No suitable annotation found for organization")
            organized["uncategorized"] = resources

        return dict(organized)

    def _get_service_labels(self, resource: K8sResource) -> List[str]:
        """Extract service identifiers from labels."""
        labels = resource.labels

        # Common service-identifying labels
        service_labels = [
            labels.get("app"),
            labels.get("app.kubernetes.io/name"),
            labels.get("app.kubernetes.io/instance"),
            labels.get("app.kubernetes.io/component"),
            labels.get("service"),
            labels.get("tier"),
        ]

        # Filter out None values and return unique values
        services = [s for s in service_labels if s]
        return list(set(services)) if services else ["uncategorized"]

    def _create_summary(
        self, resources: List[K8sResource], organized: Dict[str, List[K8sResource]]
    ) -> None:
        """Create summary file."""
        summary_path = self.output_dir / "scan-summary.txt"

        with open(summary_path, "w") as f:
            f.write("Kubernetes Resource Scan Summary\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Total resources scanned: {len(resources)}\n")
            f.write(f"Organization method: {self.organize_by}\n")
            f.write(f"Export format: {self.export_format}\n")
            f.write(f"Groups created: {len(organized)}\n\n")

            f.write("Resources by group:\n")
            for group_name, group_resources in sorted(organized.items()):
                f.write(f"\n{group_name}:\n")
                by_kind: Dict[str, int] = defaultdict(int)
                for r in group_resources:
                    by_kind[r.kind] += 1
                for kind, count in sorted(by_kind.items()):
                    f.write(f"  - {kind}: {count}\n")

        logger.info(f"Summary written to {summary_path}")
