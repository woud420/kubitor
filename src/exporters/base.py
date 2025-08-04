"""Base exporter class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any

from ..model.kubernetes import K8sResource


class Exporter(ABC):
    """Base class for resource exporters."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def export(self, resources: List[K8sResource], group_name: str):
        """Export resources to files."""
        pass

    def clean_resource(self, resource: K8sResource) -> Dict[str, Any]:
        """Clean resource for export."""
        # Convert to dict
        data = {
            "apiVersion": resource.api_version,
            "kind": resource.kind,
            "metadata": resource.metadata.copy(),
        }

        if resource.spec:
            data["spec"] = resource.spec

        if resource.data:
            data["data"] = resource.data

        # Remove runtime fields from metadata
        metadata = data["metadata"]
        fields_to_remove = [
            "managedFields",
            "resourceVersion",
            "uid",
            "generation",
            "creationTimestamp",
            "selfLink",
        ]

        for field in fields_to_remove:
            metadata.pop(field, None)

        # Remove status unless it's a Namespace
        if resource.kind != "Namespace" and resource.status:
            data.pop("status", None)

        return data
