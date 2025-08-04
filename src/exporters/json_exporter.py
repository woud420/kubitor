"""JSON exporter."""

import json
from pathlib import Path
from typing import List

from ..model.kubernetes import K8sResource
from ..utils.logger import get_logger
from .base import Exporter

logger = get_logger(__name__)


class JsonExporter(Exporter):
    """Export resources as JSON files."""

    def export(self, resources: List[K8sResource], group_name: str):
        """Export resources to JSON files."""
        if not resources:
            return

        # Group by kind
        by_kind = {}
        for resource in resources:
            kind = resource.kind.lower()
            if kind not in by_kind:
                by_kind[kind] = []
            by_kind[kind].append(resource)

        # Create group directory
        group_dir = self.output_dir / group_name
        group_dir.mkdir(exist_ok=True)

        # Export each kind
        for kind, kind_resources in by_kind.items():
            filename = f"{kind}.json"
            filepath = group_dir / filename

            cleaned_resources = [self.clean_resource(r) for r in kind_resources]

            with open(filepath, "w") as f:
                json.dump(cleaned_resources, f, indent=2)

            logger.info(f"Exported {len(kind_resources)} {kind}(s) to {filepath}")
