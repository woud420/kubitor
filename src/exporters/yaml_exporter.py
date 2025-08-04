"""YAML exporter."""

import yaml
from pathlib import Path
from typing import List

from ..model.kubernetes import K8sResource
from ..utils.logger import get_logger
from .base import Exporter

logger = get_logger(__name__)


class YamlExporter(Exporter):
    """Export resources as YAML files."""

    def export(self, resources: List[K8sResource], group_name: str):
        """Export resources to YAML files."""
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
            filename = f"{kind}.yaml"
            filepath = group_dir / filename

            with open(filepath, "w") as f:
                for i, resource in enumerate(kind_resources):
                    if i > 0:
                        f.write("---\n")

                    cleaned = self.clean_resource(resource)
                    yaml.dump(cleaned, f, default_flow_style=False, sort_keys=False)

            logger.info(f"Exported {len(kind_resources)} {kind}(s) to {filepath}")
