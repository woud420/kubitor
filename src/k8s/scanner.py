"""Kubernetes resource scanner."""

from typing import List, Optional

from ..model.kubernetes import K8sResource, ResourceType
from ..utils.logger import get_logger
from .client import K8sClient

logger = get_logger(__name__)


class ResourceScanner:
    """Scans Kubernetes cluster for resources."""

    # Resource types to skip by default
    DEFAULT_SKIP_TYPES = {"Event", "ComponentStatus", "Binding"}

    def __init__(
        self,
        client: K8sClient,
        include_types: Optional[List[str]] = None,
        exclude_types: Optional[List[str]] = None,
    ):
        self.client = client
        self.include_types = set(include_types) if include_types else None
        self.exclude_types = set(exclude_types) if exclude_types else set()
        self.exclude_types.update(self.DEFAULT_SKIP_TYPES)

    def scan(self) -> List[K8sResource]:
        """Scan cluster for all resources."""
        logger.info("Starting cluster scan")

        # Get available resource types
        api_resources = self._get_filtered_resources()
        logger.info(f"Found {len(api_resources)} resource types to scan")

        all_resources = []

        for resource_type in api_resources:
            resources = self._scan_resource_type(resource_type)
            all_resources.extend(resources)

        logger.info(f"Scan complete. Found {len(all_resources)} resources")
        return all_resources

    def _get_filtered_resources(self) -> List[ResourceType]:
        """Get filtered list of API resources."""
        api_resources = self.client.get_api_resources()

        filtered = []
        for resource in api_resources:
            kind = resource.get("kind", "")

            # Apply filters
            if self.include_types and kind not in self.include_types:
                continue
            if kind in self.exclude_types:
                continue

            resource_type = ResourceType(
                name=resource.get("name", ""),
                kind=kind,
                namespaced=resource.get("namespaced", False),
                api_group=resource.get("group"),
                version=resource.get("version"),
            )
            filtered.append(resource_type)

        return filtered

    def _scan_resource_type(self, resource_type: ResourceType) -> List[K8sResource]:
        """Scan all resources of a specific type."""
        logger.debug(f"Scanning {resource_type.kind} resources")

        # Get resources
        data = self.client.get_json(
            resource_type.name,
            all_namespaces=resource_type.namespaced and not self.client.namespace,
        )

        if not data:
            return []

        resources = []
        items = data.get("items", [])

        for item in items:
            try:
                # Create K8sResource model
                resource = K8sResource(
                    api_version=item.get("apiVersion", ""),
                    kind=item.get("kind", resource_type.kind),
                    metadata=item.get("metadata", {}),
                    spec=item.get("spec"),
                    status=item.get("status"),
                    data=item.get("data"),
                    _resource_type=resource_type,
                )
                resources.append(resource)
            except Exception as e:
                logger.error(f"Failed to parse resource: {e}")
                continue

        logger.debug(f"Found {len(resources)} {resource_type.kind} resources")
        return resources
