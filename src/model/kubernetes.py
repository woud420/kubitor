"""Kubernetes resource models."""

from typing import Dict, Any, Optional, List
from pydantic import BaseModel


class ResourceType(BaseModel):
    """Kubernetes resource type information."""

    name: str
    kind: str
    namespaced: bool = False
    api_group: Optional[str] = None
    version: Optional[str] = None


class K8sResource(BaseModel):
    """Kubernetes resource."""

    api_version: str = ""
    kind: str
    metadata: Dict[str, Any]
    spec: Optional[Dict[str, Any]] = None
    status: Optional[Dict[str, Any]] = None
    data: Optional[Dict[str, Any]] = None  # For ConfigMaps/Secrets
    _resource_type: Optional[ResourceType] = None

    @property
    def name(self) -> str:
        """Get resource name."""
        name = self.metadata.get("name", "")
        return str(name) if name is not None else ""

    @property
    def namespace(self) -> Optional[str]:
        """Get resource namespace."""
        return self.metadata.get("namespace")

    @property
    def labels(self) -> Dict[str, str]:
        """Get resource labels."""
        labels = self.metadata.get("labels", {})
        return dict(labels) if isinstance(labels, dict) else {}

    @property
    def annotations(self) -> Dict[str, str]:
        """Get resource annotations."""
        annotations = self.metadata.get("annotations", {})
        return dict(annotations) if isinstance(annotations, dict) else {}

    @property
    def is_helm_managed(self) -> bool:
        """Check if resource is managed by Helm."""
        labels = self.labels
        return "helm.sh/release" in labels or labels.get("app.kubernetes.io/managed-by") == "Helm"
