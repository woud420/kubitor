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

    class Config:
        underscore_attrs_are_private = False

    @property
    def name(self) -> str:
        """Get resource name."""
        return self.metadata.get("name", "")

    @property
    def namespace(self) -> Optional[str]:
        """Get resource namespace."""
        return self.metadata.get("namespace")

    @property
    def labels(self) -> Dict[str, str]:
        """Get resource labels."""
        return self.metadata.get("labels", {})

    @property
    def annotations(self) -> Dict[str, str]:
        """Get resource annotations."""
        return self.metadata.get("annotations", {})

    @property
    def is_helm_managed(self) -> bool:
        """Check if resource is managed by Helm."""
        labels = self.labels
        annotations = self.annotations
        return (
            "helm.sh/release" in labels
            or labels.get("app.kubernetes.io/managed-by") == "Helm"
            or "meta.helm.sh/release-name" in annotations
        )

    @property
    def helm_release(self) -> Optional[str]:
        """Return the Helm release name if the resource is Helm managed."""
        annotations = self.annotations
        if "meta.helm.sh/release-name" in annotations:
            return annotations["meta.helm.sh/release-name"]
        labels = self.labels
        if "helm.sh/release" in labels:
            return labels["helm.sh/release"]
        if labels.get("app.kubernetes.io/managed-by") == "Helm":
            return labels.get("app.kubernetes.io/instance")
        return None
