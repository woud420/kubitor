"""Report-related models."""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .cluster import ClusterInfo, UpgradeSuggestion


class ReportFormat(str, Enum):
    """Supported report formats."""

    TEXT = "text"
    JSON = "json"
    YAML = "yaml"


class HelmRelease(BaseModel):
    """Helm release information."""

    name: str
    namespace: str
    revision: str = "1"
    status: str = "unknown"
    chart: str = ""
    app_version: Optional[str] = None


class HelmRepository(BaseModel):
    """Helm repository information."""

    name: str
    url: str


class ResourceDetail(BaseModel):
    """Detailed resource information."""
    
    name: str
    kind: str
    api_version: str
    namespace: Optional[str] = None
    helm_release: Optional[str] = None
    helm_chart: Optional[str] = None
    helm_version: Optional[str] = None
    labels: Dict[str, str] = Field(default_factory=dict)


class NamespaceDetail(BaseModel):
    """Detailed namespace information."""
    
    name: str
    resource_count: int = 0
    resources: List[ResourceDetail] = Field(default_factory=list)
    helm_releases: List[str] = Field(default_factory=list)


class ResourceSummary(BaseModel):
    """Resource statistics summary."""

    total_resources: int = 0
    by_namespace: Dict[str, int] = Field(default_factory=dict)
    by_type: Dict[str, int] = Field(default_factory=dict)
    helm_managed: int = 0
    non_helm_managed: int = 0
    namespace_details: List[NamespaceDetail] = Field(default_factory=list)
    resources_by_namespace: Dict[str, List[ResourceDetail]] = Field(default_factory=dict)


class ResourceUpgradeAction(BaseModel):
    """Recommended upgrade action for a specific resource."""

    name: str
    kind: str
    namespace: Optional[str] = None
    current_version: str
    suggested_version: str


class ClusterReport(BaseModel):
    """Complete cluster report."""

    timestamp: datetime
    cluster_info: ClusterInfo
    helm_releases: List[HelmRelease] = Field(default_factory=list)
    helm_repositories: List[HelmRepository] = Field(default_factory=list)
    resources: ResourceSummary
    upgrade_suggestions: Optional[UpgradeSuggestion] = None
    resource_upgrade_actions: List[ResourceUpgradeAction] = Field(default_factory=list)
