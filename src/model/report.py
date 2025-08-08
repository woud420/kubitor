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


class ResourceSummary(BaseModel):
    """Resource statistics summary."""

    total_resources: int = 0
    by_namespace: Dict[str, int] = Field(default_factory=dict)
    by_type: Dict[str, int] = Field(default_factory=dict)
    helm_managed: int = 0
    non_helm_managed: int = 0


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
