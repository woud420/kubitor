"""Report-related models."""

from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

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
    by_namespace: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    helm_managed: int = 0
    non_helm_managed: int = 0


class ClusterReport(BaseModel):
    """Complete cluster report."""

    timestamp: datetime
    cluster_info: ClusterInfo
    helm_releases: List[HelmRelease] = []
    helm_repositories: List[HelmRepository] = []
    resources: ResourceSummary
    upgrade_suggestions: Optional[UpgradeSuggestion] = None
