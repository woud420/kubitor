"""Cluster-related models."""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class NodeInfo(BaseModel):
    """Information about a Kubernetes node."""

    name: str
    status: str
    roles: str
    version: str
    os: str
    container_runtime: str


class ClusterVersion(BaseModel):
    """Kubernetes version information."""

    major: str
    minor: str
    git_version: str = Field(alias="gitVersion")
    platform: Optional[str] = None

    class Config:
        populate_by_name = True


class ClusterInfo(BaseModel):
    """Cluster information."""

    server_version: Optional[ClusterVersion] = None
    client_version: Optional[ClusterVersion] = None
    nodes: List[NodeInfo] = []


class UpgradeSuggestion(BaseModel):
    """Upgrade path suggestions."""

    current_version: str
    suggested_next_version: str
    upgrade_notes: List[str] = []
    api_deprecations: List[str] = []
    required_actions: List[str] = []
    general_recommendations: List[str] = []
