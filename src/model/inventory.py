"""Inventory model classes."""

from typing import List, Dict, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

if TYPE_CHECKING:
    from .management import ManagementInfo


@dataclass
class Application:
    """Represents an application in the cluster."""
    name: str
    namespace: str
    kind: str  # Deployment, StatefulSet, DaemonSet, etc.
    version: Optional[str] = None
    management: Optional['ManagementInfo'] = None
    replicas: int = 0
    containers: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    
    @property
    def display_name(self) -> str:
        """Get display name for the application."""
        if self.management and self.management.release_name:
            return self.management.release_name
        return self.name
    
    @property
    def management_tool(self) -> str:
        """Get management tool name."""
        if self.management:
            return self.management.tool.value
        from .management import ManagementTool
        return ManagementTool.UNKNOWN.value


@dataclass
class ClusterInventory:
    """Complete cluster inventory."""
    cluster_name: str
    kubernetes_version: str
    node_count: int
    scan_timestamp: datetime
    applications: List[Application] = field(default_factory=list)
    
    @property
    def total_applications(self) -> int:
        """Total number of applications."""
        return len(self.applications)
    
    @property
    def applications_by_management(self) -> Dict[str, List[Application]]:
        """Group applications by management tool."""
        grouped = defaultdict(list)
        for app in self.applications:
            grouped[app.management_tool].append(app)
        return dict(grouped)
    
    @property
    def applications_by_namespace(self) -> Dict[str, List[Application]]:
        """Group applications by namespace."""
        grouped = defaultdict(list)
        for app in self.applications:
            grouped[app.namespace].append(app)
        return dict(grouped)