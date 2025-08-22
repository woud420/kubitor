"""Management detection model classes."""

from typing import Dict, Any, Optional
from enum import Enum


class ManagementTool(Enum):
    """Enumeration of resource management tools."""
    HELM = "Helm"
    ARGOCD = "ArgoCD"
    FLUX = "Flux"
    OPERATOR = "Operator"
    KUSTOMIZE = "Kustomize"
    TERRAFORM = "Terraform"
    MANUAL = "Manual"
    UNKNOWN = "Unknown"


class ManagementInfo:
    """Information about how a resource is managed."""
    
    def __init__(self, tool: ManagementTool, details: Optional[Dict[str, Any]] = None):
        self.tool = tool
        self.details = details or {}
        
    @property
    def release_name(self) -> Optional[str]:
        """Get release/application name if available."""
        return self.details.get("release_name")
    
    @property
    def version(self) -> Optional[str]:
        """Get version if available."""
        return self.details.get("version")
    
    @property
    def chart(self) -> Optional[str]:
        """Get chart name for Helm resources."""
        return self.details.get("chart")
    
    @property
    def operator_name(self) -> Optional[str]:
        """Get operator name for operator-managed resources."""
        return self.details.get("operator_name")
    
    def __str__(self) -> str:
        if self.tool == ManagementTool.HELM and self.release_name:
            return f"Helm:{self.release_name}"
        elif self.tool == ManagementTool.ARGOCD and self.release_name:
            return f"ArgoCD:{self.release_name}"
        elif self.tool == ManagementTool.OPERATOR and self.operator_name:
            return f"Operator:{self.operator_name}"
        else:
            return self.tool.value