"""Detect management tools for Kubernetes resources."""

from typing import Dict, Any, List, Tuple, Callable
from ..model.management import ManagementTool, ManagementInfo
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ManagementDetector:
    """Detect how Kubernetes resources are managed."""
    
    def __init__(self) -> None:
        # Define detection strategies as a list of tuples (checker, info_getter)
        # Order matters - first match wins
        self._detection_strategies = [
            (self._is_helm_managed, self._get_helm_info),
            (self._is_argocd_managed, self._get_argocd_info),
            (self._is_flux_managed, self._get_flux_info),
            (self._is_kustomize_managed, self._get_kustomize_info),
            (self._is_terraform_managed, self._get_terraform_info),
        ]
    
    def detect(self, resource: Dict[str, Any]) -> ManagementInfo:
        """Detect management tool for a resource."""
        if not isinstance(resource, dict):
            logger.warning(f"Expected dict for resource, got {type(resource).__name__}")
            return ManagementInfo(ManagementTool.MANUAL)
        
        metadata = resource.get("metadata", {})
        if not isinstance(metadata, dict):
            logger.warning("Resource metadata is not a dict")
            return ManagementInfo(ManagementTool.MANUAL)
            
        labels = metadata.get("labels", {})
        annotations = metadata.get("annotations", {})
        owner_refs = metadata.get("ownerReferences", [])
        
        # Ensure we have proper types
        if not isinstance(labels, dict):
            labels = {}
        if not isinstance(annotations, dict):
            annotations = {}
        if not isinstance(owner_refs, list):
            owner_refs = []
        
        # Try each detection strategy
        for checker, info_getter in self._detection_strategies:
            if checker(labels, annotations):
                return info_getter(labels, annotations)
        
        # Check Operator (special case with different parameters)
        if self._is_operator_managed(owner_refs):
            return self._get_operator_info(owner_refs)
        
        # Default to manual if no management tool detected
        return ManagementInfo(ManagementTool.MANUAL)
    
    def _is_helm_managed(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        """Check if resource is managed by Helm."""
        try:
            return (
                labels.get("app.kubernetes.io/managed-by") == "Helm" or
                "helm.sh/release" in labels or
                "meta.helm.sh/release-name" in annotations
            )
        except (AttributeError, TypeError) as e:
            logger.debug(f"Error checking Helm management: {e}")
            return False
    
    def _get_helm_info(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> ManagementInfo:
        """Extract Helm management information."""
        details: Dict[str, Any] = {}
        
        # Get release name
        details["release_name"] = (
            labels.get("app.kubernetes.io/instance") or
            labels.get("helm.sh/release") or
            annotations.get("meta.helm.sh/release-name")
        )
        
        # Get chart info
        details["chart"] = (
            labels.get("helm.sh/chart") or
            labels.get("app.kubernetes.io/name")
        )
        
        # Get version
        details["version"] = labels.get("app.kubernetes.io/version")
        details["chart_version"] = labels.get("helm.sh/chart", "").split("-")[-1] if labels.get("helm.sh/chart") else None
        
        # Get namespace
        details["namespace"] = annotations.get("meta.helm.sh/release-namespace")
        
        return ManagementInfo(ManagementTool.HELM, details)
    
    def _is_argocd_managed(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        """Check if resource is managed by ArgoCD."""
        return (
            "argocd.argoproj.io/instance" in labels or
            "app.kubernetes.io/managed-by" in labels and "argocd" in labels["app.kubernetes.io/managed-by"].lower()
        )
    
    def _get_argocd_info(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> ManagementInfo:
        """Extract ArgoCD management information."""
        details: Dict[str, Any] = {}
        
        details["release_name"] = labels.get("argocd.argoproj.io/instance")
        details["sync_wave"] = annotations.get("argocd.argoproj.io/sync-wave")
        details["hook"] = annotations.get("argocd.argoproj.io/hook")
        
        return ManagementInfo(ManagementTool.ARGOCD, details)
    
    def _is_flux_managed(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        """Check if resource is managed by Flux."""
        return (
            "kustomize.toolkit.fluxcd.io/name" in labels or
            "helm.toolkit.fluxcd.io/name" in labels or
            "fluxcd.io/sync-gc-mark" in labels
        )
    
    def _get_flux_info(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> ManagementInfo:
        """Extract Flux management information."""
        details: Dict[str, Any] = {}
        
        details["release_name"] = (
            labels.get("kustomize.toolkit.fluxcd.io/name") or
            labels.get("helm.toolkit.fluxcd.io/name")
        )
        details["namespace"] = (
            labels.get("kustomize.toolkit.fluxcd.io/namespace") or
            labels.get("helm.toolkit.fluxcd.io/namespace")
        )
        
        return ManagementInfo(ManagementTool.FLUX, details)
    
    def _is_kustomize_managed(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        """Check if resource is managed by Kustomize."""
        return (
            "app.kubernetes.io/managed-by" in labels and 
            "kustomize" in labels["app.kubernetes.io/managed-by"].lower()
        ) or "kustomize.config.k8s.io/origin" in annotations
    
    def _get_kustomize_info(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> ManagementInfo:
        """Extract Kustomize management information."""
        details: Dict[str, Any] = {}
        
        if "kustomize.config.k8s.io/origin" in annotations:
            origin = annotations["kustomize.config.k8s.io/origin"]
            details["origin"] = origin
        
        return ManagementInfo(ManagementTool.KUSTOMIZE, details)
    
    def _is_terraform_managed(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        """Check if resource is managed by Terraform."""
        return (
            "app.kubernetes.io/managed-by" in labels and 
            "terraform" in labels["app.kubernetes.io/managed-by"].lower()
        ) or "kubernetes.io/change-cause" in annotations and "terraform" in annotations["kubernetes.io/change-cause"].lower()
    
    def _get_terraform_info(self, labels: Dict[str, Any], annotations: Dict[str, Any]) -> ManagementInfo:
        """Extract Terraform management information."""
        return ManagementInfo(ManagementTool.TERRAFORM)
    
    def _is_operator_managed(self, owner_refs: List[Dict[str, Any]]) -> bool:
        """Check if resource is managed by an operator."""
        if not owner_refs:
            return False
        
        # Check if owned by a custom resource (not built-in controllers)
        controller_kinds = {
            "ReplicaSet", "Deployment", "StatefulSet", "DaemonSet",
            "Job", "CronJob", "ReplicationController"
        }
        
        for ref in owner_refs:
            if ref.get("controller") and ref.get("kind") not in controller_kinds:
                return True
        
        return False
    
    def _get_operator_info(self, owner_refs: List[Dict[str, Any]]) -> ManagementInfo:
        """Extract operator management information."""
        details: Dict[str, Any] = {}
        
        for ref in owner_refs:
            if ref.get("controller"):
                details["operator_kind"] = ref.get("kind")
                details["operator_name"] = ref.get("name")
                details["operator_api_version"] = ref.get("apiVersion")
                break
        
        return ManagementInfo(ManagementTool.OPERATOR, details)