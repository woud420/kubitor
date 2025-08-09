"""Tests for management models."""

import pytest

from src.model.management import ManagementTool, ManagementInfo


class TestManagementTool:
    """Test ManagementTool enum."""
    
    def test_management_tool_values(self):
        """Test ManagementTool enum values."""
        assert ManagementTool.HELM.value == "Helm"
        assert ManagementTool.ARGOCD.value == "ArgoCD"
        assert ManagementTool.FLUX.value == "Flux"
        assert ManagementTool.OPERATOR.value == "Operator"
        assert ManagementTool.KUSTOMIZE.value == "Kustomize"
        assert ManagementTool.TERRAFORM.value == "Terraform"
        assert ManagementTool.MANUAL.value == "Manual"
        assert ManagementTool.UNKNOWN.value == "Unknown"


class TestManagementInfo:
    """Test ManagementInfo class."""
    
    def test_management_info_creation(self):
        """Test creating ManagementInfo instance."""
        info = ManagementInfo(ManagementTool.HELM)
        assert info.tool == ManagementTool.HELM
        assert info.details == {}
        
        info_with_details = ManagementInfo(
            ManagementTool.HELM,
            {"release_name": "nginx", "version": "1.0.0"}
        )
        assert info_with_details.tool == ManagementTool.HELM
        assert info_with_details.details["release_name"] == "nginx"
        assert info_with_details.details["version"] == "1.0.0"
    
    def test_management_info_properties(self):
        """Test ManagementInfo properties."""
        info = ManagementInfo(
            ManagementTool.HELM,
            {
                "release_name": "nginx",
                "version": "1.0.0",
                "chart": "nginx-ingress",
                "operator_name": "test-operator"
            }
        )
        
        assert info.release_name == "nginx"
        assert info.version == "1.0.0"
        assert info.chart == "nginx-ingress"
        assert info.operator_name == "test-operator"
    
    def test_management_info_str(self):
        """Test ManagementInfo string representation."""
        # Helm with release name
        helm_info = ManagementInfo(
            ManagementTool.HELM,
            {"release_name": "nginx"}
        )
        assert str(helm_info) == "Helm:nginx"
        
        # ArgoCD with release name
        argo_info = ManagementInfo(
            ManagementTool.ARGOCD,
            {"release_name": "my-app"}
        )
        assert str(argo_info) == "ArgoCD:my-app"
        
        # Operator with operator name
        operator_info = ManagementInfo(
            ManagementTool.OPERATOR,
            {"operator_name": "cert-manager"}
        )
        assert str(operator_info) == "Operator:cert-manager"
        
        # Manual without details
        manual_info = ManagementInfo(ManagementTool.MANUAL)
        assert str(manual_info) == "Manual"
        
        # Flux without release name
        flux_info = ManagementInfo(ManagementTool.FLUX)
        assert str(flux_info) == "Flux"