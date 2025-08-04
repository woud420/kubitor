"""Kubernetes upgrade path management."""

from .advisor import UpgradeAdvisor
from .versions import KUBERNETES_VERSIONS

__all__ = ["UpgradeAdvisor", "KUBERNETES_VERSIONS"]
