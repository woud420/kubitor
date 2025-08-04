"""Helm client for interacting with Helm."""

import subprocess
import json
from typing import List, Tuple, Optional

from ..model.report import HelmRelease, HelmRepository
from ..utils.logger import get_logger

logger = get_logger(__name__)


class HelmClient:
    """Client for Helm operations."""

    def __init__(self):
        self.helm_available = self._check_helm()

    def _check_helm(self) -> bool:
        """Check if Helm is available."""
        try:
            subprocess.run(["helm", "version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("Helm not available")
            return False

    def _execute(self, args: List[str]) -> Tuple[bool, str]:
        """Execute Helm command."""
        if not self.helm_available:
            return False, "Helm not available"

        cmd = ["helm"] + args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            return False, e.stderr

    def get_releases(self) -> List[HelmRelease]:
        """Get all Helm releases."""
        success, output = self._execute(["list", "--all-namespaces", "-o", "json"])

        if not success:
            return []

        try:
            data = json.loads(output)
            releases = []

            for item in data:
                release = HelmRelease(
                    name=item.get("name", ""),
                    namespace=item.get("namespace", "default"),
                    revision=str(item.get("revision", "1")),
                    status=item.get("status", "unknown"),
                    chart=item.get("chart", ""),
                    app_version=item.get("app_version"),
                )
                releases.append(release)

            return releases
        except json.JSONDecodeError:
            logger.error("Failed to parse Helm releases")
            return []

    def get_repositories(self) -> List[HelmRepository]:
        """Get configured Helm repositories."""
        success, output = self._execute(["repo", "list", "-o", "json"])

        if not success:
            return []

        try:
            data = json.loads(output)
            repos = []

            for item in data:
                repo = HelmRepository(name=item.get("name", ""), url=item.get("url", ""))
                repos.append(repo)

            return repos
        except json.JSONDecodeError:
            logger.error("Failed to parse Helm repositories")
            return []
