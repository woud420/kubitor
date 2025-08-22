"""Helm client for interacting with Helm."""

import subprocess
import json
import logging
from typing import List, Tuple, Optional, Dict, Any

from ..model.report import HelmRelease, HelmRepository
from ..utils.logger import get_logger

logger = get_logger(__name__)


class HelmClient:
    """Client for Helm operations."""

    def __init__(self) -> None:
        self.helm_available: bool = self._check_helm()

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
        
        if not isinstance(args, list):
            logger.error(f"Expected list for args, got {type(args).__name__}")
            return False, "Invalid arguments"
        
        # Validate args are all strings
        for arg in args:
            if not isinstance(arg, str):
                logger.error(f"All arguments must be strings, got {type(arg).__name__}")
                return False, "Invalid argument type"

        cmd = ["helm"] + args
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True,
                timeout=30  # Add timeout for safety
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr if e.stderr else str(e)
            logger.debug(f"Helm command failed: {' '.join(cmd)}, error: {error_msg}")
            return False, error_msg
        except subprocess.TimeoutExpired:
            logger.error(f"Helm command timed out: {' '.join(cmd)}")
            return False, "Command timed out"
        except Exception as e:
            logger.error(f"Unexpected error executing helm command: {e}")
            return False, f"Unexpected error: {e}"

    def get_releases(self) -> List[HelmRelease]:
        """Get all Helm releases."""
        success, output = self._execute(["list", "--all-namespaces", "-o", "json"])

        if not success:
            return []

        try:
            data = json.loads(output)
            if not isinstance(data, list):
                logger.error(f"Expected list from helm list, got {type(data).__name__}")
                return []
                
            releases: List[HelmRelease] = []

            for item in data:
                if not isinstance(item, dict):
                    logger.warning(f"Skipping non-dict item in helm releases: {type(item).__name__}")
                    continue
                    
                try:
                    release = HelmRelease(
                        name=item.get("name", ""),
                        namespace=item.get("namespace", "default"),
                        revision=str(item.get("revision", "1")),
                        status=item.get("status", "unknown"),
                        chart=item.get("chart", ""),
                        app_version=item.get("app_version"),
                    )
                    releases.append(release)
                except Exception as e:
                    logger.warning(f"Failed to parse helm release item: {e}")
                    continue

            return releases
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Helm releases JSON: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing helm releases: {e}")
            return []

    def get_repositories(self) -> List[HelmRepository]:
        """Get configured Helm repositories."""
        success, output = self._execute(["repo", "list", "-o", "json"])

        if not success:
            return []

        try:
            data = json.loads(output)
            if not isinstance(data, list):
                logger.error(f"Expected list from helm repo list, got {type(data).__name__}")
                return []
                
            repos: List[HelmRepository] = []

            for item in data:
                if not isinstance(item, dict):
                    logger.warning(f"Skipping non-dict item in helm repositories: {type(item).__name__}")
                    continue
                    
                try:
                    repo = HelmRepository(
                        name=item.get("name", ""), 
                        url=item.get("url", "")
                    )
                    repos.append(repo)
                except Exception as e:
                    logger.warning(f"Failed to parse helm repository item: {e}")
                    continue

            return repos
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Helm repositories JSON: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing helm repositories: {e}")
            return []
