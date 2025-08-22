"""Kubernetes client wrapper."""

import subprocess
import json
import shlex
from typing import List, Tuple, Optional, Dict, Any

from ..utils.logger import get_logger
from ..model.cluster import ClusterInfo, ClusterVersion, NodeInfo
from ..core.exceptions import (
    KubernetesConnectionError,
    KubernetesCommandError,
    ResourceNotFoundError,
    InputSanitizationError,
    ValidationError
)

logger = get_logger(__name__)


class K8sClient:
    """Wrapper for kubectl commands."""

    def __init__(self, context: Optional[str] = None, namespace: Optional[str] = None):
        self.context = self._sanitize_identifier(context) if context else None
        self.namespace = self._sanitize_identifier(namespace) if namespace else None
        self._verify_kubectl()

    def _sanitize_identifier(self, value: str) -> str:
        """Sanitize Kubernetes identifiers (context, namespace, resource names).
        
        Args:
            value: The identifier to sanitize
            
        Returns:
            The sanitized identifier
            
        Raises:
            InputSanitizationError: If the identifier contains invalid characters
        """
        if not value:
            return value
            
        # Kubernetes identifiers must be lowercase alphanumeric, dash, or dot
        # and must start and end with alphanumeric character
        import re
        if not re.match(r'^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$', value):
            raise InputSanitizationError(
                f"Invalid Kubernetes identifier: {value}",
                value,
                "kubernetes_identifier"
            )
        return value
    
    def _sanitize_resource_type(self, resource_type: str) -> str:
        """Sanitize resource type names.
        
        Args:
            resource_type: The resource type to sanitize
            
        Returns:
            The sanitized resource type
            
        Raises:
            InputSanitizationError: If the resource type contains invalid characters
        """
        if not resource_type:
            raise ValidationError("Resource type cannot be empty")
            
        # Resource types are alphanumeric with optional dots and dashes
        import re
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9]?$', resource_type):
            raise InputSanitizationError(
                f"Invalid resource type: {resource_type}",
                resource_type,
                "resource_type"
            )
        return resource_type

    def _verify_kubectl(self) -> None:
        """Verify kubectl is available and configured.
        
        Raises:
            KubernetesConnectionError: If kubectl is not available or configured
        """
        try:
            result = subprocess.run(
                ["kubectl", "version", "--client", "-o", "json"],
                capture_output=True,
                text=True,
                check=True,
                timeout=30  # Add timeout for security
            )
            logger.debug("kubectl verified successfully")
        except subprocess.CalledProcessError as e:
            raise KubernetesConnectionError(
                "kubectl is not installed or not in PATH",
                self.context
            ) from e
        except FileNotFoundError as e:
            raise KubernetesConnectionError(
                "kubectl command not found. Please install kubectl.",
                self.context
            ) from e
        except subprocess.TimeoutExpired as e:
            raise KubernetesConnectionError(
                "kubectl verification timed out",
                self.context
            ) from e

    def _build_command(self, args: List[str]) -> List[str]:
        """Build kubectl command with context and namespace.
        
        Args:
            args: Command arguments to append to kubectl
            
        Returns:
            Complete command list ready for subprocess execution
            
        Raises:
            ValidationError: If args contain invalid elements
        """
        if not args:
            raise ValidationError("Command arguments cannot be empty")
            
        # Sanitize all arguments
        sanitized_args = []
        for arg in args:
            if not isinstance(arg, str):
                raise ValidationError(f"All arguments must be strings, got {type(arg)}")
            # Basic sanitization - no shell metacharacters
            if any(char in arg for char in ';&|$`><(){}[]\\"\'\''):
                raise InputSanitizationError(
                    f"Argument contains shell metacharacters: {arg}",
                    arg,
                    "command_argument"
                )
            sanitized_args.append(arg)
        
        cmd = ["kubectl"]

        if self.context:
            cmd.extend(["--context", self.context])

        cmd.extend(sanitized_args)

        if self.namespace and "--all-namespaces" not in sanitized_args and "-n" not in sanitized_args:
            cmd.extend(["-n", self.namespace])

        return cmd

    def execute(self, args: List[str]) -> Tuple[bool, str]:
        """Execute kubectl command and return success status and output.
        
        Args:
            args: Command arguments
            
        Returns:
            Tuple of (success: bool, output: str)
            
        Raises:
            KubernetesCommandError: If command execution fails with details
        """
        cmd = self._build_command(args)
        logger.debug(f"Executing: {' '.join(shlex.quote(arg) for arg in cmd)}")

        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True,
                timeout=300  # 5 minute timeout for safety
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with exit code {e.returncode}: {e.stderr}")
            raise KubernetesCommandError(
                f"kubectl command failed: {' '.join(cmd)}",
                cmd,
                e.returncode,
                e.stderr or ""
            ) from e
        except subprocess.TimeoutExpired as e:
            logger.error(f"Command timed out: {' '.join(cmd)}")
            raise KubernetesCommandError(
                f"kubectl command timed out: {' '.join(cmd)}",
                cmd,
                -1,
                "Command execution timed out"
            ) from e

    def get_json(
        self, resource_type: str, name: Optional[str] = None, all_namespaces: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Get resource(s) as JSON.
        
        Args:
            resource_type: Type of Kubernetes resource to get
            name: Optional specific resource name
            all_namespaces: Whether to search all namespaces
            
        Returns:
            Resource data as dictionary or None if not found
            
        Raises:
            ValidationError: If parameters are invalid
            ResourceNotFoundError: If specific resource is not found
        """
        # Sanitize inputs
        sanitized_resource_type = self._sanitize_resource_type(resource_type)
        sanitized_name = None
        if name:
            sanitized_name = self._sanitize_identifier(name)
            
        args = ["get", sanitized_resource_type]

        if sanitized_name:
            args.append(sanitized_name)

        if all_namespaces:
            args.append("--all-namespaces")

        args.extend(["-o", "json"])

        try:
            success, output = self.execute(args)
            if success:
                try:
                    result = json.loads(output)
                    return result if isinstance(result, dict) else None
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON output: {e}")
                    raise KubernetesCommandError(
                        "Failed to parse kubectl JSON output",
                        args,
                        0,
                        "Invalid JSON response"
                    ) from e
        except KubernetesCommandError as e:
            # Check if it's a "not found" error
            if "not found" in e.stderr.lower() and sanitized_name:
                raise ResourceNotFoundError(
                    sanitized_resource_type,
                    sanitized_name,
                    self.namespace
                ) from e
            # Re-raise other kubectl errors
            raise
        return None

    def get_version(self) -> Optional[Dict[str, Any]]:
        """Get cluster version information.
        
        Returns:
            Version information as dictionary or None if unavailable
            
        Raises:
            KubernetesCommandError: If version command fails
        """
        try:
            success, output = self.execute(["version", "-o", "json"])
            if success:
                try:
                    result = json.loads(output)
                    return result if isinstance(result, dict) else None
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse version JSON: {e}")
                    raise KubernetesCommandError(
                        "Failed to parse kubectl version JSON output",
                        ["version", "-o", "json"],
                        0,
                        "Invalid JSON response"
                    ) from e
        except KubernetesCommandError:
            # Version command failures are common, just return None
            logger.warning("Could not retrieve cluster version information")
            return None
        return None

    def get_api_resources(self) -> List[Dict[str, Any]]:
        """Get available API resources.
        
        Returns:
            List of available API resources with their metadata
            
        Raises:
            KubernetesCommandError: If api-resources command fails
        """
        try:
            success, output = self.execute(["api-resources", "--verbs=list"])
            if success:
                # Parse the table format output
                resources = []
                lines = output.strip().split("\n")[1:]  # Skip header

                for line in lines:
                    if not line.strip():
                        continue
                    parts = line.split()
                    if len(parts) >= 4:
                        name = parts[0]
                        api_version = parts[-3] if len(parts) >= 4 else "v1"
                        namespaced = parts[-2].lower() == "true" if len(parts) >= 4 else False
                        kind = parts[-1] if len(parts) >= 4 else name.title()

                        resources.append(
                            {
                                "name": name,
                                "kind": kind,
                                "namespaced": namespaced,
                                "apiVersion": api_version,
                            }
                        )
                return resources
        except KubernetesCommandError as e:
            logger.warning(f"Could not retrieve API resources: {e}")
            return []
        return []

    def get_cluster_info(self) -> ClusterInfo:
        """Get cluster version and node information.
        
        Returns:
            ClusterInfo object with version and node data
            
        Raises:
            KubernetesConnectionError: If cluster is not accessible
        """
        cluster_info = ClusterInfo()

        try:
            # Get version
            version_data = self.get_version()
            if version_data:
                if "serverVersion" in version_data:
                    cluster_info.server_version = ClusterVersion(**version_data["serverVersion"])
                if "clientVersion" in version_data:
                    cluster_info.client_version = ClusterVersion(**version_data["clientVersion"])

            # Get nodes
            nodes_data = self.get_json("nodes")
            if nodes_data and "items" in nodes_data:
                for node in nodes_data["items"]:
                    node_info = self._parse_node_info(node)
                    if node_info:
                        cluster_info.nodes.append(node_info)

        except KubernetesCommandError as e:
            raise KubernetesConnectionError(
                f"Failed to retrieve cluster information: {e.message}",
                self.context
            ) from e
        except ResourceNotFoundError:
            # Nodes not accessible, cluster might have permission issues
            logger.warning("Could not access node information")

        return cluster_info

    def _parse_node_info(self, node_data: Dict[str, Any]) -> Optional[NodeInfo]:
        """Parse node information."""
        try:
            metadata = node_data["metadata"]
            status = node_data["status"]

            # Get roles from labels
            roles = []
            labels = metadata.get("labels", {})
            if "node-role.kubernetes.io/control-plane" in labels:
                roles.append("control-plane")
            if "node-role.kubernetes.io/master" in labels:
                roles.append("master")
            if "node-role.kubernetes.io/worker" in labels:
                roles.append("worker")
            if not roles:
                roles.append("worker")  # Default to worker if no role labels

            # Get node status
            node_status = "Unknown"
            for condition in status.get("conditions", []):
                if condition["type"] == "Ready":
                    node_status = "Ready" if condition["status"] == "True" else "NotReady"
                    break

            # Get node info
            node_info_data = status.get("nodeInfo", {})

            return NodeInfo(
                name=metadata["name"],
                status=node_status,
                roles=",".join(roles),
                version=node_info_data.get("kubeletVersion", "unknown"),
                os=f"{node_info_data.get('osImage', 'unknown')} ({node_info_data.get('architecture', 'unknown')})",
                container_runtime=node_info_data.get("containerRuntimeVersion", "unknown"),
            )
        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse node info: {e}")
            return None
