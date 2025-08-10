"""Kubernetes client wrapper."""

import subprocess
import json
from typing import List, Tuple, Optional, Dict, Any

from ..utils.logger import get_logger
from ..model.cluster import ClusterInfo, ClusterVersion, NodeInfo

logger = get_logger(__name__)


class K8sClient:
    """Wrapper for kubectl commands."""

    def __init__(self, context: Optional[str] = None, namespace: Optional[str] = None):
        self.context = context
        self.namespace = namespace
        self._verify_kubectl()

    def _verify_kubectl(self):
        """Verify kubectl is available and configured."""
        try:
            result = subprocess.run(
                ["kubectl", "version", "--client", "-o", "json"],
                capture_output=True,
                text=True,
                check=True,
            )
            logger.debug("kubectl verified successfully")
        except subprocess.CalledProcessError:
            raise RuntimeError("kubectl is not installed or not in PATH")
        except FileNotFoundError:
            raise RuntimeError("kubectl command not found. Please install kubectl.")

    def _build_command(self, args: List[str]) -> List[str]:
        """Build kubectl command with context and namespace."""
        cmd = ["kubectl"]

        if self.context:
            cmd.extend(["--context", self.context])

        cmd.extend(args)

        if self.namespace and "--all-namespaces" not in args and "-n" not in args:
            cmd.extend(["-n", self.namespace])

        return cmd

    def execute(self, args: List[str]) -> Tuple[bool, str]:
        """Execute kubectl command and return success status and output."""
        cmd = self._build_command(args)
        logger.debug(f"Executing: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.stderr}")
            return False, e.stderr

    def get_json(
        self, resource_type: str, name: Optional[str] = None, all_namespaces: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Get resource(s) as JSON."""
        args = ["get", resource_type]

        if name:
            args.append(name)

        if all_namespaces:
            args.append("--all-namespaces")

        args.extend(["-o", "json"])

        success, output = self.execute(args)
        if success:
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                logger.error("Failed to parse JSON output")
                return None
        return None

    def get_version(self) -> Optional[Dict[str, Any]]:
        """Get cluster version information."""
        success, output = self.execute(["version", "-o", "json"])
        if success:
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                return None
        return None

    def get_api_resources(self) -> List[Dict[str, Any]]:
        """Get available API resources."""
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
        return []

    def get_cluster_info(self) -> ClusterInfo:
        """Get cluster version and node information."""
        cluster_info = ClusterInfo()

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
