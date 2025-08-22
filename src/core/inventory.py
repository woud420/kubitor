"""Cluster inventory collector."""

from typing import List, Dict, Any, Optional
from collections import defaultdict
from datetime import datetime

from ..k8s import K8sClient
from ..model.inventory import Application, ClusterInventory
from ..model.management import ManagementTool
from ..utils.logger import get_logger
from .management_detector import ManagementDetector

logger = get_logger(__name__)


class InventoryCollector:
    """Collects cluster inventory information."""
    
    # System namespaces to exclude by default
    SYSTEM_NAMESPACES = {
        'kube-system', 'kube-public', 'kube-node-lease', 
        'eks-auth', 'eks-managed-nodes', 'eks-managed-addons',
        'aks-system', 'gke-system', 'gke-managed-system',
        'openshift-system', 'openshift-operators',
        'cert-manager', 'ingress-nginx', 'nginx-ingress',
        'istio-system', 'linkerd', 'calico-system',
        'prometheus-system', 'monitoring', 'logging',
        'elastic-system', 'datadog', 'newrelic',
        'velero', 'flux-system', 'argocd'
    }
    
    # System prefixes to check
    SYSTEM_PREFIXES = [
        'kube-', 'eks-', 'aks-', 'gke-', 'openshift-',
        'istio-', 'linkerd-', 'calico-', 'cilium-',
        'prometheus-', 'grafana-', 'elastic-', 'fluentd-'
    ]
    
    def __init__(self, client: K8sClient, include_system: bool = False):
        self.client = client
        self.include_system = include_system
        self.detector = ManagementDetector()
    
    def collect(self) -> ClusterInventory:
        """Collect complete cluster inventory."""
        logger.info("Collecting cluster inventory")
        
        # Get cluster info
        cluster_name = self._get_cluster_name()
        k8s_version = self._get_kubernetes_version()
        node_count = self._get_node_count()
        
        # Create inventory
        inventory = ClusterInventory(
            cluster_name=cluster_name,
            kubernetes_version=k8s_version,
            node_count=node_count,
            scan_timestamp=datetime.now()
        )
        
        # Collect applications
        applications = self._collect_applications()
        
        # Filter system resources unless requested
        if not self.include_system:
            applications = [app for app in applications if not self._is_system_resource(app)]
        
        inventory.applications = applications
        
        logger.info(f"Collected inventory: {inventory.total_applications} applications")
        return inventory
    
    def _get_cluster_name(self) -> str:
        """Get cluster name from context or nodes."""
        # Try to get from current context
        context = self.client.context
        if context:
            return context
        
        # Try to get from node labels
        try:
            nodes = self.client.get_json("nodes")
            if nodes and nodes.get("items"):
                node = nodes["items"][0]
                labels = node.get("metadata", {}).get("labels", {})
                
                # Cloud provider cluster name labels
                cluster_name_labels = {
                    "eks.amazonaws.com/cluster-name": lambda l: l.get("eks.amazonaws.com/cluster-name"),
                    "kubernetes.azure.com/cluster": lambda l: l.get("kubernetes.azure.com/cluster"),
                    "cloud.google.com/gke-cluster-name": lambda l: l.get("cloud.google.com/gke-cluster-name"),
                }
                
                for label_key, extractor in cluster_name_labels.items():
                    if label_key in labels:
                        result = extractor(labels)
                        return str(result) if result is not None else "unknown"
        except Exception as e:
            logger.debug(f"Could not get cluster name from nodes: {e}")
        
        return "unknown"
    
    def _get_kubernetes_version(self) -> str:
        """Get Kubernetes server version."""
        try:
            version_info = self.client.get_version()
            if version_info and "serverVersion" in version_info:
                git_version = version_info["serverVersion"]["gitVersion"]
                return str(git_version) if git_version is not None else "unknown"
        except Exception as e:
            logger.error(f"Could not get Kubernetes version: {e}")
        
        return "unknown"
    
    def _get_node_count(self) -> int:
        """Get number of nodes in the cluster."""
        try:
            nodes = self.client.get_json("nodes")
            if nodes and "items" in nodes:
                return len(nodes["items"])
        except Exception as e:
            logger.error(f"Could not get node count: {e}")
        
        return 0
    
    def _collect_applications(self) -> List[Application]:
        """Collect all applications from the cluster."""
        applications = []
        
        # Collect Deployments
        deployments = self._get_resources("deployments", all_namespaces=True)
        for dep in deployments:
            app = self._create_application(dep, "Deployment")
            if app:
                applications.append(app)
        
        # Collect StatefulSets
        statefulsets = self._get_resources("statefulsets", all_namespaces=True)
        for sts in statefulsets:
            app = self._create_application(sts, "StatefulSet")
            if app:
                applications.append(app)
        
        # Collect DaemonSets
        daemonsets = self._get_resources("daemonsets", all_namespaces=True)
        for ds in daemonsets:
            app = self._create_application(ds, "DaemonSet")
            if app:
                applications.append(app)
        
        # Collect standalone pods (not managed by controllers)
        pods = self._get_resources("pods", all_namespaces=True)
        for pod in pods:
            if self._is_standalone_pod(pod):
                app = self._create_application(pod, "Pod")
                if app:
                    applications.append(app)
        
        # Collect Jobs (only active ones)
        jobs = self._get_resources("jobs", all_namespaces=True)
        for job in jobs:
            if self._is_active_job(job):
                app = self._create_application(job, "Job")
                if app:
                    applications.append(app)
        
        # Collect CronJobs
        cronjobs = self._get_resources("cronjobs", all_namespaces=True)
        for cj in cronjobs:
            app = self._create_application(cj, "CronJob")
            if app:
                applications.append(app)
        
        return applications
    
    def _get_resources(self, resource_type: str, all_namespaces: bool = False) -> List[Dict]:
        """Get resources from Kubernetes API."""
        try:
            result = self.client.get_json(resource_type, all_namespaces=all_namespaces)
            if result and "items" in result:
                items = result["items"]
                return list(items) if isinstance(items, list) else []
        except Exception as e:
            logger.debug(f"Could not get {resource_type}: {e}")
        
        return []
    
    def _create_application(self, resource: Dict[str, Any], kind: str) -> Optional[Application]:
        """Create Application object from Kubernetes resource."""
        try:
            metadata = resource.get("metadata", {})
            spec = resource.get("spec", {})
            status = resource.get("status", {})
            
            # Basic info
            name = metadata.get("name")
            namespace = metadata.get("namespace", "default")
            labels = metadata.get("labels", {})
            
            if not name:
                return None
            
            # Get version from labels
            version = (
                labels.get("app.kubernetes.io/version") or
                labels.get("version") or
                labels.get("app.version")
            )
            
            # Detect management
            management = self.detector.detect(resource)
            
            # Replica count extractors by kind
            replica_extractors = {
                "Deployment": lambda: status.get("readyReplicas", 0),
                "StatefulSet": lambda: status.get("readyReplicas", 0),
                "DaemonSet": lambda: status.get("numberReady", 0),
                "Pod": lambda: 1 if status.get("phase") == "Running" else 0,
            }
            
            # Get replica count
            replicas = replica_extractors.get(kind, lambda: 0)()
            
            # Pod spec extractors by kind
            pod_spec_extractors = {
                "Deployment": lambda: spec.get("template", {}).get("spec", {}),
                "StatefulSet": lambda: spec.get("template", {}).get("spec", {}),
                "DaemonSet": lambda: spec.get("template", {}).get("spec", {}),
                "Pod": lambda: spec,
                "Job": lambda: spec,
                "CronJob": lambda: spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec", {}),
            }
            
            # Get container info
            containers = []
            images = []
            pod_spec = pod_spec_extractors.get(kind, lambda: {})()
            
            for container in pod_spec.get("containers", []):
                containers.append(container.get("name", "unknown"))
                images.append(container.get("image", "unknown"))
            
            return Application(
                name=name,
                namespace=namespace,
                kind=kind,
                version=version,
                management=management,
                replicas=replicas,
                containers=containers,
                images=images,
                labels=labels
            )
            
        except Exception as e:
            logger.debug(f"Could not create application from resource: {e}")
            return None
    
    def _is_standalone_pod(self, pod: Dict[str, Any]) -> bool:
        """Check if pod is standalone (not managed by a controller)."""
        metadata = pod.get("metadata", {})
        owner_refs = metadata.get("ownerReferences", [])
        
        # If no owner references, it's standalone
        if not owner_refs:
            return True
        
        # Check if owned by a Job (jobs create standalone-ish pods)
        for ref in owner_refs:
            if ref.get("kind") in ["ReplicaSet", "StatefulSet", "DaemonSet", "Job", "CronJob"]:
                return False
        
        return True
    
    def _is_active_job(self, job: Dict[str, Any]) -> bool:
        """Check if job is currently active."""
        status = job.get("status", {})
        return status.get("active", 0) > 0 or not status.get("completionTime")
    
    def _is_system_resource(self, app: Application) -> bool:
        """Check if application is a system resource."""
        # Check if in system namespace
        if app.namespace in self.SYSTEM_NAMESPACES:
            return True
        
        # Check namespace prefixes
        for prefix in self.SYSTEM_PREFIXES:
            if app.namespace.startswith(prefix):
                return True
        
        # Check common system app names
        system_apps = {
            'coredns', 'kube-proxy', 'aws-node', 'azure-cni',
            'calico-node', 'cilium-agent', 'flannel', 'weave-net',
            'metrics-server', 'cluster-autoscaler', 'dns-autoscaler',
            'vertical-pod-autoscaler', 'node-problem-detector'
        }
        
        if app.name.lower() in system_apps:
            return True
        
        # Check if managed by system operators
        if app.management and app.management.tool == ManagementTool.OPERATOR:
            operator_name = app.management.operator_name or ""
            if any(prefix in operator_name.lower() for prefix in ['kube-', 'eks-', 'aks-', 'gke-']):
                return True
        
        return False