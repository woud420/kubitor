"""Kubernetes version upgrade information."""

from typing import Dict, List

# Comprehensive upgrade information for Kubernetes versions 1.25 to 1.34
KUBERNETES_VERSIONS: Dict[str, Dict[str, List[str]]] = {
    "1.25": {
        "notes": [
            "PodDisruptionBudget eviction support in the eviction API is GA",
            "Ephemeral containers are stable",
            "CSI migration for AWS EBS and GCE PD is GA",
            "cgroups v2 support is GA",
            "Network Policy endPort field is stable",
        ],
        "deprecations": [
            "PodSecurityPolicy is removed (deprecated since 1.21)",
            "Pod spec.serviceAccount field is removed (use serviceAccountName)",
            "kube-controller-manager --service-account-api-audiences is removed",
        ],
        "actions": [
            "Migrate from PodSecurityPolicy to Pod Security Standards",
            "Update pod specs to use serviceAccountName instead of serviceAccount",
            "Update controller manager configuration for service account audiences",
        ],
    },
    "1.26": {
        "notes": [
            "CEL for admission control is alpha",
            "Job PodFailurePolicy is beta",
            "Dynamic Resource Allocation is alpha",
            "kubectl events command is alpha",
            "Non-graceful node shutdown is GA",
        ],
        "deprecations": [
            "In-tree Azure Disk and OpenStack Cinder volume plugins removed",
            "CephFS volume plugin deprecated",
            "FlexVolume deprecated in favor of CSI",
            "kube-proxy userspace mode removed",
        ],
        "actions": [
            "Migrate to CSI drivers for Azure Disk and OpenStack Cinder",
            "Plan migration from CephFS to CSI driver",
            "Replace FlexVolume with CSI drivers",
            "Ensure kube-proxy is using iptables or ipvs mode",
        ],
    },
    "1.27": {
        "notes": [
            "SeccompDefault feature is GA",
            "Job tracking without lingering Pods is stable",
            "kubectl apply prune is alpha",
            "Node log access via kubectl is alpha",
            "StatefulSet start ordinal is beta",
        ],
        "deprecations": [
            "k8s.gcr.io registry is frozen (use registry.k8s.io)",
            "CSIMigration for RBD and Portworx in-tree plugins deprecated",
            "SecurityContextDeny admission plugin deprecated",
        ],
        "actions": [
            "Update all image references from k8s.gcr.io to registry.k8s.io",
            "Plan migration to CSI drivers for RBD and Portworx",
            "Remove SecurityContextDeny admission plugin usage",
        ],
    },
    "1.28": {
        "notes": [
            "CSI migration for in-tree storage plugins is GA",
            "Mixed version proxy is alpha",
            "Retroactive default StorageClass assignment is stable",
            "Non-graceful node shutdown is GA for stateful workloads",
            "Sidecar containers are alpha",
        ],
        "deprecations": [
            "In-tree GlusterFS volume plugin removed",
            "CephFS volume plugin removed",
            "All alpha API versions for stable resources deprecated",
        ],
        "actions": [
            "Complete migration to CSI drivers for all storage",
            "Remove any usage of GlusterFS or CephFS in-tree plugins",
            "Update manifests to use stable API versions",
        ],
    },
    "1.29": {
        "notes": [
            "ReadWriteOncePod PersistentVolume access mode is GA",
            "Node volume limits for CSI drivers is GA",
            "KMS v2 encryption at rest is stable",
            "Structured Authentication Configuration is beta",
            "nftables kube-proxy backend is alpha",
        ],
        "deprecations": [
            "In-tree cloud provider code frozen",
            "flowcontrol.apiserver.k8s.io/v1beta2 deprecated",
            "node.k8s.io/v1beta1 deprecated",
        ],
        "actions": [
            "Migrate to external cloud providers",
            "Update to flowcontrol.apiserver.k8s.io/v1",
            "Update to node.k8s.io/v1",
        ],
    },
    "1.30": {
        "notes": [
            "ContextualLogging is beta",
            "AppArmor support is GA",
            "Pod Scheduling Readiness is stable",
            "Min domains in PodTopologySpread is stable",
            "Structured authentication configuration is beta",
        ],
        "deprecations": [
            "flowcontrol.apiserver.k8s.io/v1beta3 deprecated",
            "Multiple deprecated feature gates removed",
            "Legacy service account token cleaning GA",
        ],
        "actions": [
            "Update to flowcontrol.apiserver.k8s.io/v1",
            "Remove usage of deprecated feature gates",
            "Ensure applications handle service account token rotation",
        ],
    },
    "1.31": {
        "notes": [
            "PersistentVolume last phase transition time is beta",
            "Persistent volume recursive ownership change is GA",
            "AppArmor fields are GA",
            "Traffic distribution for services is alpha",
            "cgroups v1 support is deprecated",
        ],
        "deprecations": [
            "cgroups v1 is deprecated (removal targeted for 1.35)",
            "In-tree cloud providers fully deprecated",
            "Several beta APIs moving to GA with deprecation of beta versions",
        ],
        "actions": [
            "Plan migration to cgroups v2",
            "Complete migration to external cloud providers",
            "Update to GA API versions for beta APIs",
        ],
    },
    "1.32": {
        "notes": [
            "CEL for CRD validation is GA",
            "Pod lifecycle sleep action is GA",
            "Job managed-by field is alpha",
            "Recursive Read-only mounts is GA",
            "Node memory swap support is beta",
        ],
        "deprecations": [
            "Legacy kube-proxy configuration deprecated",
            "Multiple feature gates removed after GA graduation",
            "resource.k8s.io/v1alpha1 deprecated",
        ],
        "actions": [
            "Update kube-proxy configuration to v1alpha1 or newer",
            "Remove usage of graduated feature gates",
            "Migrate from resource.k8s.io/v1alpha1 to v1alpha2",
        ],
    },
    "1.33": {
        "notes": [
            "Gateway API support is beta",
            "Coordinated leader election is alpha",
            "RecoverVolumeExpansionFailure is alpha",
            "Job success/completion policy is beta",
            "Device plugin CDI support is beta",
        ],
        "deprecations": [
            "coordination.k8s.io/v1beta1 Lease deprecated",
            "extensions/v1beta1 Ingress removed",
            "Dockershim removal completed (removed in 1.24)",
        ],
        "actions": [
            "Update to coordination.k8s.io/v1 for Leases",
            "Ensure all Ingress resources use networking.k8s.io/v1",
            "Ensure container runtime is containerd or CRI-O",
        ],
    },
    "1.34": {
        "notes": [
            "Custom profiling in kubectl debug is alpha",
            "Component SLIs is GA",
            "LoadBalancer IPMode is GA",
            "Storage version migration is alpha",
            "Bound service account tokens is GA",
        ],
        "deprecations": [
            "v1beta1 CRD version support removed",
            "Legacy field selectors in various resources deprecated",
            "Multiple alpha API versions removed",
        ],
        "actions": [
            "Update all CRDs to use v1 version",
            "Update field selectors to use supported fields",
            "Remove usage of removed alpha APIs",
        ],
    },
}
