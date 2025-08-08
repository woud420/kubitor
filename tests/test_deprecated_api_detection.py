import yaml
from src.upgrade.advisor import UpgradeAdvisor
from src.model.kubernetes import K8sResource

sample_manifest = """
apiVersion: apps/v1beta1
kind: Deployment
metadata:
  name: old-deployment
  namespace: default
---
apiVersion: extensions/v1beta1
kind: Ingress
metadata:
  name: old-ingress
  namespace: default
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: new-deployment
  namespace: default
"""

def test_deprecated_api_detection():
    resources = []
    for doc in yaml.safe_load_all(sample_manifest):
        doc["api_version"] = doc.pop("apiVersion")
        resources.append(K8sResource(**doc))
    advisor = UpgradeAdvisor()
    actions = advisor.get_deprecated_resources(resources)
    assert len(actions) == 2
    mapping = {(a.kind, a.name): a.suggested_version for a in actions}
    assert mapping[("Deployment", "old-deployment")] == "apps/v1"
    assert mapping[("Ingress", "old-ingress")] == "networking.k8s.io/v1"
