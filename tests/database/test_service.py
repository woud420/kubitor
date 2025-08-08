"""Tests for DatabaseService wrapper."""

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.database import DatabaseService
from src.model.kubernetes import K8sResource
from src.model.cluster import ClusterInfo, ClusterVersion


@pytest.fixture()
def service(tmp_path):
    db_url = f"sqlite:///{tmp_path/'test.db'}"
    svc = DatabaseService(db_url)
    yield svc
    svc.close()


def make_resource(value: str) -> K8sResource:
    return K8sResource(
        api_version="v1",
        kind="ConfigMap",
        metadata={"name": "cfg", "namespace": "default", "labels": {"v": value}},
        data={"key": value},
    )


def make_cluster() -> ClusterInfo:
    return ClusterInfo(
        server_version=ClusterVersion(major="1", minor="28", git_version="v1.28.0"),
        nodes=[],
    )


def test_store_and_recent_scans(service):
    cluster = make_cluster()
    resource = make_resource("1")
    scan_id = service.store_scan([resource], cluster, context="ctx")
    scans = service.get_recent_scans()
    assert scans and scans[0].id == scan_id


def test_detect_changes(service):
    cluster = make_cluster()
    res1 = make_resource("1")
    scan1 = service.store_scan([res1], cluster)
    res2 = make_resource("2")
    scan2 = service.store_scan([res2], cluster)
    changes = service.detect_changes(scan2)
    assert len(changes) == 1
    assert changes[0].change_type == "updated"


def test_resource_history(service):
    cluster = make_cluster()
    service.store_scan([make_resource("1")], cluster)
    service.store_scan([make_resource("2")], cluster)
    history = service.get_resource_history("ConfigMap", "cfg", "default")
    assert len(history) == 2


def test_historical_summary(service):
    cluster = make_cluster()
    service.store_scan([make_resource("1")], cluster)
    service.store_scan([make_resource("2")], cluster)
    summary = service.get_historical_summary(7)
    assert summary.total_scans == 2
    assert "v1.28.0" in summary.cluster_versions
