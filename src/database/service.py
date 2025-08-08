"""High-level database service utilizing simple DAO classes."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .connection import DatabaseConnection
from ..model.cluster import ClusterInfo
from ..model.kubernetes import K8sResource
from ..model.database import ScanRecordResponse, ResourceChangeResponse, HistoricalSummary
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ScanDAO:
    """Data access object for scan_records table."""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def create(
        self,
        cluster_context: Optional[str],
        namespace: Optional[str],
        total_resources: int,
        cluster_version: Optional[str],
        node_count: int,
        cluster_info: Optional[Dict[str, Any]],
    ) -> int:
        data = {
            "cluster_context": cluster_context,
            "namespace": namespace,
            "scan_type": "full",
            "total_resources": total_resources,
            "cluster_version": cluster_version,
            "node_count": node_count,
            "cluster_info": json.dumps(cluster_info) if cluster_info else None,
        }
        return self.db.insert_returning_id("scan_records", data)

    def get_previous(self, scan_id: int) -> Optional[Dict[str, Any]]:
        return self.db.fetch_one(
            "SELECT * FROM scan_records WHERE id < :id ORDER BY id DESC LIMIT 1",
            {"id": scan_id},
        )

    def get_recent(self, limit: int) -> List[Dict[str, Any]]:
        return self.db.fetch_all(
            f"SELECT * FROM scan_records ORDER BY timestamp DESC LIMIT {limit}"
        )

    def get_in_range(
        self, start: datetime, end: datetime, context: Optional[str]
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM scan_records WHERE timestamp >= :start AND timestamp <= :end"
        params: Dict[str, Any] = {"start": start, "end": end}
        if context is not None:
            query += " AND cluster_context = :ctx"
            params["ctx"] = context
        return self.db.fetch_all(query + " ORDER BY timestamp DESC", params)

    def cleanup_old(self, keep_days: int) -> int:
        cutoff = datetime.utcnow() - timedelta(days=keep_days)
        return self.db.execute(
            "DELETE FROM scan_records WHERE timestamp < :cutoff", {"cutoff": cutoff}
        )


class ResourceDAO:
    """Data access object for resource_records table."""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def bulk_create(self, resources: List[Dict[str, Any]]) -> int:
        return self.db.bulk_insert("resource_records", resources)

    def get_by_scan(self, scan_id: int) -> List[Dict[str, Any]]:
        return self.db.fetch_all(
            "SELECT * FROM resource_records WHERE scan_id = :id", {"id": scan_id}
        )

    def history(
        self, kind: str, name: str, namespace: Optional[str], limit: int
    ) -> List[Dict[str, Any]]:
        query = (
            "SELECT r.*, s.timestamp FROM resource_records r "
            "JOIN scan_records s ON r.scan_id = s.id "
            "WHERE r.kind = :kind AND r.name = :name"
        )
        params: Dict[str, Any] = {"kind": kind, "name": name}
        if namespace is not None:
            query += " AND r.namespace = :ns"
            params["ns"] = namespace
        else:
            query += " AND r.namespace IS NULL"
        query += " ORDER BY s.timestamp DESC LIMIT :limit"
        params["limit"] = limit
        return self.db.fetch_all(query, params)


class ChangeDAO:
    """Data access object for resource_changes table."""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def create(
        self,
        kind: str,
        namespace: Optional[str],
        name: str,
        change_type: str,
        old_scan_id: int,
        new_scan_id: int,
    ) -> int:
        data = {
            "kind": kind,
            "namespace": namespace,
            "name": name,
            "change_type": change_type,
            "old_scan_id": old_scan_id,
            "new_scan_id": new_scan_id,
        }
        return self.db.insert_returning_id("resource_changes", data)

    def find(self, change_id: int) -> Dict[str, Any]:
        return self.db.fetch_one(
            "SELECT * FROM resource_changes WHERE id = :id", {"id": change_id}
        )


class DatabaseService:
    """Facade providing high-level operations using DAOs."""

    def __init__(self, database_url: Optional[str] = None):
        self.db = DatabaseConnection(database_url)
        self.scan_dao = ScanDAO(self.db)
        self.resource_dao = ResourceDAO(self.db)
        self.change_dao = ChangeDAO(self.db)

    def store_scan(
        self,
        resources: List[K8sResource],
        cluster_info: ClusterInfo,
        context: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> int:
        cluster_version = (
            cluster_info.server_version.git_version
            if cluster_info and cluster_info.server_version
            else None
        )
        node_count = len(cluster_info.nodes) if cluster_info else 0

        scan_id = self.scan_dao.create(
            cluster_context=context,
            namespace=namespace,
            total_resources=len(resources),
            cluster_version=cluster_version,
            node_count=node_count,
            cluster_info=cluster_info.model_dump() if cluster_info else None,
        )

        resource_records = []
        for resource in resources:
            resource_dict = resource.model_dump()
            resource_hash = self._calculate_resource_hash(resource_dict)
            record = {
                "scan_id": scan_id,
                "api_version": resource.api_version,
                "kind": resource.kind,
                "namespace": resource.namespace,
                "name": resource.name,
                "resource_data": json.dumps(resource_dict),
                "resource_hash": resource_hash,
                "is_helm_managed": resource.is_helm_managed,
                "helm_release": resource.labels.get("helm.sh/release"),
                "labels": json.dumps(resource.labels) if resource.labels else None,
                "annotations": json.dumps(resource.annotations)
                if resource.annotations
                else None,
            }
            resource_records.append(record)

        if resource_records:
            self.resource_dao.bulk_create(resource_records)

        return scan_id

    def detect_changes(
        self, new_scan_id: int, old_scan_id: Optional[int] = None
    ) -> List[ResourceChangeResponse]:
        if old_scan_id is None:
            previous = self.scan_dao.get_previous(new_scan_id)
            if not previous:
                return []
            old_scan_id = previous["id"]

        current_resources = self.resource_dao.get_by_scan(new_scan_id)
        previous_resources = self.resource_dao.get_by_scan(old_scan_id)

        def key(res: Dict[str, Any]) -> str:
            return f"{res['api_version']}/{res['kind']}/{res.get('namespace') or ''}/{res['name']}"

        current_map = {key(r): r for r in current_resources}
        previous_map = {key(r): r for r in previous_resources}

        changes: List[ResourceChangeResponse] = []

        for k, res in current_map.items():
            if k not in previous_map:
                change_id = self.change_dao.create(
                    res["kind"], res.get("namespace"), res["name"], "created", old_scan_id, new_scan_id
                )
                changes.append(ResourceChangeResponse(**self.change_dao.find(change_id)))

        for k, res in previous_map.items():
            if k not in current_map:
                change_id = self.change_dao.create(
                    res["kind"], res.get("namespace"), res["name"], "deleted", old_scan_id, new_scan_id
                )
                changes.append(ResourceChangeResponse(**self.change_dao.find(change_id)))

        for k in current_map.keys() & previous_map.keys():
            cur = current_map[k]
            prev = previous_map[k]
            if cur.get("resource_hash") != prev.get("resource_hash"):
                change_id = self.change_dao.create(
                    cur["kind"], cur.get("namespace"), cur["name"], "updated", old_scan_id, new_scan_id
                )
                changes.append(ResourceChangeResponse(**self.change_dao.find(change_id)))

        return changes

    def get_recent_scans(self, limit: int = 10) -> List[ScanRecordResponse]:
        rows = self.scan_dao.get_recent(limit)
        return [ScanRecordResponse(**r) for r in rows]

    def get_scans_in_range(
        self,
        start_date: datetime,
        end_date: datetime,
        cluster_context: Optional[str] = None,
    ) -> List[ScanRecordResponse]:
        rows = self.scan_dao.get_in_range(start_date, end_date, cluster_context)
        return [ScanRecordResponse(**r) for r in rows]

    def get_resource_history(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        return self.resource_dao.history(kind, name, namespace, limit)

    def get_historical_summary(self, days: int = 30) -> HistoricalSummary:
        cutoff = datetime.utcnow() - timedelta(days=days)
        stats = self.db.fetch_one(
            "SELECT COUNT(*) as count, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts "
            "FROM scan_records WHERE timestamp >= :cutoff",
            {"cutoff": cutoff},
        )
        total_scans = stats["count"] if stats else 0
        date_range = (
            stats["min_ts"] if stats and stats["min_ts"] else cutoff,
            stats["max_ts"] if stats and stats["max_ts"] else datetime.utcnow(),
        )

        namespace_rows = self.db.fetch_all(
            "SELECT r.namespace, COUNT(*) as count FROM resource_records r "
            "JOIN scan_records s ON r.scan_id = s.id "
            "WHERE s.timestamp >= :cutoff GROUP BY r.namespace",
            {"cutoff": cutoff},
        )
        namespaces = {
            (row["namespace"] or "cluster-scoped"): row["count"] for row in namespace_rows
        }

        resource_rows = self.db.fetch_all(
            "SELECT r.kind, r.name, COUNT(DISTINCT r.resource_hash) as change_count "
            "FROM resource_records r JOIN scan_records s ON r.scan_id = s.id "
            "WHERE s.timestamp >= :cutoff GROUP BY r.kind, r.name ORDER BY change_count DESC",
            {"cutoff": cutoff},
        )
        resources = {
            f"{row['kind']}/{row['name']}": row["change_count"] for row in resource_rows
        }

        version_rows = self.db.fetch_all(
            "SELECT cluster_version, COUNT(*) as count FROM scan_records WHERE timestamp >= :cutoff GROUP BY cluster_version",
            {"cutoff": cutoff},
        )
        versions = {row["cluster_version"]: row["count"] for row in version_rows}

        return HistoricalSummary(
            total_scans=total_scans,
            date_range=date_range,
            most_active_namespaces=namespaces,
            most_changed_resources=resources,
            cluster_versions=versions,
        )

    def cleanup_old_scans(self, keep_days: int = 90) -> int:
        return self.scan_dao.cleanup_old(keep_days)

    def close(self) -> None:
        self.db.close()

    def _calculate_resource_hash(self, resource_data: Dict[str, Any]) -> str:
        filtered = dict(resource_data)
        if "metadata" in filtered:
            metadata = dict(filtered["metadata"])
            metadata.pop("resourceVersion", None)
            metadata.pop("generation", None)
            metadata.pop("managedFields", None)
            metadata.pop("creationTimestamp", None)
            filtered["metadata"] = metadata
        filtered.pop("status", None)
        resource_json = json.dumps(filtered, sort_keys=True)
        return hashlib.sha256(resource_json.encode()).hexdigest()
