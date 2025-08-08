"""Database models for historical tracking without ORM dependencies."""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ScanRecord(BaseModel):
    """Represents a complete cluster scan."""

    id: int | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    cluster_context: Optional[str] = None
    namespace: Optional[str] = None
    scan_type: str = "full"  # full, incremental, etc.
    total_resources: int = 0

    # Cluster info at time of scan
    cluster_version: Optional[str] = None
    node_count: Optional[int] = None
    cluster_info: Optional[Dict[str, Any]] = None


class ResourceChange(BaseModel):
    """Tracks changes between resource versions."""

    id: int | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Resource identification
    kind: str
    namespace: Optional[str] = None
    name: str

    # Change information
    change_type: str  # created, updated, deleted
    old_scan_id: Optional[int] = None
    new_scan_id: Optional[int] = None

    # Change details
    changed_fields: Optional[Dict[str, Any]] = None
    diff_summary: Optional[str] = None


# Pydantic models for API responses
class ScanRecordResponse(ScanRecord):
    """Response model for scan records."""

    class Config:
        from_attributes = True


class ResourceChangeResponse(ResourceChange):
    """Response model for resource changes."""

    class Config:
        from_attributes = True


class HistoricalSummary(BaseModel):
    """Summary of historical data."""

    total_scans: int
    date_range: tuple[datetime, datetime]
    most_active_namespaces: Dict[str, int] = Field(default_factory=dict)
    most_changed_resources: Dict[str, int] = Field(default_factory=dict)
    cluster_versions: Dict[str, int] = Field(default_factory=dict)
