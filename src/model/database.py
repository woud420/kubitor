"""Database models for historical tracking."""

from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean,
    Text,
    JSON,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship
from pydantic import BaseModel, Field

Base = declarative_base()


class ScanRecord(Base):
    """Represents a complete cluster scan."""

    __tablename__ = "scan_records"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    cluster_context = Column(String(255))
    namespace = Column(String(255))
    scan_type = Column(String(50), default="full")  # full, incremental, etc.
    total_resources = Column(Integer, default=0)

    # Cluster info at time of scan
    cluster_version = Column(String(50))
    node_count = Column(Integer)
    cluster_info = Column(JSON)

    # Relationships
    resources = relationship("ResourceRecord", back_populates="scan")

    def __repr__(self):
        return f"<ScanRecord(id={self.id}, timestamp={self.timestamp}, resources={self.total_resources})>"


class ResourceRecord(Base):
    """Represents a Kubernetes resource at a point in time."""

    __tablename__ = "resource_records"

    id = Column(Integer, primary_key=True)
    scan_id = Column(Integer, ForeignKey("scan_records.id"), nullable=False)

    # Resource identification
    api_version = Column(String(100), nullable=False)
    kind = Column(String(100), nullable=False)
    namespace = Column(String(255))
    name = Column(String(255), nullable=False)

    # Resource content
    resource_data = Column(JSON, nullable=False)
    resource_hash = Column(String(64))  # For change detection

    # Metadata
    is_helm_managed = Column(Boolean, default=False)
    helm_release = Column(String(255))

    # Relationships
    scan = relationship("ScanRecord", back_populates="resources")

    def __repr__(self):
        return f"<ResourceRecord(kind={self.kind}, name={self.name}, namespace={self.namespace})>"


class ResourceChange(Base):
    """Tracks changes between resource versions."""

    __tablename__ = "resource_changes"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Resource identification
    kind = Column(String(100), nullable=False)
    namespace = Column(String(255))
    name = Column(String(255), nullable=False)

    # Change information
    change_type = Column(String(20), nullable=False)  # created, updated, deleted
    old_scan_id = Column(Integer, ForeignKey("scan_records.id"))
    new_scan_id = Column(Integer, ForeignKey("scan_records.id"))

    # Change details
    changed_fields = Column(JSON)  # List of field paths that changed
    diff_summary = Column(Text)  # Human-readable diff summary

    def __repr__(self):
        return f"<ResourceChange(kind={self.kind}, name={self.name}, type={self.change_type})>"


# Pydantic models for API responses
class ScanRecordResponse(BaseModel):
    """Response model for scan records."""

    id: int
    timestamp: datetime
    cluster_context: Optional[str]
    namespace: Optional[str]
    scan_type: str
    total_resources: int
    cluster_version: Optional[str]
    node_count: Optional[int]

    class Config:
        from_attributes = True


class ResourceChangeResponse(BaseModel):
    """Response model for resource changes."""

    id: int
    timestamp: datetime
    kind: str
    namespace: Optional[str]
    name: str
    change_type: str
    changed_fields: Optional[Dict[str, Any]]
    diff_summary: Optional[str]

    class Config:
        from_attributes = True


class HistoricalSummary(BaseModel):
    """Summary of historical data."""

    total_scans: int
    date_range: tuple[datetime, datetime]
    most_active_namespaces: Dict[str, int] = Field(default_factory=dict)
    most_changed_resources: Dict[str, int] = Field(default_factory=dict)
    cluster_versions: Dict[str, int] = Field(default_factory=dict)
