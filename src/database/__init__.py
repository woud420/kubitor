"""Database package for k8s-scanner - connection management only."""

from .connection import DatabaseConnection, AsyncDatabaseConnection

__all__ = ["DatabaseConnection", "AsyncDatabaseConnection"]
