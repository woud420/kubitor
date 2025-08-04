"""Base DAO classes with database-specific implementations."""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Type
from enum import Enum

from ...database.connection import DatabaseConnection
from ...utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseDialect(Enum):
    """Enum for database dialects."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class BaseDAO(ABC):
    """Abstract base DAO defining the interface."""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection

    @abstractmethod
    def get_table_name(self) -> str:
        """Get the table name for this DAO."""
        pass

    @abstractmethod
    def find_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Find record by ID."""
        pass

    @abstractmethod
    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Find all records."""
        pass

    @abstractmethod
    def count_all(self, where_clause: str = "", params: List = None) -> int:
        """Count records with optional where clause."""
        pass

    @abstractmethod
    def delete_by_id(self, record_id: int) -> bool:
        """Delete record by ID."""
        pass

    @abstractmethod
    def delete_where(self, where_clause: str, params: List = None) -> int:
        """Delete records matching where clause."""
        pass

    @abstractmethod
    def find_where(
        self,
        where_clause: str,
        params: List = None,
        order_by: str = "",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Find records with where clause."""
        pass

    @abstractmethod
    def update_where(self, set_clause: str, where_clause: str, params: List = None) -> int:
        """Update records matching where clause."""
        pass


class BaseSQLiteDAO(BaseDAO):
    """Base DAO implementation for SQLite."""

    def find_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Find record by ID."""
        query = f"SELECT * FROM {self.get_table_name()} WHERE id = ?"
        return self.db.fetch_one(query, (record_id,))

    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Find all records."""
        query = f"SELECT * FROM {self.get_table_name()}"
        params = []

        if limit:
            query += " LIMIT ? OFFSET ?"
            params = [limit, offset]

        return self.db.fetch_all(query, params)

    def count_all(self, where_clause: str = "", params: List = None) -> int:
        """Count records with optional where clause."""
        query = f"SELECT COUNT(*) as count FROM {self.get_table_name()}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.db.fetch_one(query, params or [])
        return result["count"] if result else 0

    def delete_by_id(self, record_id: int) -> bool:
        """Delete record by ID."""
        query = f"DELETE FROM {self.get_table_name()} WHERE id = ?"
        rows_affected = self.db.execute(query, (record_id,))
        return rows_affected > 0

    def delete_where(self, where_clause: str, params: List = None) -> int:
        """Delete records matching where clause."""
        query = f"DELETE FROM {self.get_table_name()} WHERE {where_clause}"
        return self.db.execute(query, params or [])

    def find_where(
        self,
        where_clause: str,
        params: List = None,
        order_by: str = "",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Find records with where clause."""
        query = f"SELECT * FROM {self.get_table_name()} WHERE {where_clause}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        return self.db.fetch_all(query, params or [])

    def update_where(self, set_clause: str, where_clause: str, params: List = None) -> int:
        """Update records matching where clause."""
        query = f"UPDATE {self.get_table_name()} SET {set_clause} WHERE {where_clause}"
        return self.db.execute(query, params or [])

    def build_placeholders(self, count: int) -> str:
        """Build parameter placeholders for SQLite."""
        return ", ".join(["?" for _ in range(count)])

    def insert_returning_id(self, columns: List[str], values: List[Any]) -> Optional[int]:
        """Insert record and return ID."""
        placeholders = self.build_placeholders(len(values))
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {self.get_table_name()} ({columns_str}) VALUES ({placeholders})"
        return self.db.insert_returning_id(query, values)

    def bulk_insert(self, columns: List[str], values_list: List[List[Any]]) -> int:
        """Bulk insert records."""
        if not values_list:
            return 0

        placeholders = self.build_placeholders(len(columns))
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {self.get_table_name()} ({columns_str}) VALUES ({placeholders})"

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, values_list)
            return len(values_list)


class BasePostgreSQLDAO(BaseDAO):
    """Base DAO implementation for PostgreSQL."""

    def find_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Find record by ID."""
        query = f"SELECT * FROM {self.get_table_name()} WHERE id = %s"
        return self.db.fetch_one(query, (record_id,))

    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Find all records."""
        query = f"SELECT * FROM {self.get_table_name()}"
        params = []

        if limit:
            query += " LIMIT %s OFFSET %s"
            params = [limit, offset]

        return self.db.fetch_all(query, params)

    def count_all(self, where_clause: str = "", params: List = None) -> int:
        """Count records with optional where clause."""
        query = f"SELECT COUNT(*) as count FROM {self.get_table_name()}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.db.fetch_one(query, params or [])
        return result["count"] if result else 0

    def delete_by_id(self, record_id: int) -> bool:
        """Delete record by ID."""
        query = f"DELETE FROM {self.get_table_name()} WHERE id = %s"
        rows_affected = self.db.execute(query, (record_id,))
        return rows_affected > 0

    def delete_where(self, where_clause: str, params: List = None) -> int:
        """Delete records matching where clause."""
        query = f"DELETE FROM {self.get_table_name()} WHERE {where_clause}"
        return self.db.execute(query, params or [])

    def find_where(
        self,
        where_clause: str,
        params: List = None,
        order_by: str = "",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Find records with where clause."""
        query = f"SELECT * FROM {self.get_table_name()} WHERE {where_clause}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        return self.db.fetch_all(query, params or [])

    def update_where(self, set_clause: str, where_clause: str, params: List = None) -> int:
        """Update records matching where clause."""
        query = f"UPDATE {self.get_table_name()} SET {set_clause} WHERE {where_clause}"
        return self.db.execute(query, params or [])

    def build_placeholders(self, count: int) -> str:
        """Build parameter placeholders for PostgreSQL."""
        return ", ".join([f"%s" for _ in range(count)])

    def insert_returning_id(self, columns: List[str], values: List[Any]) -> Optional[int]:
        """Insert record and return ID."""
        placeholders = self.build_placeholders(len(values))
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {self.get_table_name()} ({columns_str}) VALUES ({placeholders}) RETURNING id"
        return self.db.insert_returning_id(query, values)

    def bulk_insert(self, columns: List[str], values_list: List[List[Any]]) -> int:
        """Bulk insert records."""
        if not values_list:
            return 0

        placeholders = self.build_placeholders(len(columns))
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {self.get_table_name()} ({columns_str}) VALUES ({placeholders})"

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, values_list)
            return len(values_list)


# Dictionary mapping dialect enum to base DAO class
BASE_DAO_REGISTRY: Dict[DatabaseDialect, Type[BaseDAO]] = {
    DatabaseDialect.SQLITE: BaseSQLiteDAO,
    DatabaseDialect.POSTGRESQL: BasePostgreSQLDAO,
}


def create_base_dao(db_connection: DatabaseConnection) -> Type[BaseDAO]:
    """Factory function to return appropriate base DAO class using enum + dictionary pattern."""
    try:
        dialect_enum = DatabaseDialect(db_connection.dialect)
    except ValueError:
        raise ValueError(f"Unsupported database dialect: {db_connection.dialect}")

    dao_class = BASE_DAO_REGISTRY.get(dialect_enum)
    if dao_class is None:
        raise ValueError(f"No base DAO implementation for dialect: {dialect_enum}")

    return dao_class
