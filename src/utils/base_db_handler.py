from typing import Optional, Dict, List, Any
import sqlite3
import logging
from datetime import datetime
import json
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Base exception for database operations."""
    pass

class BaseDBHandler(ABC):
    def __init__(self, db_path: str, max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize database handler.
        
        Args:
            db_path: Path to SQLite database
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.db_path = db_path
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._initialize_db()

    def _initialize_db(self) -> None:
        """Initialize database tables."""
        self._execute_with_retry(self._create_tables)

    @abstractmethod
    def _create_tables(self, cursor: sqlite3.Cursor) -> None:
        """Create necessary database tables.
        
        Args:
            cursor: SQLite cursor
        """
        pass

    def _execute_with_retry(self, operation: callable, *args, **kwargs) -> Any:
        """Execute database operation with retry logic.
        
        Args:
            operation: Callable database operation
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result of the operation
            
        Raises:
            DatabaseError: If operation fails after all retries
        """
        import time
        
        for attempt in range(self.max_retries):
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    result = operation(cursor, *args, **kwargs)
                    conn.commit()
                    return result
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise DatabaseError(f"Database operation failed: {str(e)}")
            except Exception as e:
                raise DatabaseError(f"Unexpected database error: {str(e)}")

    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a SELECT query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of query results
        """
        def _execute(cursor: sqlite3.Cursor) -> List[tuple]:
            cursor.execute(query, params or ())
            return cursor.fetchall()
        
        return self._execute_with_retry(_execute)

    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        def _execute(cursor: sqlite3.Cursor) -> int:
            cursor.execute(query, params or ())
            return cursor.rowcount
        
        return self._execute_with_retry(_execute)

    def get_by_id(self, table: str, id_val: Any) -> Optional[Dict]:
        """Get record by ID.
        
        Args:
            table: Table name
            id_val: ID value
            
        Returns:
            Record as dictionary or None if not found
        """
        def _execute(cursor: sqlite3.Cursor) -> Optional[Dict]:
            cursor.execute(f"SELECT * FROM {table} WHERE id = ?", (id_val,))
            columns = [description[0] for description in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
        
        return self._execute_with_retry(_execute)

    def insert(self, table: str, data: Dict) -> int:
        """Insert a new record.
        
        Args:
            table: Table name
            data: Data to insert
            
        Returns:
            ID of inserted record
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        def _execute(cursor: sqlite3.Cursor) -> int:
            cursor.execute(query, tuple(data.values()))
            return cursor.lastrowid
        
        return self._execute_with_retry(_execute)

    def update(self, table: str, id_val: Any, data: Dict) -> int:
        """Update an existing record.
        
        Args:
            table: Table name
            id_val: ID of record to update
            data: Data to update
            
        Returns:
            Number of affected rows
        """
        set_clause = ', '.join(f"{k} = ?" for k in data.keys())
        query = f"UPDATE {table} SET {set_clause} WHERE id = ?"
        params = tuple(data.values()) + (id_val,)
        
        return self.execute_update(query, params)

    def delete(self, table: str, id_val: Any) -> int:
        """Delete a record.
        
        Args:
            table: Table name
            id_val: ID of record to delete
            
        Returns:
            Number of affected rows
        """
        return self.execute_update(f"DELETE FROM {table} WHERE id = ?", (id_val,))