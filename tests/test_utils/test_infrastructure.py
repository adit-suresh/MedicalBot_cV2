import pytest
import sqlite3
from typing import Dict, Optional

from src.utils.base_db_handler import BaseDBHandler, DatabaseError
from src.utils.dependency_container import DependencyContainer, inject

# Test implementation of BaseDBHandler
class TestDBHandler(BaseDBHandler):
    def _create_tables(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value TEXT
            )
        """)

def test_base_db_handler(temp_db_path):
    """Test BaseDBHandler functionality."""
    handler = TestDBHandler(temp_db_path)
    
    # Test insert
    data = {"name": "test", "value": "123"}
    id = handler.insert("test_table", data)
    assert id == 1
    
    # Test get by id
    result = handler.get_by_id("test_table", 1)
    assert result["name"] == "test"
    assert result["value"] == "123"
    
    # Test update
    handler.update("test_table", 1, {"value": "456"})
    result = handler.get_by_id("test_table", 1)
    assert result["value"] == "456"
    
    # Test delete
    handler.delete("test_table", 1)
    result = handler.get_by_id("test_table", 1)
    assert result is None

def test_db_handler_retry_logic(temp_db_path):
    """Test database retry logic."""
    handler = TestDBHandler(temp_db_path, max_retries=2)
    
    # Simulate database lock
    def failing_operation(cursor):
        raise sqlite3.OperationalError("database is locked")
    
    with pytest.raises(DatabaseError):
        handler._execute_with_retry(failing_operation)

# Test classes for dependency injection
class IService:
    def get_value(self) -> str:
        pass

class ServiceImpl(IService):
    def get_value(self) -> str:
        return "test_value"

@inject(IService)
class Client:
    def get_service_value(self) -> str:
        return self._iservice.get_value()

def test_dependency_injection():
    """Test dependency injection container."""
    container = DependencyContainer()
    
    # Register service
    container.register(IService, ServiceImpl)
    
    # Create client
    client = Client()
    
    # Test injection
    assert client.get_service_value() == "test_value"

def test_singleton_injection():
    """Test singleton instance injection."""
    container = DependencyContainer()
    
    # Register singleton instance
    service = ServiceImpl()
    container.register_instance(IService, service)
    
    # Create multiple clients
    client1 = Client()
    client2 = Client()
    
    # Verify same instance
    assert client1._iservice is client2._iservice

def test_factory_injection():
    """Test factory function injection."""
    container = DependencyContainer()
    
    def create_service():
        return ServiceImpl()
    
    # Register factory
    container.register_factory(IService, create_service)
    
    # Create client
    client = Client()
    
    # Test injection
    assert client.get_service_value() == "test_value"

def test_missing_dependency():
    """Test handling of missing dependencies."""
    container = DependencyContainer()
    
    # Try to create client without registering dependency
    with pytest.raises(ValueError):
        client = Client()