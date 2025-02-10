import pytest
import os
import logging
from typing import Dict
from unittest.mock import Mock

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def pytest_configure(config):
    """Configure test environment."""
    # Register custom markers
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as a slow test"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers and environment."""
    # Skip integration tests if SKIP_INTEGRATION is set
    if os.getenv("SKIP_INTEGRATION"):
        skip_integration = pytest.mark.skip(reason="integration tests disabled")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

@pytest.fixture
def mock_textract_client():
    """Mock AWS Textract client for tests."""
    mock = Mock()
    mock.analyze_document.return_value = {
        'Blocks': [
            {
                'BlockType': 'LINE',
                'Text': 'Sample text',
                'Confidence': 99.0
            }
        ]
    }
    return mock

@pytest.fixture
def mock_textract_client():
    """Mock AWS Textract client for tests."""
    mock = Mock()
    mock.analyze_document.return_value = {
        'Blocks': [
            {
                'BlockType': 'LINE',
                'Text': 'Sample text',
                'Confidence': 99.0
            }
        ]
    }
    return mock

@pytest.fixture
def mock_deepseek_response():
    """Mock DeepSeek API response."""
    return {
        'text': '{"passport_number": "A1234567", "name": "John Doe"}'
    }

@pytest.fixture(scope="session")
def test_db_path(tmp_path_factory):
    """Create temporary database path."""
    db_dir = tmp_path_factory.mktemp("test_db")
    return str(db_dir / "test.db")

@pytest.fixture
def test_files(tmp_path):
    """Create test document files."""
    files = {
        'passport.pdf': b'test passport content',
        'emirates_id.jpg': b'test emirates id content',
        'visa.pdf': b'test visa content'
    }
    
    created_files = []
    for name, content in files.items():
        file_path = tmp_path / name
        file_path.write_bytes(content)
        created_files.append(str(file_path))
    
    return created_files