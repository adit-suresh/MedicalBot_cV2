import os
import pytest
import logging
from dotenv import load_dotenv

# Initialize logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def pytest_configure(config):
    """Configure test environment."""
    # Load environment variables
    load_dotenv()
    
    # Check required environment variables
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'DEEPSEEK_API_KEY',
        'SLACK_BOT_TOKEN',
        'CLIENT_ID',
        'CLIENT_SECRET',
        'TENANT_ID'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.warning(f"Missing environment variables: {', '.join(missing_vars)}")
        logger.warning("Some tests may be skipped due to missing configuration")

def create_test_files():
    """Create test document files for OCR testing."""
    test_files_dir = os.path.join(os.path.dirname(__file__), 'test_files')
    os.makedirs(test_files_dir, exist_ok=True)
    
    # List of test files to create
    test_files = {
        'passport.pdf': b'test passport content',
        'emirates_id.jpg': b'test emirates id content',
        'visa.pdf': b'test visa content',
        'work_permit.pdf': b'test work permit content'
    }
    
    created_files = []
    for filename, content in test_files.items():
        file_path = os.path.join(test_files_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(content)
        created_files.append(file_path)
    
    return created_files

# Create test files when module is imported
TEST_FILES = create_test_files()

@pytest.fixture(scope='session')
def test_files():
    """Fixture to provide test file paths."""
    return TEST_FILES

@pytest.fixture(scope='session')
def cleanup_test_files():
    """Cleanup test files after tests."""
    yield
    for file_path in TEST_FILES:
        if os.path.exists(file_path):
            os.remove(file_path)