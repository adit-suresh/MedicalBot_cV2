import os
import logging
from dotenv import load_dotenv
import boto3
import requests
from datetime import datetime

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_aws_textract():
    """Test AWS Textract connection and basic functionality."""
    try:
        # Initialize Textract client
        textract = boto3.client(
            'textract',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Create a simple test document
        test_text = "This is a test document\nPassport No: A1234567\nName: John Doe"
        from PIL import Image, ImageDraw, ImageFont
        import io
        
        # Create a test image with text
        img = Image.new('RGB', (500, 200), color='white')
        d = ImageDraw.Draw(img)
        d.text((10,10), test_text, fill=(0,0,0))
        
        # Convert to bytes
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()
        
        # Test Textract
        response = textract.analyze_document(
            Document={'Bytes': img_byte_arr},
            FeatureTypes=['FORMS', 'TABLES']
        )
        
        logger.info("✓ AWS Textract connection successful")
        logger.info(f"Found {len(response['Blocks'])} blocks in test document")
        return True
        
    except Exception as e:
        logger.error(f"✗ AWS Textract test failed: {str(e)}")
        return False

def test_deepseek():
    """Test DeepSeek API connection."""
    try:
        headers = {
            'Authorization': f"Bearer {os.getenv('DEEPSEEK_API_KEY')}",
            'Content-Type': 'application/json'
        }
        
        # Simple test request
        response = requests.get(
            os.getenv('DEEPSEEK_API_URL') + '/health',
            headers=headers
        )
        
        if response.status_code == 200:
            logger.info("✓ DeepSeek API connection successful")
            return True
        else:
            logger.error(f"✗ DeepSeek API returned status code: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"✗ DeepSeek API test failed: {str(e)}")
        return False

def test_database():
    """Test database operations."""
    try:
        from src.database.db_manager import DatabaseManager
        
        # Use test database
        db_path = f"test_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        db = DatabaseManager(db_path)
        
        # Test data
        test_client = {
            "passport_number": "TEST123",
            "first_name": "Test",
            "last_name": "User",
            "nationality": "USA"
        }
        
        # Test operations
        client_id = db.add_client(test_client)
        client_exists = db.client_exists("TEST123")
        
        if client_id and client_exists:
            logger.info("✓ Database operations successful")
            return True
        else:
            logger.error("✗ Database operations failed")
            return False
            
    except Exception as e:
        logger.error(f"✗ Database test failed: {str(e)}")
        return False
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)

def main():
    """Run all smoke tests."""
    logger.info("Starting smoke tests...")
    
    # Load environment variables
    load_dotenv()
    
    # Track test results
    results = {
        'aws_textract': test_aws_textract(),
        'deepseek': test_deepseek(),
        'database': test_database()
    }
    
    # Print summary
    logger.info("\nTest Summary:")
    for test, success in results.items():
        status = "✓" if success else "✗"
        logger.info(f"{status} {test}")
    
    # Overall status
    if all(results.values()):
        logger.info("\nAll smoke tests passed!")
        return 0
    else:
        logger.error("\nSome smoke tests failed")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())