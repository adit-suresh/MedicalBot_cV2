import sys
import os
import logging
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_manager import DatabaseManager
from src.utils.logger import setup_logger

def test_database_functionality():
    logger = setup_logger('database_test')
    logger.setLevel(logging.DEBUG)
    
    logger.info("Starting database functionality test...")
    
    # Use test database path
    test_db_path = "data/test_client_database.db"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    try:
        # Initialize database
        db_manager = DatabaseManager(test_db_path)
        logger.info("Database initialized successfully")
        
        # Test data
        test_client = {
            "passport_number": "A1234567",
            "emirates_id": "784-1234-1234567-1",
            "first_name": "John",
            "middle_name": "Michael",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
            "nationality": "USA",
            "gender": "M",
            "email": "john.doe@example.com"
        }
        
        # Add client
        logger.info("Adding test client...")
        client_id = db_manager.add_client(test_client)
        logger.info(f"Added client with ID: {client_id}")
        
        # Verify client exists
        exists = db_manager.client_exists(test_client["passport_number"])
        logger.info(f"Client exists check: {exists}")
        
        # Add submission
        submission_data = {
            "status": "pending",
            "insurance_company": "Test Insurance Co",
            "reference_email_id": "test123"
        }
        logger.info("Adding test submission...")
        submission_id = db_manager.add_submission(client_id, submission_data)
        logger.info(f"Added submission with ID: {submission_id}")
        
        # Add documents
        documents = [
            {"document_type": "passport", "file_path": "/path/to/passport.pdf", "status": "valid"},
            {"document_type": "emirates_id", "file_path": "/path/to/eid.pdf", "status": "valid"},
            {"document_type": "visa", "file_path": "/path/to/visa.pdf", "status": "valid"}
        ]
        
        logger.info("Adding test documents...")
        for doc in documents:
            doc_id = db_manager.add_document(client_id, doc)
            logger.info(f"Added document {doc['document_type']} with ID: {doc_id}")
        
        # Check missing documents
        missing_docs = db_manager.get_missing_documents(client_id)
        logger.info(f"Missing documents: {missing_docs}")
        
        # Get client status
        status = db_manager.get_client_status(test_client["passport_number"])
        logger.info("Client status:")
        for key, value in status.items():
            logger.info(f"  {key}: {value}")
        
        logger.info("\nDatabase functionality test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_database_functionality()
    sys.exit(0 if success else 1)