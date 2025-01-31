import unittest
import os
import sqlite3
import tempfile
from datetime import datetime
from src.database.db_manager import DatabaseManager

class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        """Set up test database."""
        # Create a temporary directory for the test database
        self.test_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.test_dir, "test_client_database.db")
        self.db_manager = DatabaseManager(self.test_db_path)
        
        # Sample test data with nullable emirates_id
        self.test_client_data = {
            "passport_number": "A1234567",
            "first_name": "John",
            "middle_name": "Michael",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
            "nationality": "USA",
            "gender": "M",
            "email": "john.doe@example.com"
        }

    def tearDown(self):
        """Clean up test database."""
        try:
            if os.path.exists(self.test_db_path):
                os.remove(self.test_db_path)
            os.rmdir(self.test_dir)
        except PermissionError:
            pass  # Skip if file is locked

    def test_create_tables(self):
        """Test if tables are created correctly."""
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            
            # Check if tables exist
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('clients', 'submissions', 'documents')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            self.assertEqual(len(tables), 3, "Not all required tables were created")
            self.assertIn('clients', tables)
            self.assertIn('submissions', tables)
            self.assertIn('documents', tables)

    def test_add_client(self):
        """Test adding a new client."""
        client_id = self.db_manager.add_client(self.test_client_data)
        self.assertIsNotNone(client_id)

        # Test adding client with emirates_id
        client_data_with_eid = self.test_client_data.copy()
        client_data_with_eid['passport_number'] = 'B1234567'  # Different passport number
        client_data_with_eid['emirates_id'] = '784-1234-1234567-1'
        
        client_id_2 = self.db_manager.add_client(client_data_with_eid)
        self.assertIsNotNone(client_id_2)

    def test_client_exists(self):
        """Test checking if client exists."""
        # Add test client
        self.db_manager.add_client(self.test_client_data)
        
        # Check existing client
        self.assertTrue(
            self.db_manager.client_exists(self.test_client_data["passport_number"]),
            "Should find existing client"
        )
        
        # Check non-existing client
        self.assertFalse(
            self.db_manager.client_exists("NONEXISTENT"),
            "Should not find non-existent client"
        )

    def test_add_submission(self):
        """Test adding a submission for a client."""
        # Add client first
        client_id = self.db_manager.add_client(self.test_client_data)
        
        # Add submission
        submission_data = {
            "status": "pending",
            "insurance_company": "Test Insurance Co",
            "reference_email_id": "test123"
        }
        
        submission_id = self.db_manager.add_submission(client_id, submission_data)
        
        self.assertIsNotNone(submission_id, "Submission ID should not be None")
        
        # Verify submission was added
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM submissions WHERE id = ?", (submission_id,))
            submission = cursor.fetchone()
            
            self.assertIsNotNone(submission, "Submission should exist in database")
            self.assertEqual(submission[1], client_id)
            self.assertEqual(submission[3], "pending")

    def test_add_document(self):
        """Test adding a document for a client."""
        # Add client first
        client_id = self.db_manager.add_client(self.test_client_data)
        
        # Add document
        document_data = {
            "document_type": "passport",
            "file_path": "/path/to/passport.pdf",
            "status": "valid"
        }
        
        document_id = self.db_manager.add_document(client_id, document_data)
        
        self.assertIsNotNone(document_id, "Document ID should not be None")
        
        # Verify document was added
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM documents WHERE id = ?", (document_id,))
            document = cursor.fetchone()
            
            self.assertIsNotNone(document, "Document should exist in database")
            self.assertEqual(document[1], client_id)
            self.assertEqual(document[2], "passport")

    def test_get_client_status(self):
        """Test getting client's current status."""
        # Add client
        client_id = self.db_manager.add_client(self.test_client_data)
        
        # Add submission
        submission_data = {
            "status": "pending",
            "insurance_company": "Test Insurance Co",
            "reference_email_id": "test123"
        }
        self.db_manager.add_submission(client_id, submission_data)
        
        # Get status
        status = self.db_manager.get_client_status(self.test_client_data["passport_number"])
        
        self.assertIsNotNone(status, "Should get client status")
        self.assertEqual(status["status"], "pending")
        self.assertEqual(status["insurance_company"], "Test Insurance Co")

    def test_get_missing_documents(self):
        """Test getting list of missing documents."""
        # Add client
        client_id = self.db_manager.add_client(self.test_client_data)
        
        # Add some documents but not all
        document_data = {
            "document_type": "passport",
            "file_path": "/path/to/passport.pdf",
            "status": "valid"
        }
        self.db_manager.add_document(client_id, document_data)
        
        # Get missing documents
        missing = self.db_manager.get_missing_documents(client_id)
        
        self.assertIn("emirates_id", missing, "Emirates ID should be missing")
        self.assertIn("visa", missing, "Visa should be missing")
        self.assertIn("excel_sheet", missing, "Excel sheet should be missing")
        self.assertNotIn("passport", missing, "Passport should not be missing")

    def test_duplicate_client_prevention(self):
        """Test that duplicate clients cannot be added."""
        # Add initial client
        self.db_manager.add_client(self.test_client_data)
        
        # Try to add same client again
        with self.assertRaises(sqlite3.IntegrityError):
            self.db_manager.add_client(self.test_client_data)

    def test_get_client_status_nonexistent(self):
        """Test getting status for non-existent client."""
        status = self.db_manager.get_client_status("NONEXISTENT")
        self.assertIsNone(status, "Should return None for non-existent client")

    def test_full_client_workflow(self):
        """Test complete workflow from client creation to document submission."""
        # 1. Add client
        client_id = self.db_manager.add_client(self.test_client_data)
        
        # 2. Add submission
        submission_data = {
            "status": "in_progress",
            "insurance_company": "Test Insurance Co",
            "reference_email_id": "test123"
        }
        submission_id = self.db_manager.add_submission(client_id, submission_data)
        
        # 3. Add documents one by one
        documents = [
            {"document_type": "passport", "file_path": "/path/to/passport.pdf", "status": "valid"},
            {"document_type": "emirates_id", "file_path": "/path/to/eid.pdf", "status": "valid"},
            {"document_type": "visa", "file_path": "/path/to/visa.pdf", "status": "valid"},
            {"document_type": "excel_sheet", "file_path": "/path/to/details.xlsx", "status": "valid"}
        ]
        
        for doc in documents:
            self.db_manager.add_document(client_id, doc)
        
        # 4. Verify all steps
        # Check client exists
        self.assertTrue(self.db_manager.client_exists(self.test_client_data["passport_number"]))
        
        # Check submission status
        status = self.db_manager.get_client_status(self.test_client_data["passport_number"])
        self.assertEqual(status["status"], "in_progress")
        
        # Check no missing documents
        missing_docs = self.db_manager.get_missing_documents(client_id)
        self.assertEqual(len(missing_docs), 0, "Should have no missing documents")

if __name__ == '__main__':
    unittest.main()