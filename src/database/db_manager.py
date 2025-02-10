from typing import Dict, List, Optional
from datetime import datetime
import logging
from src.utils.base_db_handler import BaseDBHandler, DatabaseError

logger = logging.getLogger(__name__)

class DatabaseManager(BaseDBHandler):
    """Manages client and document database operations."""

    def _create_tables(self, cursor) -> None:
        """Create necessary database tables."""
        # Clients table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                passport_number TEXT UNIQUE NOT NULL,
                emirates_id TEXT UNIQUE NULL,
                first_name TEXT NOT NULL,
                middle_name TEXT,
                last_name TEXT NOT NULL,
                arabic_first_name TEXT,
                arabic_middle_name TEXT,
                arabic_last_name TEXT,
                date_of_birth TEXT,
                nationality TEXT,
                gender TEXT,
                email TEXT,
                mobile_number TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Submissions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                submission_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                insurance_company TEXT NOT NULL,
                policy_number TEXT,
                reference_email_id TEXT,
                FOREIGN KEY (client_id) REFERENCES clients (id)
            )
        """)

        # Documents table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                document_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                processed_path TEXT,
                status TEXT NOT NULL,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients (id)
            )
        """)

    def client_exists(self, passport_number: str) -> bool:
        """Check if a client exists based on passport number."""
        result = self.execute_query(
            "SELECT id FROM clients WHERE passport_number = ?",
            (passport_number,)
        )
        return bool(result)

    def add_client(self, client_data: Dict) -> int:
        """Add a new client to the database."""
        # Handle empty emirates_id
        if 'emirates_id' in client_data and not client_data['emirates_id']:
            client_data.pop('emirates_id')

        try:
            return self.insert('clients', client_data)
        except DatabaseError as e:
            logger.error(f"Failed to add client: {str(e)}")
            raise

    def add_submission(self, client_id: int, submission_data: Dict) -> int:
        """Record a new submission for a client."""
        submission_data['client_id'] = client_id
        try:
            return self.insert('submissions', submission_data)
        except DatabaseError as e:
            logger.error(f"Failed to add submission: {str(e)}")
            raise

    def add_document(self, client_id: int, document_data: Dict) -> int:
        """Record a new document for a client."""
        document_data['client_id'] = client_id
        try:
            return self.insert('documents', document_data)
        except DatabaseError as e:
            logger.error(f"Failed to add document: {str(e)}")
            raise

    def get_client_status(self, passport_number: str) -> Optional[Dict]:
        """Get the current status of a client's submission process."""
        query = """
            SELECT 
                c.id,
                c.passport_number,
                c.emirates_id,
                c.first_name,
                c.last_name,
                s.submission_date,
                s.status,
                s.insurance_company,
                s.policy_number
            FROM clients c
            LEFT JOIN submissions s ON c.id = s.client_id
            WHERE c.passport_number = ?
            ORDER BY s.submission_date DESC
            LIMIT 1
        """
        
        result = self.execute_query(query, (passport_number,))
        
        if not result:
            return None
            
        row = result[0]
        return {
            "client_id": row[0],
            "passport_number": row[1],
            "emirates_id": row[2],
            "first_name": row[3],
            "last_name": row[4],
            "last_submission_date": row[5],
            "status": row[6],
            "insurance_company": row[7],
            "policy_number": row[8]
        }

    def get_missing_documents(self, client_id: int) -> List[str]:
        """Get list of missing required documents for a client."""
        required_docs = {'passport', 'emirates_id', 'visa', 'excel_sheet'}
        
        query = """
            SELECT document_type
            FROM documents
            WHERE client_id = ? AND status = 'valid'
        """
        
        results = self.execute_query(query, (client_id,))
        submitted_docs = {row[0] for row in results}
        
        return list(required_docs - submitted_docs)

    def get_client_documents(self, client_id: int) -> List[Dict]:
        """Get all documents for a client."""
        query = """
            SELECT id, document_type, file_path, processed_path, status, upload_date
            FROM documents
            WHERE client_id = ?
            ORDER BY upload_date DESC
        """
        
        results = self.execute_query(query, (client_id,))
        return [{
            'id': row[0],
            'document_type': row[1],
            'file_path': row[2],
            'processed_path': row[3],
            'status': row[4],
            'upload_date': row[5]
        } for row in results]

    def get_active_submissions(self) -> List[Dict]:
        """Get all active submissions."""
        query = """
            SELECT 
                s.id,
                s.client_id,
                c.passport_number,
                c.first_name,
                c.last_name,
                s.submission_date,
                s.status,
                s.insurance_company
            FROM submissions s
            JOIN clients c ON s.client_id = c.id
            WHERE s.status NOT IN ('completed', 'rejected')
            ORDER BY s.submission_date DESC
        """
        
        results = self.execute_query(query)
        return [{
            'submission_id': row[0],
            'client_id': row[1],
            'passport_number': row[2],
            'client_name': f"{row[3]} {row[4]}",
            'submission_date': row[5],
            'status': row[6],
            'insurance_company': row[7]
        } for row in results]

    def update_submission_status(self, submission_id: int, status: str, 
                               details: Optional[Dict] = None) -> None:
        """Update submission status."""
        update_data = {
            'status': status,
            'last_updated': datetime.now().isoformat()
        }
        if details:
            update_data.update(details)
            
        self.update('submissions', submission_id, update_data)