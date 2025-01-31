import sqlite3
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import os
import time

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "data/client_database.db"):
        """Initialize database manager."""
        self.db_path = db_path
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._create_tables()

    def _execute_with_retry(self, func, *args, **kwargs):
        """Execute a database operation with retry logic."""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) or "process cannot access" in str(e):
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                raise
            except Exception as e:
                raise

    def _create_tables(self) -> None:
        """Create necessary database tables if they don't exist."""
        def create_tables_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

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

                conn.commit()
                logger.info("Database tables created successfully")

        self._execute_with_retry(create_tables_func)

    def client_exists(self, passport_number: str) -> bool:
        """Check if a client exists based on passport number."""
        def check_client_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM clients WHERE passport_number = ?",
                    (passport_number,)
                )
                return cursor.fetchone() is not None

        return self._execute_with_retry(check_client_func)

    def add_client(self, client_data: Dict[str, str]) -> int:
        """Add a new client to the database."""
        def add_client_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Handle empty emirates_id
                if 'emirates_id' in client_data and not client_data['emirates_id']:
                    client_data.pop('emirates_id')
                
                fields = []
                values = []
                placeholders = []
                
                for field, value in client_data.items():
                    if value is not None:
                        fields.append(field)
                        values.append(value)
                        placeholders.append('?')

                query = f"""
                    INSERT INTO clients ({', '.join(fields)})
                    VALUES ({', '.join(placeholders)})
                """
                
                cursor.execute(query, values)
                conn.commit()
                return cursor.lastrowid

        return self._execute_with_retry(add_client_func)

    def add_submission(self, client_id: int, submission_data: Dict[str, str]) -> int:
        """Record a new submission for a client."""
        def add_submission_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                submission_data['client_id'] = client_id
                fields = list(submission_data.keys())
                placeholders = ['?' for _ in fields]
                values = [submission_data[field] for field in fields]

                query = f"""
                    INSERT INTO submissions ({', '.join(fields)})
                    VALUES ({', '.join(placeholders)})
                """
                
                cursor.execute(query, values)
                conn.commit()
                return cursor.lastrowid

        return self._execute_with_retry(add_submission_func)

    def add_document(self, client_id: int, document_data: Dict[str, str]) -> int:
        """Record a new document for a client."""
        def add_document_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                document_data['client_id'] = client_id
                fields = list(document_data.keys())
                placeholders = ['?' for _ in fields]
                values = [document_data[field] for field in fields]

                query = f"""
                    INSERT INTO documents ({', '.join(fields)})
                    VALUES ({', '.join(placeholders)})
                """
                
                cursor.execute(query, values)
                conn.commit()
                return cursor.lastrowid

        return self._execute_with_retry(add_document_func)

    def get_client_status(self, passport_number: str) -> Optional[Dict]:
        """Get the current status of a client's submission process."""
        def get_status_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
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
                
                cursor.execute(query, (passport_number,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        "client_id": result[0],
                        "passport_number": result[1],
                        "emirates_id": result[2],
                        "first_name": result[3],
                        "last_name": result[4],
                        "last_submission_date": result[5],
                        "status": result[6],
                        "insurance_company": result[7],
                        "policy_number": result[8]
                    }
                return None

        return self._execute_with_retry(get_status_func)

    def get_missing_documents(self, client_id: int) -> List[str]:
        """Get list of missing required documents for a client."""
        required_docs = {'passport', 'emirates_id', 'visa', 'excel_sheet'}
        
        def get_documents_func():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = """
                    SELECT document_type
                    FROM documents
                    WHERE client_id = ? AND status = 'valid'
                """
                
                cursor.execute(query, (client_id,))
                submitted_docs = {row[0] for row in cursor.fetchall()}
                
                return list(required_docs - submitted_docs)

        return self._execute_with_retry(get_documents_func)