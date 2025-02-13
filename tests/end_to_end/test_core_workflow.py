import os
import sys
import pytest
import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.data_extractor import DataExtractor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_combiner import DataCombiner
from src.utils.process_tracker import ProcessTracker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestCoreWorkflow:
    """End-to-end tests for core bot workflow."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment and services."""
        # Initialize services
        self.outlook_client = OutlookClient()
        self.attachment_handler = AttachmentHandler()
        self.textract_processor = TextractProcessor()
        self.data_extractor = DataExtractor()
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract_processor, self.excel_processor)
        self.process_tracker = ProcessTracker()
        
        # Test data paths
        self.test_files = self._get_test_files()
        self.template_path = os.path.join("test_files", "template.xlsx")
        
        # Create output directory
        os.makedirs("test_output", exist_ok=True)
        
        yield
        
        # Cleanup
        self._cleanup()

    def _get_test_files(self) -> Dict[str, str]:
        """Get paths of test files."""
        test_files = {}
        test_dir = "test_files"
        
        for filename in os.listdir(test_dir):
            filepath = os.path.join(test_dir, filename)
            if 'passport' in filename.lower():
                test_files['passport'] = filepath
            elif 'emirates' in filename.lower():
                test_files['emirates_id'] = filepath
            elif 'visa' in filename.lower():
                test_files['visa'] = filepath
            elif filename.endswith('.xlsx') and 'template' not in filename.lower():
                test_files['excel'] = filepath
                
        return test_files

    def test_1_email_scanning(self):
        """Test email scanning and filtering."""
        # Fetch recent emails
        emails = self.outlook_client.fetch_emails()
        assert isinstance(emails, list)
        
        if emails:
            test_email = emails[0]
            assert 'id' in test_email
            assert 'subject' in test_email
            assert 'hasAttachments' in test_email

    def test_2_attachment_handling(self):
        """Test attachment downloading and validation."""
        # Mock email with attachments
        test_attachments = []
        for doc_type, file_path in self.test_files.items():
            with open(file_path, 'rb') as f:
                content = f.read()
                test_attachments.append({
                    'name': os.path.basename(file_path),
                    'contentBytes': content,
                    'contentType': 'application/pdf' if file_path.endswith('.pdf') 
                                 else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                })
        
        # Process attachments
        saved_paths = self.attachment_handler.process_attachments(
            test_attachments,
            f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        assert len(saved_paths) > 0
        for path in saved_paths:
            assert os.path.exists(path)

    def test_3_document_extraction(self):
        """Test document data extraction."""
        results = {}
        
        # Process each document
        for doc_type, file_path in self.test_files.items():
            if doc_type != 'excel':
                try:
                    # Extract text
                    extracted_data = self.textract_processor.process_document(file_path, doc_type)
                    assert isinstance(extracted_data, dict)
                    
                    # Validate data
                    if doc_type == 'passport':
                        assert any(key in extracted_data for key in ['passport_number', 'name', 'nationality'])
                    elif doc_type == 'emirates_id':
                        assert any(key in extracted_data for key in ['emirates_id', 'name_en'])
                    elif doc_type == 'visa':
                        assert any(key in extracted_data for key in ['visa_number', 'full_name'])
                        
                    results[doc_type] = extracted_data
                    
                except Exception as e:
                    logger.error(f"Error processing {doc_type}: {str(e)}")
                    
        assert len(results) > 0
        return results

    def test_4_excel_processing(self):
        """Test Excel data processing."""
        if 'excel' in self.test_files:
            # Process Excel file
            df, errors = self.excel_processor.process_excel(
                self.test_files['excel'],
                dayfirst=True
            )
            
            assert not df.empty
            assert isinstance(df, pd.DataFrame)
            
            # Verify required columns
            required_columns = [
                'first_name', 'last_name', 'emirates_id', 
                'passport_number', 'nationality'
            ]
            for col in required_columns:
                assert col in df.columns, f"Missing required column: {col}"
                
            # Verify date formats (if any date column exists)
            
            date_columns = ['date_of_birth', 'effective_date']
            for col in date_columns:
                if col in df.columns:
                    # Check if dates are in YYYY-MM-DD format
                    date_format_ok = df[col].str.match(r'^\d{4}-\d{2}-\d{2}$').all()
                    assert date_format_ok, f"Incorrect date format in column {col}"
                
            return df, errors
        return None

    def test_5_data_combination(self):
        """Test combining extracted data with Excel template."""
        # Get extracted document data
        doc_data = self.test_3_document_extraction()
        
        # Get Excel data if available
        excel_data = None
        if 'excel' in self.test_files:
            df, _ = self.test_4_excel_processing()
            excel_data = df.iloc[0].to_dict() if not df.empty else None
        
        # Combine data into template
        output_path = os.path.join(
            "test_output",
            f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        
        result = self.data_combiner.combine_and_populate_template(
            self.template_path,
            output_path,
            doc_data,
            excel_data
        )
        
        assert result['status'] in ['success', 'completed_with_errors']
        assert os.path.exists(result['output_path'])
        
        # Verify output file
        df = pd.read_excel(result['output_path'])
        assert not df.empty
        
        return result

    def _cleanup(self):
        """Clean up test artifacts."""
        if os.path.exists("test_output"):
            import shutil
            shutil.rmtree("test_output")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])