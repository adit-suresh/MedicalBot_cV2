import os
import sys
import logging
import pandas as pd
import re
from datetime import datetime
import traceback
from typing import Dict, List, Any, Optional

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for maximum info
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("diagnostic_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("diagnosis")

def print_separator(title=None):
    """Print a separator line with optional title."""
    if title:
        logger.info(f"{'=' * 20} {title} {'=' * 20}")
    else:
        logger.info("=" * 80)

def diagnose_email_fetch():
    """Diagnose issues with email fetching."""
    print_separator("EMAIL FETCH DIAGNOSIS")
    
    try:
        # Import the OutlookClient
        from src.email_handler.outlook_client import OutlookClient
        
        logger.info("Creating OutlookClient instance...")
        client = OutlookClient()
        
        logger.info("Fetching emails...")
        emails = client.fetch_emails()
        
        logger.info(f"Successfully fetched {len(emails)} emails")
        
        # Print basic info about each email
        for idx, email in enumerate(emails, 1):
            logger.info(f"Email {idx}: ID={email['id']}, Subject={email.get('subject', 'N/A')}")
            
            # Check if this email was processed before
            tracking_file = "processed_emails.txt"
            if os.path.exists(tracking_file):
                with open(tracking_file, 'r') as f:
                    processed_ids = f.read().splitlines()
                    
                if email['id'] in processed_ids:
                    logger.warning(f"Email {email['id']} was processed before - duplicate processing likely!")
        
        return True
    except Exception as e:
        logger.error(f"Error in email fetch: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def diagnose_attachment_handling(email_id=None):
    """Diagnose issues with attachment handling."""
    print_separator("ATTACHMENT HANDLING DIAGNOSIS")
    
    try:
        # Import necessary classes
        from src.email_handler.outlook_client import OutlookClient
        from src.email_handler.attachment_handler import AttachmentHandler
        
        logger.info("Creating OutlookClient and AttachmentHandler instances...")
        outlook = OutlookClient()
        attachment_handler = AttachmentHandler()
        
        # If no email_id provided, fetch one
        if not email_id:
            emails = outlook.fetch_emails()
            if not emails:
                logger.error("No emails found to test attachment handling")
                return False
            email_id = emails[0]['id']
            logger.info(f"Using email {email_id} for attachment test")
        
        # Get attachments
        logger.info(f"Getting attachments for email {email_id}...")
        attachments = outlook.get_attachments(email_id)
        logger.info(f"Found {len(attachments)} attachments")
        
        # Process attachments
        logger.info("Processing attachments...")
        saved_files = attachment_handler.process_attachments(attachments, email_id)
        logger.info(f"Saved {len(saved_files)} files")
        
        # Print file info
        for file_path in saved_files:
            logger.info(f"File: {file_path}")
            
            # Check file type based on name
            name = file_path.lower()
            file_type = "unknown"
            if name.endswith(('.xlsx', '.xls')):
                file_type = "excel"
            elif 'passport' in name:
                file_type = "passport"
            elif 'emirates' in name or 'eid' in name:
                file_type = "emirates_id"
            elif 'visa' in name:
                file_type = "visa"
                
            logger.info(f"  - Type (based on name): {file_type}")
            
            # If Excel, try to read and check rows
            if file_type == "excel":
                try:
                    df = pd.read_excel(file_path)
                    logger.info(f"  - Excel rows: {len(df)}")
                    logger.info(f"  - Excel columns: {list(df.columns)}")
                    
                    # Check first row
                    if not df.empty:
                        first_row = df.iloc[0]
                        logger.info(f"  - First row data: {first_row.to_dict()}")
                except Exception as e:
                    logger.error(f"  - Error reading Excel: {str(e)}")
        
        return True
    except Exception as e:
        logger.error(f"Error in attachment handling: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def diagnose_document_processing():
    """Diagnose issues with document processing."""
    print_separator("DOCUMENT PROCESSING DIAGNOSIS")
    
    try:
        # Import necessary classes
        from src.document_processor.textract_processor import TextractProcessor
        
        logger.info("Creating TextractProcessor instance...")
        textract = TextractProcessor()
        
        # Look for document files to test
        doc_files = []
        for root, _, files in os.walk('.'):
            for file in files:
                lower_name = file.lower()
                if any(term in lower_name for term in ['passport', 'emirates', 'eid', 'visa']) and \
                   lower_name.endswith(('.pdf', '.jpg', '.jpeg', '.png')):
                    doc_files.append(os.path.join(root, file))
                    if len(doc_files) >= 3:  # Limit to 3 files for testing
                        break
        
        if not doc_files:
            logger.warning("No document files found for testing")
            return False
            
        # Test document processing
        for file_path in doc_files:
            logger.info(f"Processing document: {file_path}")
            
            # Determine document type from filename
            name = file_path.lower()
            doc_type = "unknown"
            if 'passport' in name:
                doc_type = "passport"
            elif 'emirates' in name or 'eid' in name:
                doc_type = "emirates_id"
            elif 'visa' in name:
                doc_type = "visa"
                
            logger.info(f"  - Document type (based on name): {doc_type}")
            
            # Extract text and data
            try:
                extracted_text = textract.extract_text(file_path)
                logger.info(f"  - Extracted text length: {len(extracted_text)} characters")
                logger.info(f"  - First 100 chars: {extracted_text[:100]}...")
                
                data = textract.process_document(file_path, doc_type)
                logger.info(f"  - Extracted data: {data}")
                
                # Check date formats in extracted data
                for key, value in data.items():
                    if isinstance(value, str) and any(date_field in key for date_field in ['date', 'expiry', 'dob']):
                        logger.info(f"  - Date field {key}: {value}")
                        if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                            logger.warning(f"  - Date format is YYYY-MM-DD, should be DD-MM-YYYY")
            except Exception as e:
                logger.error(f"  - Error processing document: {str(e)}")
                logger.error(traceback.format_exc())
        
        return True
    except Exception as e:
        logger.error(f"Error in document processing: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def diagnose_excel_processing():
    """Diagnose issues with Excel processing."""
    print_separator("EXCEL PROCESSING DIAGNOSIS")
    
    try:
        # Look for Excel files to test
        excel_files = []
        for root, _, files in os.walk('.'):
            for file in files:
                if file.endswith(('.xlsx', '.xls')) and 'template' not in file.lower():
                    excel_files.append(os.path.join(root, file))
                    if len(excel_files) >= 3:  # Limit to 3 files for testing
                        break
        
        if not excel_files:
            logger.warning("No Excel files found for testing")
            return False
            
        # Find Excel processing class - try different possible names
        excel_processor = None
        try:
            from src.document_processor.excel_processor import ExcelProcessor
            excel_processor = ExcelProcessor()
            logger.info("Found ExcelProcessor class")
        except (ImportError, AttributeError):
            try:
                import src.document_processor.excel_processor as module
                for attr_name in dir(module):
                    if not attr_name.startswith('_') and attr_name.endswith('Processor'):
                        cls = getattr(module, attr_name)
                        if callable(cls):
                            excel_processor = cls()
                            logger.info(f"Found {attr_name} class")
                            break
            except Exception:
                logger.warning("Could not find Excel processor class, using pandas directly")
        
        # Test Excel processing
        for file_path in excel_files:
            logger.info(f"Processing Excel: {file_path}")
            
            # Read with pandas first
            try:
                df = pd.read_excel(file_path)
                logger.info(f"  - Excel rows: {len(df)}")
                logger.info(f"  - Excel columns: {list(df.columns)}")
                
                # Check for multiple rows
                if len(df) > 1:
                    logger.info(f"  - Multiple rows detected: {len(df)} rows")
                    # Check if first row is being used exclusively
                    logger.warning("  - Check if workflow is only using first row (df.iloc[0])")
                
                # Check date formats
                for col in df.columns:
                    if any(date_term in col.lower() for date_term in ['date', 'dob', 'expiry']):
                        sample_values = df[col].dropna().astype(str).tolist()[:3]
                        logger.info(f"  - Date column {col} values: {sample_values}")
                        
                        for val in sample_values:
                            if re.match(r'^\d{4}-\d{2}-\d{2}$', val):
                                logger.warning(f"  - Date format is YYYY-MM-DD, should be DD-MM-YYYY")
            except Exception as e:
                logger.error(f"  - Error reading Excel with pandas: {str(e)}")
            
            # Try with Excel processor if available
            if excel_processor:
                try:
                    logger.info("  - Using Excel processor...")
                    if hasattr(excel_processor, 'process_excel'):
                        result, errors = excel_processor.process_excel(file_path, dayfirst=True)
                        logger.info(f"  - Processor result: {len(result)} rows, {len(errors) if errors else 0} errors")
                        
                        # Check first row vs all rows
                        if len(result) > 1:
                            logger.info(f"  - Excel processor found {len(result)} rows")
                            logger.warning("  - Check if only first row is used in workflow (result.iloc[0])")
                    else:
                        logger.warning("  - Excel processor does not have process_excel method")
                except Exception as e:
                    logger.error(f"  - Error with Excel processor: {str(e)}")
                    logger.error(traceback.format_exc())
        
        return True
    except Exception as e:
        logger.error(f"Error in Excel processing diagnosis: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def diagnose_data_combination():
    """Diagnose issues with data combination."""
    print_separator("DATA COMBINATION DIAGNOSIS")
    
    try:
        # Try to import data combiner
        data_combiner = None
        try:
            from src.services.data_combiner import DataCombiner
            from src.document_processor.textract_processor import TextractProcessor
            # For Excel processor, try to find the right class
            excel_processor = None
            try:
                from src.document_processor.excel_processor import ExcelProcessor
                excel_processor = ExcelProcessor()
            except (ImportError, AttributeError):
                import src.document_processor.excel_processor as module
                for attr_name in dir(module):
                    if not attr_name.startswith('_') and attr_name.endswith('Processor'):
                        cls = getattr(module, attr_name)
                        excel_processor = cls()
                        break
            
            if excel_processor:
                data_combiner = DataCombiner(TextractProcessor(), excel_processor)
                logger.info("Created DataCombiner instance")
            else:
                logger.warning("Could not create Excel processor, skipping data combiner test")
                return False
        except Exception as e:
            logger.error(f"Error creating DataCombiner: {str(e)}")
            return False
        
        # Create a sample document data and Excel data
        extracted_data = {
            "first_name": "John",
            "last_name": "Doe",
            "passport_number": "AB123456",
            "nationality": "United States",
            "date_of_birth": "1990-01-01",  # Note YYYY-MM-DD format
            "passport_expiry_date": "2030-01-01"  # Note YYYY-MM-DD format
        }
        
        excel_data = {
            "first_name": "John",
            "last_name": "Doe",
            "nationality": "United States",
            "emirates_id": "123-4567-8901234-5",
            "date_of_birth": "1990-01-01",  # Note YYYY-MM-DD format
        }
        
        # Test combining data
        logger.info("Testing data combination...")
        if hasattr(data_combiner, 'combine_data'):
            combined = data_combiner.combine_data(extracted_data, excel_data)
            logger.info(f"Combined data: {combined}")
            
            # Check date formats in combined data
            for key, value in combined.items():
                if isinstance(value, str) and any(date_term in key for date_term in ['date', 'dob', 'expiry']):
                    logger.info(f"Date field {key}: {value}")
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                        logger.warning(f"Date format is YYYY-MM-DD, should be DD-MM-YYYY")
        else:
            logger.warning("DataCombiner does not have combine_data method")
        
        # Test template population
        template_path = None
        for possible_path in ["template.xlsx", os.path.join(project_root, "template.xlsx")]:
            if os.path.exists(possible_path):
                template_path = possible_path
                break
                
        if template_path and hasattr(data_combiner, 'combine_and_populate_template'):
            logger.info(f"Testing template population with {template_path}...")
            output_path = "diagnostic_output.xlsx"
            
            result = data_combiner.combine_and_populate_template(
                template_path,
                output_path,
                extracted_data,
                excel_data
            )
            
            logger.info(f"Template population result: {result}")
            
            # Check output file
            if os.path.exists(output_path):
                logger.info(f"Output file created: {output_path}")
                try:
                    df = pd.read_excel(output_path)
                    logger.info(f"Output has {len(df)} rows")
                    
                    # Check date formats
                    if not df.empty:
                        for col in df.columns:
                            if any(date_term in col.lower() for date_term in ['date', 'dob', 'expiry']):
                                val = str(df.iloc[0][col])
                                logger.info(f"Date field {col}: {val}")
                                if re.match(r'^\d{4}-\d{2}-\d{2}$', val):
                                    logger.warning(f"Date format is YYYY-MM-DD, should be DD-MM-YYYY")
                except Exception as e:
                    logger.error(f"Error checking output: {str(e)}")
            else:
                logger.warning(f"Output file not created: {output_path}")
        else:
            if not template_path:
                logger.warning("Template file not found")
            else:
                logger.warning("DataCombiner does not have combine_and_populate_template method")
        
        return True
    except Exception as e:
        logger.error(f"Error in data combination diagnosis: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def run_diagnosis():
    """Run all diagnostic tests."""
    print_separator("STARTING DIAGNOSIS")
    logger.info(f"Project root: {project_root}")
    
    results = {
        "Email Fetch": diagnose_email_fetch(),
        "Attachment Handling": diagnose_attachment_handling(),
        "Document Processing": diagnose_document_processing(),
        "Excel Processing": diagnose_excel_processing(),
        "Data Combination": diagnose_data_combination()
    }
    
    print_separator("DIAGNOSIS RESULTS")
    for test, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        logger.info(f"{test}: {status}")
    
    logger.info("Check diagnostic_log.txt for detailed information")

if __name__ == "__main__":
    run_diagnosis()