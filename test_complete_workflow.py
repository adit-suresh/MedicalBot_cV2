#!/usr/bin/env python3
# test_complete_workflow.py

import os
import sys
import logging
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Optional, List
import json
import argparse
import shutil
import time

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Import original components
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.deepseek_processor import DeepseekProcessor
from src.services.enhanced_document_processor import EnhancedDocumentProcessorService
from src.utils.file_sharer import FileSharer
from src.utils.teams_notifier import TeamsNotifier
from src.utils.email_sender import EmailSender
from src.document_processor.gpt_processor import GPTProcessor

# Import original workflow components
from src.utils.process_tracker import ProcessTracker
from src.services.data_combiner import DataCombiner
from src.document_processor.excel_processor import EnhancedExcelProcessor as ExcelProcessor

def is_email_processed(email_id):
    """Simple check if email has been processed."""
    processed_file = "processed_emails_simple.txt"
    
    # Check if file exists and contains the email ID
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            processed_ids = [line.strip() for line in f.readlines()]
            return email_id in processed_ids
    return False

def mark_email_processed(email_id):
    """Mark email as processed in simple tracking file."""
    processed_file = "processed_emails_simple.txt"
    
    # Append email ID to file
    with open(processed_file, 'a') as f:
        f.write(f"{email_id}\n")
    logger.info(f"Marked email {email_id} as processed")

# Create WorkflowTester class from the original file
class CompletedSubmission:
    def __init__(self, process_id: str, documents: Dict[str, str], final_excel: str):
        self.process_id = process_id
        self.documents = documents  # Map of document types to file paths
        self.final_excel = final_excel
        self.timestamp = datetime.now()
        self.status = 'completed'

class WorkflowTester:
    def __init__(self):
        # Initialize services
        self.outlook = OutlookClient()
        self.attachment_handler = AttachmentHandler()
        self.textract = TextractProcessor()
        self.DEFAULT_VALUE = "."  # Add this line
        try:
            self.gpt = GPTProcessor()  # Use GPT instead of DeepSeek
            logger.info("GPT processor initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize GPT: {str(e)}")
            self.gpt = None
            
        self.document_processor = EnhancedDocumentProcessorService(self.textract, self.gpt)
        self.file_sharer = FileSharer()
        
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract, self.excel_processor, self.gpt)
        self.process_tracker = ProcessTracker()
        self.teams_notifier = TeamsNotifier()
        self.email_sender = EmailSender()
        
        # Validate templates
        self._validate_templates()
        
        # Storage for completed submissions
        self.completed_submissions: List[CompletedSubmission] = []
        
        # Create necessary directories
        os.makedirs("processed_submissions", exist_ok=True)
        os.makedirs("templates", exist_ok=True)

        
    def _select_template_for_company(self, subject: str) -> str:
        """Select the appropriate template based on the email subject."""
        subject_lower = subject.lower()
        
        # Check for Al Madallah first
        if 'al madallah' in subject_lower or 'almadallah' in subject_lower:
            template = 'templates/al_madallah.xlsx'
            logger.info(f"Selected Al Madallah template: {template}")
            return template
            
        # Default to NAS template
        template = 'templates/nas.xlsx'
        logger.info(f"Selected NAS template (default): {template}")
        return template
    
    def _validate_templates(self):
        """Ensure all required templates exist."""
        templates = [
            'templates/nas.xlsx',
            'templates/al_madallah.xlsx',
            'templates/union.xlsx',
            'templates/al_sagar.xlsx',
            'templates/dic.xlsx',
            'templates/dni.xlsx',
            'templates/ngi.xlsx',
            'templates/qic.xlsx',
            'templates/orient.xlsx',
            'templates/takaful.xlsx'
        ]
        
        missing = []
        for template in templates:
            if not os.path.exists(template):
                missing.append(template)
        
        if missing:
            logger.warning(f"Missing templates: {', '.join(missing)}")
            logger.warning("Default template will be used for missing templates")
           
    def run_complete_workflow(self, bypass_dedup=False) -> Dict:
        """Run complete workflow from email to final Excel."""
        try:
            # Step 1: Fetch and process new emails
            logger.info("Step 1: Fetching emails...")
            emails = self.outlook.fetch_emails()
            logger.info(f"Fetched {len(emails)} emails before filtering")
            
            # Force deduplication
            if not bypass_dedup:
                emails = force_deduplication(emails)
                logger.info(f"After force deduplication: {len(emails)} emails remain")
            
            # Initialize email tracker
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            # Check tracker file integrity
            processed_ids = set()
            try:
                if os.path.exists("processed_emails.json"):
                    with open("processed_emails.json", "r") as f:
                        try:
                            processed_data = json.load(f)
                            for email_id in processed_data:
                                processed_ids.add(email_id)
                            logger.info(f"Loaded {len(processed_ids)} processed email IDs from tracking file")
                        except json.JSONDecodeError:
                            logger.error("Error parsing processed_emails.json, treating as empty")
                            # Create a backup of the corrupted file
                            import shutil
                            backup_name = f"processed_emails.json.corrupted.{int(time.time())}"
                            shutil.copy2("processed_emails.json", backup_name)
                            logger.info(f"Created backup of corrupted file: {backup_name}")
                            # Create a new empty file
                            with open("processed_emails.json", "w") as f_new:
                                f_new.write("{}")
            except Exception as e:
                logger.error(f"Error loading processed email IDs: {str(e)}")
            
            # Examine subjects for debugging
            subjects_found = [email.get('subject', '').strip() for email in emails]
            logger.info(f"Found emails with subjects: {subjects_found}")

            # Log each email we're going to process
            for idx, email in enumerate(emails, 1):
                email_id = email['id']
                subject = email.get('subject', '').strip()
                # Check both methods
                is_processed_tracker = email_tracker.is_processed(email_id)
                is_processed_direct = email_id in processed_ids
                
                if is_processed_tracker != is_processed_direct:
                    logger.warning(f"Inconsistent tracking state for email {email_id}: tracker={is_processed_tracker}, direct={is_processed_direct}")
                    
                is_processed = is_processed_tracker or is_processed_direct
                
                logger.info(f"Email {idx}: ID={email_id}, Subject={subject}")
                logger.info(f"  - Is processed (tracker): {is_processed_tracker}")
                logger.info(f"  - Is processed (direct): {is_processed_direct}")
                
                if not bypass_dedup and is_processed:
                    logger.info(f"  - Status: Will be skipped (already processed)")
                else:
                    logger.info(f"  - Status: Will be processed")
            
            # Check if --reset flag was used but didn't clear the tracker
            if bypass_dedup and len(processed_ids) > 0:
                logger.warning(f"Reset flag was used but {len(processed_ids)} processed emails still in tracker")
                
            results = []
            successful = 0
            skipped = 0
            failed = 0
            failed_emails = []  # Track details of failed emails
            
            for email in emails:
                try:
                    email_id = email['id']
                    subject = email.get('subject', '').strip()
                    
                    # Double-check processing status
                    is_processed_tracker = email_tracker.is_processed(email_id)
                    is_processed_direct = email_id in processed_ids
                    is_processed = is_processed_tracker or is_processed_direct
                    
                    # Skip if already processed (unless bypassing)
                    if not bypass_dedup and is_email_processed(email_id):
                        logger.info(f"Skipping already processed email: {subject}")
                        skipped += 1
                        continue
                        
                    # Process email
                    logger.info(f"Processing email: {subject}")
                    try:
                        result = self._process_single_email(email)
                        results.append(result)
                        
                        # Record result
                        if result['status'] == 'success':
                            logger.info(f"Successfully processed email: {subject}")
                            successful += 1
                            # Record as processed if successful
                            if not bypass_dedup:
                                mark_email_processed(email_id)
                                # Also update our direct tracking set
                                processed_ids.add(email_id)
                        else:
                            logger.error(f"Failed to process email: {subject}, Error: {result.get('error', 'Unknown error')}")
                            failed += 1
                            failed_emails.append({
                                'id': email_id,
                                'subject': subject,
                                'error': result.get('error', 'Unknown error'),
                                'received': email.get('receivedDateTime', 'Unknown')
                            })
                            
                    except Exception as e:
                        logger.error(f"Uncaught exception processing email {subject}: {str(e)}", exc_info=True)
                        failed += 1
                        failed_emails.append({
                            'id': email_id,
                            'subject': subject,
                            'error': str(e),
                            'received': email.get('receivedDateTime', 'Unknown')
                        })
                        
                except Exception as e:
                    logger.error(f"Error accessing email information: {str(e)}", exc_info=True)
                    failed += 1
                    failed_emails.append({
                        'id': email.get('id', 'Unknown'),
                        'subject': email.get('subject', 'Unknown'),
                        'error': str(e),
                        'received': email.get('receivedDateTime', 'Unknown')
                    })
                    continue
            
            # Save failed emails info to file for inspection
            if failed_emails:
                with open('failed_emails_debug.json', 'w') as f:
                    json.dump(failed_emails, f, indent=2, default=str)
                logger.info(f"Saved details of {len(failed_emails)} failed emails to failed_emails_debug.json")
            
            logger.info(f"Email processing summary:")
            logger.info(f"  - Total emails: {len(emails)}")
            logger.info(f"  - Successfully processed: {successful}")
            logger.info(f"  - Skipped (already processed): {skipped}")
            logger.info(f"  - Failed: {failed}")
            
            return {
                "status": "success",
                "emails_processed": len(emails),
                "successful": successful,
                "skipped": skipped,
                "failed": failed
            }

        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _process_single_email(self, email: Dict) -> Dict:
        """Process a single email submission with improved error handling."""
        email_id = email['id']
        # Clean subject for folder name
        subject = re.sub(r'[<>:"/\\|?*]', '', email.get('subject', 'No Subject'))
        process_id = f"{subject}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create submission directory
            submission_dir = os.path.join("processed_submissions", re.sub(r'[^a-zA-Z0-9]', '_', process_id)[:50])
            os.makedirs(submission_dir, exist_ok=True)
            
            # Step 2: Get and save attachments
            logger.info(f"Processing email {email_id} with subject: {subject}")
            try:
                attachments = self.outlook.get_attachments(email_id)
                logger.info(f"Retrieved {len(attachments)} attachments")
            except Exception as e:
                logger.error(f"Error getting attachments: {str(e)}", exc_info=True)
                raise Exception(f"Failed to get attachments: {str(e)}")
                    
            try:
                saved_files = self.attachment_handler.process_attachments(attachments, email_id)
                logger.info(f"Saved {len(saved_files)} attachment files")
            except Exception as e:
                logger.error(f"Error processing attachments: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process attachments: {str(e)}")
            
            if not saved_files:
                logger.warning(f"No valid attachments found for email {email_id}")
                raise Exception("No valid attachments found")

            # Step 3: Categorize and process files
            try:
                document_paths = {}
                excel_files = []
                processed_docs = []
                all_excel_rows = []  # Initialize this variable here

                # First pass: identify Excel files and documents
                for file_path in saved_files:
                    file_type = self._determine_file_type(file_path)
                    logger.debug(f"Determined file type for {file_path}: {file_type}")
                    
                    if file_type == 'excel':
                        excel_files.append(file_path)
                    else:
                        doc_path = os.path.join(submission_dir, os.path.basename(file_path))
                        shutil.copy2(file_path, doc_path)
                        document_paths[file_type] = doc_path
                        processed_docs.append({
                            'type': file_type,
                            'original_name': os.path.basename(file_path),
                            'path': doc_path
                        })
                
                logger.info(f"Categorized files: {len(excel_files)} Excel files, {len(document_paths)} documents")
            except Exception as e:
                logger.error(f"Error categorizing files: {str(e)}", exc_info=True)
                raise Exception(f"Failed to categorize files: {str(e)}")

            # Step 4: Process all documents
            try:
                logger.info(f"Processing {len(document_paths)} documents")
                extracted_data = {}
                
                # Process documents with GPT first if available
                if self.gpt:
                    for doc_type, file_path in document_paths.items():
                        try:
                            logger.info(f"Processing {doc_type} with GPT: {file_path}")
                            gpt_data = self.gpt.process_document(file_path, doc_type)
                            
                            # Check if GPT extraction was successful
                            if gpt_data and 'error' not in gpt_data:
                                logger.info(f"GPT successfully extracted data from {doc_type}")
                                # Update extracted data, giving priority to GPT results
                                for key, value in gpt_data.items():
                                    if key not in extracted_data or value != self.DEFAULT_VALUE:
                                        extracted_data[key] = value
                            else:
                                logger.warning(f"GPT extraction failed for {doc_type}, will try Textract")
                        except Exception as e:
                            logger.error(f"Error processing {doc_type} with GPT: {str(e)}", exc_info=True)
                            logger.warning(f"Will try Textract for {doc_type}")
                
                # Also try Textract for any missing fields or if GPT failed
                for doc_type, file_path in document_paths.items():
                    try:
                        logger.info(f"Processing {doc_type} with Textract: {file_path}")
                        textract_data = self.textract.process_document(file_path, doc_type)
                        
                        # Update extracted data, but don't overwrite GPT data with default values
                        for key, value in textract_data.items():
                            if key not in extracted_data or (value != self.DEFAULT_VALUE and extracted_data[key] == self.DEFAULT_VALUE):
                                extracted_data[key] = value
                        
                        logger.info(f"Extracted data from {doc_type}: {list(textract_data.keys())}")
                    except Exception as e:
                        logger.error(f"Error processing {doc_type} document with Textract: {str(e)}", exc_info=True)
                        # Continue with other documents instead of failing
                        logger.warning(f"Continuing with partial data due to document processing error")
                
                # Log the combined extracted data
                logger.info(f"Combined extracted data contains {len(extracted_data)} fields: {list(extracted_data.keys())}")
                
            except Exception as e:
                logger.error(f"Error processing documents: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process documents: {str(e)}")

            # Step 5: Process all Excel files
            try:
                logger.info(f"Processing {len(excel_files)} Excel files")
                all_excel_rows = []  # Initialize again to be safe
                excel_row_identifiers = {}  # Track identifiers for mapping documents to rows
                
                for excel_path in excel_files:
                    try:
                        df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                        if not df.empty:
                            # Process all rows
                            for idx, row in df.iterrows():
                                row_dict = row.to_dict()
                                all_excel_rows.append(row_dict)
                                
                                # Create identifier from staff ID, passport number, and/or name
                                identifier_parts = []
                                
                                # Try different field names for key fields
                                for field_name in ['staff_id', 'Staff ID', 'StaffID']:
                                    if field_name in row_dict and row_dict[field_name] not in [None, '', '.']:
                                        identifier_parts.append(str(row_dict[field_name]))
                                        break
                                        
                                for field_name in ['passport_no', 'Passport No', 'passport_number', 'Passport Number']:
                                    if field_name in row_dict and row_dict[field_name] not in [None, '', '.']:
                                        identifier_parts.append(str(row_dict[field_name]))
                                        break
                                        
                                for field_name in ['full_name', 'name', 'First Name', 'first_name']:
                                    if field_name in row_dict and row_dict[field_name] not in [None, '', '.']:
                                        identifier_parts.append(str(row_dict[field_name]))
                                        break
                                
                                row_id = "_".join(identifier_parts)
                                excel_row_identifiers[idx] = row_id
                            
                            logger.info(f"Processed Excel file with {len(df)} rows")
                        if errors:
                            logger.warning(f"Excel validation errors: {errors}")
                    except Exception as e:
                        logger.error(f"Error processing Excel file {excel_path}: {str(e)}", exc_info=True)
                        # Continue with other Excel files instead of failing
                        logger.warning(f"Continuing with partial data due to Excel processing error")
                
                logger.info(f"Extracted {len(all_excel_rows)} rows from Excel files")
            except Exception as e:
                logger.error(f"Error processing Excel files: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process Excel files: {str(e)}")

            # Match documents to specific employees
            if document_paths and all_excel_rows:
                logger.info(f"Attempting to match {len(document_paths)} documents to {len(all_excel_rows)} employees")
                
                # Process each document to extract data
                doc_data_by_type = {}
                for doc_type, file_path in document_paths.items():
                    try:
                        # Process with GPT first if available
                        if self.gpt:
                            try:
                                doc_data = self.gpt.process_document(file_path, doc_type)
                                if 'error' not in doc_data:
                                    doc_data_by_type[doc_type] = doc_data
                                    continue
                            except Exception as e:
                                logger.warning(f"GPT extraction failed for {doc_type}, using Textract: {str(e)}")
                        
                        # Fallback to Textract
                        doc_data = self.textract.process_document(file_path, doc_type)
                        doc_data_by_type[doc_type] = doc_data
                        
                    except Exception as e:
                        logger.error(f"Error processing {doc_type} document: {str(e)}")
                
                # Only do document matching if we have multiple rows
                if len(all_excel_rows) > 1:
                    # Extract name from documents for matching
                    extracted_names = []
                    for doc_type, doc_data in doc_data_by_type.items():
                        # Try different name fields
                        name = None
                        for field in ['full_name', 'name']:
                            if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                name = doc_data[field]
                                break
                        
                        # If no direct name field, try surname + given names
                        if not name and 'surname' in doc_data and 'given_names' in doc_data:
                            if doc_data['surname'] != self.DEFAULT_VALUE and doc_data['given_names'] != self.DEFAULT_VALUE:
                                name = f"{doc_data['given_names']} {doc_data['surname']}"
                        
                        if name:
                            extracted_names.append(name)
                            logger.info(f"Found name in {doc_type}: {name}")
                    
                    # If we found names in documents, try to match
                    if extracted_names:
                        # Map to track which rows matched
                        matched_rows = []
                        matched_indices = set()
                        unmatched_rows = []
                        
                        # For each Excel row, try to find a matching document name
                        for idx, row in enumerate(all_excel_rows):
                            # Get employee name from Excel
                            first_name = str(row.get('First Name', '')).strip()
                            last_name = str(row.get('Last Name', '')).strip()
                            employee_name = f"{first_name} {last_name}".strip()
                            
                            logger.info(f"Row {idx} employee name from Excel: '{employee_name}', First: '{first_name}', Last: '{last_name}'")
                            
                            # Log all document names for debugging
                            logger.info(f"Document names available for matching: {extracted_names}")
                            
                            best_match = None
                            best_score = 0
                            
                            # Compare with each extracted name
                            for doc_name in extracted_names:
                                # Calculate similarity
                                doc_parts = set(doc_name.lower().split())
                                emp_parts = set(employee_name.lower().split())
                                common_parts = doc_parts.intersection(emp_parts)
                                
                                if common_parts:
                                    score = len(common_parts) / max(len(doc_parts), len(emp_parts))
                                    if score > best_score:
                                        best_score = score
                                        best_match = doc_name
                            
                            # If good match found (threshold 0.3)
                            if best_match and best_score > 0.3:
                                logger.info(f"Row {idx} matched to '{best_match}' with score {best_score:.2f}")
                                
                                # Create combined row with document data - PRESERVE ORIGINAL NAMES FIRST
                                combined_row = row.copy()
                                
                                # Store original names to ensure they don't get overwritten unless explicitly matched
                                original_first_name = combined_row.get('First Name', '')
                                original_last_name = combined_row.get('Last Name', '')
                                
                                logger.info(f"Original row data before modification: Last Name: '{original_last_name}', First Name: '{original_first_name}'")
                                
                                # Add document data to this row
                                for doc_type, doc_data in doc_data_by_type.items():
                                    # Only apply data if it relates to the matched name
                                    doc_name = None
                                    for field in ['full_name', 'name']:
                                        if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                            doc_name = doc_data[field]
                                            break
                                    
                                    if not doc_name and 'surname' in doc_data and 'given_names' in doc_data:
                                        if doc_data['surname'] != self.DEFAULT_VALUE and doc_data['given_names'] != self.DEFAULT_VALUE:
                                            doc_name = f"{doc_data['given_names']} {doc_data['surname']}"
                                    
                                    # If this document matches the current best match
                                    if doc_name and doc_name.lower() == best_match.lower():
                                        # Apply document data to this row
                                        for key, value in doc_data.items():
                                            if value != self.DEFAULT_VALUE:
                                                # Special handling for name fields - BE MORE CAREFUL HERE
                                                if key == 'full_name':
                                                    # SKIP updating names if both first and last name already exist and are not empty
                                                    if original_first_name and original_last_name:
                                                        logger.info(f"Skipping full_name update as original names exist: {original_first_name} {original_last_name}")
                                                        continue
                                                        
                                                    # Only update name if it's substantially different
                                                    if len(common_parts) / len(doc_parts) < 0.8:
                                                        # Try to split full name into parts
                                                        parts = value.split()
                                                        if len(parts) >= 2:
                                                            # Only update if original is empty
                                                            if not original_first_name:
                                                                combined_row['First Name'] = parts[0]
                                                            if not original_last_name:
                                                                combined_row['Last Name'] = parts[-1]
                                                            if len(parts) > 2 and 'Middle Name' in combined_row:
                                                                combined_row['Middle Name'] = ' '.join(parts[1:-1])
                                                elif key == 'surname':
                                                    # CRITICAL FIX: Only update surname if the original last name is empty
                                                    if not original_last_name:
                                                        logger.info(f"Updating Last Name from empty to '{value}'")
                                                        combined_row['Last Name'] = value
                                                    else:
                                                        logger.info(f"Preserving original Last Name: '{original_last_name}' (ignoring '{value}')")
                                                elif key == 'given_names':
                                                    parts = value.split()
                                                    if parts:
                                                        # Only update if original is empty
                                                        if not original_first_name:
                                                            combined_row['First Name'] = parts[0]
                                                        if len(parts) > 1 and 'Middle Name' in combined_row:
                                                            combined_row['Middle Name'] = ' '.join(parts[1:])
                                                # Other fields mapping - THESE ARE FINE
                                                elif key == 'passport_number':
                                                    combined_row['Passport No'] = value
                                                elif key == 'date_of_birth':
                                                    combined_row['DOB'] = value
                                                elif key == 'nationality':
                                                    combined_row['Nationality'] = value
                                                elif key == 'gender' or key == 'sex':
                                                    # Standardize gender format
                                                    if value.upper() in ['M', 'MALE']:
                                                        combined_row['Gender'] = 'Male'
                                                    elif value.upper() in ['F', 'FEMALE']:
                                                        combined_row['Gender'] = 'Female'
                                                elif key == 'unified_no':
                                                    combined_row['Unified No'] = value
                                                elif key == 'visa_file_number' or key == 'entry_permit_no':
                                                    combined_row['Visa File Number'] = value
                                
                                # Verify names after processing
                                logger.info(f"Final row data after modification: Last Name: '{combined_row.get('Last Name', '')}', First Name: '{combined_row.get('First Name', '')}'")
                                
                                matched_rows.append(combined_row)
                                matched_indices.add(idx)
                                logger.info(f"Matched row {idx}: {employee_name} to document name: {best_match}")
                            else:
                                # Keep row as is if no good match
                                unmatched_rows.append(row)
                                logger.info(f"No good match for row {idx}: {employee_name}")
                        
                        # Combine matched and unmatched rows
                        processed_rows = matched_rows + unmatched_rows
                        
                        # Only use processed rows if we successfully matched at least one row
                        if matched_rows:
                            logger.info(f"Successfully matched {len(matched_rows)} rows out of {len(all_excel_rows)}")
                            all_excel_rows = processed_rows
                        else:
                            logger.warning("No rows successfully matched with documents, using original Excel data")
                    else:
                        logger.warning("No names found in documents for matching")

                # Apply document data to all rows if we only have one row or couldn't match
                else:
                    logger.info("Only one Excel row or no matching performed, applying document data directly")
                    # Combine all document data
                    combined_doc_data = {}
                    for doc_data in doc_data_by_type.values():
                        for key, value in doc_data.items():
                            if value != self.DEFAULT_VALUE:
                                combined_doc_data[key] = value
                    
                    # Apply to each row
                    for i, row in enumerate(all_excel_rows):
                        # Only apply non-name fields to avoid overwriting names
                        for key, value in combined_doc_data.items():
                            # Skip name fields
                            if key not in ['full_name', 'name', 'surname', 'given_names']:
                                # Apply mappings for other fields
                                if key == 'passport_number':
                                    row['Passport No'] = value
                                elif key == 'date_of_birth':
                                    row['DOB'] = value
                                elif key == 'nationality':
                                    row['Nationality'] = value
                                elif key == 'gender' or key == 'sex':
                                    # Standardize gender format
                                    if value.upper() in ['M', 'MALE']:
                                        row['Gender'] = 'Male'
                                    elif value.upper() in ['F', 'FEMALE']:
                                        row['Gender'] = 'Female'
                                elif key == 'unified_no':
                                    row['Unified No'] = value
                                elif key == 'visa_file_number' or key == 'entry_permit_no':
                                    row['Visa File Number'] = value
                
                # Extract common data for use in other parts of the workflow
                extracted_data = {}
                for doc_data in doc_data_by_type.values():
                    for key, value in doc_data.items():
                        if value != self.DEFAULT_VALUE:
                            extracted_data[key] = value

            # Rename client files based on Excel data
            if all_excel_rows:
                try:
                    document_paths = self._rename_client_files(document_paths, all_excel_rows)
                except Exception as e:
                    logger.error(f"Error renaming client files: {str(e)}", exc_info=True)
                    # Continue without failing the whole process
                    logger.warning("Continuing with original filenames")

            # Step 6: Select template based on company in subject
            template_path = self._select_template_for_company(subject)
                
            # Step 7: Combine data
            try:
                logger.info(f"Combining data using template: {template_path}")
                output_path = os.path.join(
                    submission_dir,
                    f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )

                # Use selected template instead of hardcoded "template.xlsx"
                result = self.data_combiner.combine_and_populate_template(
                    template_path,
                    output_path,
                    extracted_data,  # This now contains combined GPT and Textract data
                    all_excel_rows if all_excel_rows else None,
                    document_paths
                )
                
                logger.info(f"Data combination result: {result['status']}, rows processed: {result.get('rows_processed', 0)}")
            except Exception as e:
                logger.error(f"Error combining data: {str(e)}", exc_info=True)
                raise Exception(f"Failed to combine data: {str(e)}")


            # Step 8: Save submission
            try:
                submission = CompletedSubmission(
                    process_id=process_id,
                    documents=document_paths,
                    final_excel=output_path
                )
                self.completed_submissions.append(submission)
                
                # Try creating and sending a ZIP file
                try:
                    zip_path = self._create_zip(submission_dir)
                    
                    # Create message body with local file path
                    email_body = (
                        f"New submission processed: {subject}\n\n"
                        f"Please find the attached ZIP file containing all processed documents.\n\n"
                        f"Local file location: {zip_path}\n"
                    )
                    
                    # Send email with attachment
                    email_sent = self.email_sender.send_email(
                        subject=f"Medical Bot: {subject} - Submission Complete",
                        body=email_body,
                        attachment_path=zip_path
                    )
                    
                    if email_sent:
                        logger.info(f"Email sent with submission ZIP: {zip_path}")
                        logger.info(f"Files are also available locally at: {zip_path}")
                        print(f"\n✅ SUBMISSION COMPLETE!\nProcessed files are available at:\n{zip_path}\n")
                    else:
                        logger.error("Failed to send email with submission")
                        logger.info(f"Files are available locally at: {zip_path}")
                        print(f"\n⚠️ Email sending failed, but files are available at:\n{zip_path}\n")
                        
                        # Try sending without attachment as fallback
                        logger.warning("Trying to send email without attachment")
                        fallback_body = (
                            f"New submission processed: {subject}\n\n"
                            f"The attachment was too large to send via email.\n"
                            f"Files are available locally at: {zip_path}\n"
                        )
                        
                        fallback_sent = self.email_sender.send_email(
                            subject=f"Medical Bot: {subject} - Submission Complete (No Attachment)",
                            body=fallback_body
                        )
                        
                        if fallback_sent:
                            logger.info("Successfully sent email without attachment")
                            
                except Exception as e:
                    logger.error(f"Error creating or sending submission: {str(e)}", exc_info=True)      
            except Exception as e:
                logger.error(f"Error saving submission: {str(e)}", exc_info=True)
                # Continue without failing the whole process
                logger.warning("Continuing despite submission saving error")

            # Copy all files to submission directory for reference
            try:
                for excel_path in excel_files:
                    new_path = os.path.join(submission_dir, os.path.basename(excel_path))
                    shutil.copy2(excel_path, new_path)
            except Exception as e:
                logger.error(f"Error copying files to submission directory: {str(e)}", exc_info=True)
                # Continue without failing the whole process
                logger.warning("Continuing despite file copying error")

            return {
                "status": "success",
                "process_id": process_id,
                "submission_dir": submission_dir,
                "documents": document_paths,
                "excel_files": excel_files,
                "documents_processed": processed_docs,
                "rows_processed": len(all_excel_rows) if all_excel_rows else 0
            }

        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "process_id": process_id,
                "error": str(e)
            }
    
    def _rename_client_files(self, document_paths: Dict[str, str], excel_data: List[Dict]) -> Dict[str, str]:
        """Rename files based on staff ID from Excel with improved matching."""
        if not excel_data:
            logger.warning("No Excel data provided, cannot rename files")
            return document_paths
            
        try:
            # Build a map of staff ID to name for reference
            staff_id_map = {}
            for row in excel_data:
                # Check both lowercase and proper case field names
                staff_id = None
                # Try different field name variations for staff ID
                for field_name in ['staff_id', 'Staff ID', 'StaffID', 'staff id']:
                    if field_name in row and row[field_name]:
                        val = str(row.get(field_name, '')).strip()
                        if val and val != '.':
                            staff_id = val
                            break
                            
                if staff_id:
                    # Try different field name variations for first name
                    first_name = ''
                    for field_name in ['first_name', 'First Name', 'firstname', 'FirstName']:
                        if field_name in row:
                            val = str(row.get(field_name, '')).strip()
                            if val and val != '.':
                                first_name = val
                                break
                                
                    # Try different field name variations for last name
                    last_name = ''
                    for field_name in ['last_name', 'Last Name', 'lastname', 'LastName']:
                        if field_name in row:
                            val = str(row.get(field_name, '')).strip()
                            if val and val != '.':
                                last_name = val
                                break
                                
                    if first_name or last_name:
                        staff_id_map[staff_id] = {
                            'first_name': first_name,
                            'last_name': last_name,
                            'full_name': f"{first_name} {last_name}".strip()
                        }
                        
            logger.info(f"Found {len(staff_id_map)} staff IDs in Excel data")
            
            # Create a copy of document paths to update
            updated_paths = {}
            
            # Create direct one-to-one mapping if appropriate
            if len(document_paths) == len(excel_data) and len(document_paths) > 0:
                logger.info(f"Equal number of documents ({len(document_paths)}) and rows ({len(excel_data)}), using direct mapping")
                sorted_docs = sorted(document_paths.items())
                sorted_excel = sorted(excel_data, key=lambda x: x.get('staff_id', ''))
                
                for i, ((doc_type, file_path), row_data) in enumerate(zip(sorted_docs, sorted_excel)):
                    staff_id = str(row_data.get('staff_id', '')).strip()
                    if not staff_id or staff_id == '.':
                        logger.warning(f"Missing staff_id in row {i+1}, using row index")
                        staff_id = f"UNKNOWN_{i+1}"
                    
                    file_dir = os.path.dirname(file_path)
                    file_ext = os.path.splitext(file_path)[1]
                    
                    # Determine document type suffix
                    if 'passport' in doc_type.lower():
                        doc_suffix = "PASSPORT"
                    elif 'emirates' in doc_type.lower() or 'eid' in doc_type.lower():
                        doc_suffix = "EMIRATES_ID"
                    elif 'visa' in doc_type.lower() or 'permit' in doc_type.lower():
                        doc_suffix = "VISA"
                    else:
                        doc_suffix = doc_type.upper().replace(' ', '_')
                    
                    # Create new filename with staff ID and doc type
                    safe_id = re.sub(r'[<>:"/\\|?*]', '_', staff_id)
                    new_name = f"{safe_id}_{doc_suffix}{file_ext}"
                    new_path = os.path.join(file_dir, new_name)
                    
                    # Handle potential conflicts
                    if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(file_path):
                        import uuid
                        unique = str(uuid.uuid4())[:8]
                        new_name = f"{safe_id}_{doc_suffix}_{unique}{file_ext}"
                        new_path = os.path.join(file_dir, new_name)
                    
                    # Rename file
                    os.rename(file_path, new_path)
                    logger.info(f"Renamed file: {os.path.basename(file_path)} -> {new_name}")
                    updated_paths[doc_type] = new_path
                
                return updated_paths
            
            # Process each document using the regular matching approach
            for doc_type, file_path in document_paths.items():
                try:
                    # Skip if file doesn't exist
                    if not os.path.exists(file_path):
                        logger.warning(f"File not found: {file_path}")
                        updated_paths[doc_type] = file_path
                        continue
                        
                    # Extract file info
                    file_dir = os.path.dirname(file_path)
                    file_ext = os.path.splitext(file_path)[1]
                    file_name = os.path.basename(file_path).lower()
                    
                    # Find matching staff ID
                    matched_staff_id = None
                    
                    # Strategy 1: Check if staff ID is already in the filename
                    for staff_id in staff_id_map:
                        if staff_id.lower() in file_name:
                            matched_staff_id = staff_id
                            logger.info(f"Found staff ID {staff_id} in filename {file_name}")
                            break
                    
                    # Strategy 2: Try to match by name if no staff ID match
                    if not matched_staff_id:
                        best_match = None
                        best_score = 0
                        
                        for staff_id, info in staff_id_map.items():
                            score = 0
                            
                            # Check for first name match
                            if info['first_name'] and info['first_name'].lower() in file_name:
                                score += 5
                                
                            # Check for last name match (weighted higher)
                            if info['last_name'] and info['last_name'].lower() in file_name:
                                score += 10
                                
                            # Check for full name match
                            if info['full_name'] and info['full_name'].lower() in file_name:
                                score += 15
                                
                            # Update best match if better score
                            if score > best_score:
                                best_score = score
                                best_match = staff_id
                        
                        # Use match if score is high enough
                        if best_match and best_score >= 5:
                            matched_staff_id = best_match
                            logger.info(f"Matched file {file_name} to staff ID {matched_staff_id} by name (score: {best_score})")
                    
                    # If no match found, try the first staff ID as a fallback
                    if not matched_staff_id and staff_id_map:
                        matched_staff_id = list(staff_id_map.keys())[0]
                        logger.warning(f"No match found for {file_name}, using first staff ID: {matched_staff_id}")
                    
                    # If no staff ID available, keep original name
                    if not matched_staff_id:
                        logger.warning(f"No staff ID available, keeping original name for {file_name}")
                        updated_paths[doc_type] = file_path
                        continue
                    
                    # Determine document type suffix
                    if 'passport' in doc_type.lower():
                        doc_suffix = "PASSPORT"
                    elif 'emirates' in doc_type.lower() or 'eid' in doc_type.lower():
                        doc_suffix = "EMIRATES_ID"
                    elif 'visa' in doc_type.lower() or 'permit' in doc_type.lower():
                        doc_suffix = "VISA"
                    else:
                        # Use doc_type as is for other document types
                        doc_suffix = doc_type.upper()
                    
                    # Create new filename with staff ID and doc type
                    import re
                    safe_id = re.sub(r'[<>:"/\\|?*]', '_', matched_staff_id)
                    new_name = f"{safe_id}_{doc_suffix}{file_ext}"
                    new_path = os.path.join(file_dir, new_name)
                    
                    # Ensure no filename conflicts
                    if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(file_path):
                        import uuid
                        unique = str(uuid.uuid4())[:8]
                        new_name = f"{safe_id}_{doc_suffix}_{unique}{file_ext}"
                        new_path = os.path.join(file_dir, new_name)
                    
                    # Rename file
                    os.rename(file_path, new_path)
                    logger.info(f"Renamed file: {os.path.basename(file_path)} -> {new_name}")
                    
                    # Update path in result
                    updated_paths[doc_type] = new_path
                    
                except Exception as e:
                    logger.error(f"Error renaming file {file_path}: {str(e)}")
                    updated_paths[doc_type] = file_path
            
            return updated_paths
            
        except Exception as e:
            logger.error(f"Error in rename_client_files: {str(e)}")
            return document_paths
    
    def _create_zip(self, folder_path: str) -> str:
        """Create a ZIP file from a folder.
        
        Args:
            folder_path: Path to the folder to zip
            
        Returns:
            Path to the created ZIP file
        """
        import zipfile
        
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")
            
        # Create ZIP filename
        folder_basename = os.path.basename(folder_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_name = f"{folder_basename}_{timestamp}.zip"
        
        # Create ZIP file path
        zip_path = os.path.join(os.path.dirname(folder_path), zip_name)
        
        # Create ZIP file
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Walk through all files in the directory
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    # Calculate path inside the ZIP file
                    rel_path = os.path.relpath(file_path, os.path.dirname(folder_path))
                    
                    # Add file to ZIP
                    zipf.write(file_path, rel_path)
                    
        logger.info(f"Created ZIP file: {zip_path}")
        return zip_path
            
    def _validate_output_excel(self, excel_path: str) -> Dict:
        """Validate the output Excel file."""
        try:
            df = pd.read_excel(excel_path)
            if df.empty:
                return {
                    "is_valid": False,
                    "issues": ["Output file is empty"]
                }

            issues = []
            
            # Check required columns have data
            required_fields = [
                'first_name', 'last_name', 'nationality', 
                'emirates_id', 'passport_no', 'date_of_birth'
            ]
            
            for field in required_fields:
                if field not in df.columns:
                    issues.append(f"Missing required column: {field}")
                elif df[field].iloc[0] in ['.', 'nan', '']:
                    issues.append(f"Missing required data in {field}")

            # Validate data formats
            first_row = df.iloc[0]
            
            # Emirates ID format
            eid = str(first_row.get('emirates_id', ''))
            if not eid.startswith('.'):
                if not re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', eid):
                    issues.append("Invalid Emirates ID format")

            # Date format
            date_fields = ['date_of_birth', 'passport_expiry_date', 'visa_expiry_date']
            for field in date_fields:
                if field in df.columns:
                    date_val = str(first_row.get(field, ''))
                    if date_val not in ['.', 'nan', '']:
                        try:
                            # Check if date is in DD-MM-YYYY format
                            if not re.match(r'^\d{2}-\d{2}-\d{4}$', date_val):
                                issues.append(f"Invalid date format in {field}: {date_val}")
                        except ValueError:
                            issues.append(f"Invalid date format in {field}")

            return {
                "is_valid": len(issues) == 0,
                "issues": issues
            }

        except Exception as e:
            return {
                "is_valid": False,
                "issues": [f"Validation error: {str(e)}"]
            }

    def _determine_file_type(self, file_path: str) -> str:
        """Determine file type from filename and content if possible."""
        name = file_path.lower()
        
        # First check extensions for common file types
        if name.endswith(('.xlsx', '.xls')):
            return 'excel'
            
        # If content-based detection is available, use it here
        # For now, fallback to filename-based classification with improvements
        if 'passport' in name:
            return 'passport'
        elif 'emirates' in name or 'eid' in name or 'id card' in name:
            return 'emirates_id'
        elif 'visa' in name or 'permit' in name or 'residence' in name:
            return 'visa'
            
        # Try additional checks for content if unable to determine from filename
        # For example, check file size, basic content patterns, etc.
        
        return 'unknown'
    
    def get_completed_submissions(self) -> List[CompletedSubmission]:
        """Get list of completed submissions."""
        return self.completed_submissions
    
    def check_data_transfer(self, extracted_data, output_path):
        """Check if extracted data was properly transferred to Excel."""
        logger.info(f"Checking data transfer to Excel file: {output_path}")
        
        if not os.path.exists(output_path):
            logger.error(f"Output file does not exist: {output_path}")
            return False
            
        try:
            df = pd.read_excel(output_path)
            
            # Print key extracted fields
            logger.info("Key extracted fields:")
            key_fields = ['unified_no', 'nationality', 'passport_no', 'passport_number', 
                        'visa_file_number', 'dob', 'date_of_birth']
            
            extracted_values = {}
            for field in key_fields:
                if field in extracted_data:
                    value = extracted_data[field]
                    if value != '.':
                        extracted_values[field] = value
                        logger.info(f"  Extracted {field}: {value}")
                        
            # Print corresponding values in Excel
            logger.info("Corresponding fields in Excel:")
            excel_values = {}
            for field in key_fields:
                excel_field = field
                # Handle field name variants
                if field == 'passport_number':
                    excel_field = 'passport_no'
                elif field == 'date_of_birth':
                    excel_field = 'dob'
                    
                if excel_field in df.columns:
                    value = df[excel_field].iloc[0]
                    if pd.notna(value) and str(value).strip() != '':
                        excel_values[excel_field] = value
                        logger.info(f"  Excel {excel_field}: {value}")
            
            # Check if fields match
            for ext_field, ext_value in extracted_values.items():
                excel_field = ext_field
                if ext_field == 'passport_number':
                    excel_field = 'passport_no'
                elif ext_field == 'date_of_birth':
                    excel_field = 'dob'
                    
                if excel_field in excel_values:
                    excel_value = str(excel_values[excel_field])
                    ext_value = str(ext_value)
                    
                    if excel_value != ext_value:
                        logger.warning(f"Mismatch for {ext_field}: Extracted={ext_value}, Excel={excel_value}")
            
            return True
        except Exception as e:
            logger.error(f"Error checking data transfer: {str(e)}")
            return False
    
    def _match_documents_to_employees(self, document_paths: Dict[str, str], all_excel_rows: List[Dict]) -> Dict[int, Dict[str, str]]:
        """Match documents to specific employees based on name matching."""
        if not document_paths or not all_excel_rows:
            return {}
            
        # Extract employee names from Excel
        employee_names = []
        for idx, row in enumerate(all_excel_rows):
            first_name = str(row.get('First Name', '')).strip()
            last_name = str(row.get('Last Name', '')).strip()
            full_name = f"{first_name} {last_name}".strip()
            employee_names.append({
                'index': idx,
                'first_name': first_name,
                'last_name': last_name,
                'full_name': full_name
            })
        
        # For each document, try to extract name and match to employee
        document_matches = {}
        for doc_type, file_path in document_paths.items():
            try:
                # Process document to extract data
                doc_data = self.textract.process_document(file_path, doc_type)
                
                # Look for name in extracted data
                extracted_name = None
                for field in ['name', 'full_name']:
                    if field in doc_data and doc_data[field] != '.':
                        extracted_name = doc_data[field]
                        break
                
                if not extracted_name:
                    continue
                    
                # Find best employee match
                best_match = None
                best_score = 0
                for emp in employee_names:
                    score = self._calculate_name_match_score(extracted_name, emp['full_name'])
                    if score > best_score:
                        best_score = score
                        best_match = emp
                
                # If good match found, assign document to employee
                if best_match and best_score > 50:
                    idx = best_match['index']
                    if idx not in document_matches:
                        document_matches[idx] = {}
                    document_matches[idx][doc_type] = file_path
                    logger.info(f"Matched {doc_type} to employee {best_match['full_name']} (score: {best_score})")
                    
            except Exception as e:
                logger.error(f"Error matching document {doc_type}: {str(e)}")
        
        return document_matches

    def _calculate_name_match_score(self, name1: str, name2: str) -> int:
        """Calculate similarity score between two names."""
        if not name1 or not name2:
            return 0
            
        # Normalize names
        name1 = name1.lower().strip()
        name2 = name2.lower().strip()
        
        # Exact match
        if name1 == name2:
            return 100
            
        # Split into parts
        parts1 = set(name1.split())
        parts2 = set(name2.split())
        
        # Calculate intersection
        common_parts = parts1.intersection(parts2)
        
        # Calculate score based on common parts
        if not common_parts:
            return 0
            
        return int(100 * len(common_parts) / max(len(parts1), len(parts2)))
        
    def debug_data_flow(self, extracted_data, excel_data, document_paths, final_excel_path):
        """Create detailed debug report of data flow from extraction to final Excel."""
        try:
            debug_dir = "data_flow_debug"
            os.makedirs(debug_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            debug_file = os.path.join(debug_dir, f"data_flow_{timestamp}.txt")
            
            with open(debug_file, 'w') as f:
                # Document paths section
                f.write("=" * 80 + "\n")
                f.write("DOCUMENT PATHS:\n")
                f.write("=" * 80 + "\n")
                for doc_type, path in document_paths.items():
                    f.write(f"{doc_type}: {path}\n")
                f.write("\n\n")
                
                # Extracted data section
                f.write("=" * 80 + "\n")
                f.write("EXTRACTED DATA FROM DOCUMENTS:\n")
                f.write("=" * 80 + "\n")
                for key, value in sorted(extracted_data.items()):
                    f.write(f"{key}: {value}\n")
                f.write("\n\n")
                
                # Excel data section
                f.write("=" * 80 + "\n")
                f.write("INPUT EXCEL DATA:\n")
                f.write("=" * 80 + "\n")
                if isinstance(excel_data, list):
                    for i, row in enumerate(excel_data):
                        f.write(f"ROW {i+1}:\n")
                        for key, value in sorted(row.items()):
                            f.write(f"  {key}: {value}\n")
                        f.write("\n")
                elif isinstance(excel_data, dict):
                    for key, value in sorted(excel_data.items()):
                        f.write(f"{key}: {value}\n")
                f.write("\n\n")
                
                # Final Excel output
                f.write("=" * 80 + "\n")
                f.write("FINAL EXCEL OUTPUT:\n")
                f.write("=" * 80 + "\n")
                try:
                    df = pd.read_excel(final_excel_path)
                    for i, row in df.iterrows():
                        f.write(f"ROW {i+1}:\n")
                        for col in df.columns:
                            f.write(f"  {col}: {row[col]}\n")
                        f.write("\n")
                except Exception as e:
                    f.write(f"Error reading final Excel: {str(e)}\n")
                
            logger.info(f"Created data flow debug report: {debug_file}")
            return debug_file
        except Exception as e:
            logger.error(f"Error creating debug report: {str(e)}")
            return None
        
    def _combine_data_for_row(self, row_data, extracted_data, document_matches):
        """Combine Excel row data with extracted document data."""
        combined = row_data.copy()
        
        # Priority fields from different document types
        passport_fields = ['passport_number', 'surname', 'given_names', 'nationality', 'date_of_birth', 'sex', 'gender']
        visa_fields = ['entry_permit_no', 'unified_no', 'visa_file_number', 'sponsor_name', 'profession']
        
        # Apply document data based on priority
        for field, value in extracted_data.items():
            if value == self.DEFAULT_VALUE:
                continue
                
            # Handle passport fields with highest priority
            if field in passport_fields:
                # Map to Excel column names
                if field == 'passport_number':
                    combined['passport_no'] = value
                    combined['Passport No'] = value
                elif field == 'surname':
                    combined['last_name'] = value
                    combined['Last Name'] = value
                elif field == 'given_names':
                    combined['first_name'] = value
                    combined['First Name'] = value
                elif field == 'nationality':
                    combined['nationality'] = value
                    combined['Nationality'] = value
                elif field == 'date_of_birth':
                    combined['dob'] = value
                    combined['DOB'] = value
                elif field in ['gender', 'sex']:
                    combined['Gender'] = value
            
            # Handle visa fields
            elif field in visa_fields:
                if field == 'entry_permit_no' or field == 'visa_file_number':
                    combined['visa_file_number'] = value
                    combined['Visa File Number'] = value
                elif field == 'unified_no':
                    combined['unified_no'] = value
                    combined['Unified No'] = value
                elif field == 'sponsor_name':
                    combined['sponsor'] = value
                    combined['Sponsor'] = value
                elif field == 'profession':
                    combined['profession'] = value
                    combined['Profession'] = value
                    combined['Occupation'] = value
        
        return combined

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_separator():
    """Print a separator line for better readability."""
    logger.info("=" * 80)

def run_test(reset=False):
    """Run the workflow test."""
    print_separator()
    logger.info("STARTING WORKFLOW TEST")
    print_separator()
    
    # Initialize workflow tester
    tester = WorkflowTester()
    
    # Reset email tracker if requested
    if reset:
        from src.email_tracker.email_tracker import EmailTracker
        tracker = EmailTracker()
        if tracker.reset_tracker():
            logger.info("Successfully reset email tracker")
        else:
            logger.warning("Failed to reset email tracker")
    
    # Run workflow
    result = tester.run_complete_workflow(bypass_dedup=reset)
    
    # Print results
    print_separator()
    logger.info("WORKFLOW TEST COMPLETED")
    
    if result['status'] == 'success':
        logger.info(f"Successfully processed {result['successful']} out of {result['emails_processed']} emails")
        
        # Show completed submissions
        submissions = tester.get_completed_submissions()
        for sub in submissions:
            logger.info(f"\nSubmission {sub.process_id}:")
            logger.info(f"Status: {sub.status}")
            logger.info(f"Timestamp: {sub.timestamp}")
            logger.info(f"Documents: {list(sub.documents.keys())}")
            logger.info(f"Final Excel: {sub.final_excel}")
    else:
        logger.error(f"Workflow failed: {result.get('error')}")

def validate_excel_file(file_path: str) -> Dict:
    """
    Validate an output Excel file.
    
    Args:
        file_path: Path to Excel file
        
    Returns:
        Validation result dictionary
    """
    try:
        logger.info(f"Validating {file_path}")
        df = pd.read_excel(file_path)
        
        if df.empty:
            return {
                'file': os.path.basename(file_path),
                'is_valid': False,
                'row_count': 0,
                'issues': ["File is empty"]
            }
            
        issues = []
        
        # Check required columns
        required_fields = [
            'first_name', 'last_name', 'nationality', 
            'passport_no', 'dob', 'gender'
        ]
        
        missing_columns = [field for field in required_fields if field not in df.columns]
        if missing_columns:
            issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            
        # Check for empty values in required fields
        for field in [f for f in required_fields if f in df.columns]:
            empty_rows = df[df[field].isin(['', '.', 'nan'])].index.tolist()
            if empty_rows:
                if len(empty_rows) <= 3:  # Show only first few for readability
                    issues.append(f"Empty values in {field} at rows: {[i+2 for i in empty_rows]}")  # +2 for Excel row numbers (header + 1-indexing)
                else:
                    issues.append(f"Empty values in {field} at {len(empty_rows)} rows")
                    
        # Validate date formats
        date_fields = ['dob', 'passport_expiry_date', 'visa_expiry_date']
        for field in [f for f in date_fields if f in df.columns]:
            # Check if dates are in DD-MM-YYYY format
            invalid_format = []
            for idx, val in enumerate(df[field]):
                if pd.notna(val) and val not in ['.', '']:
                    if not re.match(r'^\d{2}-\d{2}-\d{4}$', str(val)):
                        invalid_format.append(idx)
                        
            if invalid_format:
                if len(invalid_format) <= 3:
                    issues.append(f"Invalid date format in {field} at rows: {[i+2 for i in invalid_format]}")
                else:
                    issues.append(f"Invalid date format in {field} at {len(invalid_format)} rows")
        
        # Check Emirates ID format if present
        if 'emirates_id' in df.columns:
            invalid_eid = []
            for idx, val in enumerate(df['emirates_id']):
                if pd.notna(val) and val not in ['.', '']:
                    if not re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', str(val)):
                        invalid_eid.append(idx)
                        
            if invalid_eid:
                if len(invalid_eid) <= 3:
                    issues.append(f"Invalid Emirates ID format at rows: {[i+2 for i in invalid_eid]}")
                else:
                    issues.append(f"Invalid Emirates ID format at {len(invalid_eid)} rows")
                    
        return {
            'file': os.path.basename(file_path),
            'is_valid': len(issues) == 0,
            'row_count': len(df),
            'issues': issues
        }
    
    except Exception as e:
        logger.error(f"Error validating {file_path}: {str(e)}")
        return {
            'file': os.path.basename(file_path),
            'is_valid': False,
            'row_count': 0,
            'issues': [f"Validation error: {str(e)}"]
        }

def run_validation(output_dir: str):
    """
    Validate output files in a directory.
    
    Args:
        output_dir: Directory containing output files
    """
    print_separator()
    logger.info(f"VALIDATING OUTPUT FILES IN {output_dir}")
    print_separator()
    
    if not os.path.exists(output_dir):
        logger.error(f"Output directory does not exist: {output_dir}")
        return False
        
    # Find all Excel files in output directory and subdirectories
    excel_files = []
    for root, _, files in os.walk(output_dir):
        for file in files:
            if file.endswith(('.xlsx', '.xls')) and 'template' not in file.lower():
                excel_files.append(os.path.join(root, file))
                
    if not excel_files:
        logger.warning("No output Excel files found")
        return False
        
    logger.info(f"Found {len(excel_files)} output Excel files")
    
    # Validate each Excel file
    validation_results = []
    for excel_file in excel_files:
        result = validate_excel_file(excel_file)
        validation_results.append(result)
        
    # Print summary
    valid_count = sum(1 for r in validation_results if r['is_valid'])
    logger.info(f"Validation results: {valid_count}/{len(validation_results)} files are valid")
    
    for idx, result in enumerate(validation_results, 1):
        logger.info(f"File {idx}: {result['file']}")
        if result['is_valid']:
            logger.info(f"  Valid: Yes - {result['row_count']} rows")
        else:
            logger.info(f"  Valid: No - {len(result['issues'])} issues")
            for issue in result['issues']:
                logger.warning(f"  - {issue}")
                
    return valid_count == len(validation_results)

def reset_processed_emails():
    """Simple reset for email tracking."""
    processed_file = "processed_emails_simple.txt"
    
    # Delete and recreate the file
    if os.path.exists(processed_file):
        os.remove(processed_file)
        logger.info(f"Removed {processed_file}")
    
    # Create empty file
    with open(processed_file, 'w') as f:
        pass
    logger.info("Reset processed emails tracking")
        
def run_diagnostics():
    """Run diagnostics on document processing."""
    print_separator()
    logger.info("RUNNING TEXTRACT DIAGNOSTICS")
    print_separator()
    
    # Ask for a document to test
    import tkinter as tk
    from tkinter import filedialog
    
    root = tk.Tk()
    root.withdraw()
    
    file_path = filedialog.askopenfilename(
        title="Select Document to Test",
        filetypes=[("PDF Files", "*.pdf"), ("Image Files", "*.jpg;*.jpeg;*.png")]
    )
    
    if not file_path:
        logger.info("No file selected, exiting diagnostics")
        return
    
    logger.info(f"Testing document: {file_path}")
    
    # Initialize components
    textract = TextractProcessor()
    
    # Run diagnostics
    doc_type = None
    if "passport" in file_path.lower():
        doc_type = "passport"
    elif "emirates" in file_path.lower() or "eid" in file_path.lower():
        doc_type = "emirates_id"
    elif "visa" in file_path.lower():
        doc_type = "visa"
    
    logger.info(f"Detected document type: {doc_type}")
    result = textract.diagnostic_extract(file_path, doc_type)
    
    logger.info("Diagnostic summary:")
    for key, value in result.items():
        logger.info(f"  {key}: {value}")
    
    print_separator()
    logger.info(f"Diagnostics complete. Check {result.get('diagnostic_dir', 'logs')} for details")
    
def force_deduplication(emails, processed_file="processed_emails.json"):
    """Force deduplication by directly checking the file."""
    if not os.path.exists(processed_file):
        return emails
        
    try:
        with open(processed_file, 'r') as f:
            processed = json.load(f)
            processed_ids = set(processed.keys())
            
        original_count = len(emails)
        emails = [e for e in emails if e.get('id') not in processed_ids]
        
        logger.info(f"Force deduplication: Removed {original_count - len(emails)} emails")
        logger.info(f"Remaining emails: {[e.get('subject', 'No Subject') for e in emails]}")
        return emails
    except Exception as e:
        logger.error(f"Error in force deduplication: {str(e)}")
        return emails
    
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test complete workflow')
    parser.add_argument('--reset', action='store_true', help='Reset processed emails tracking')
    parser.add_argument('--validate', metavar='DIR', help='Validate output files in directory')
    parser.add_argument('--diagnose', action='store_true', help='Run diagnostics on document processing')
    args = parser.parse_args()
    
    if args.reset:
        reset_processed_emails()
    
    if args.diagnose:
        run_diagnostics()
    elif args.validate:
        run_validation(args.validate)
    else:
        run_test()