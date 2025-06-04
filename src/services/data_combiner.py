from typing import Dict, Optional, List, Any, Tuple, Set
import pandas as pd
import os
import logging
import re
import time
import threading
import numpy as np
from datetime import datetime, timedelta
import hashlib
import copy
import json
from functools import lru_cache

from src.utils.error_handling import ServiceError, handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class DataCombiner:
    """Enhanced data combiner with improved merging logic and performance."""
    
    def __init__(self, textract_processor, excel_processor, deepseek_processor=None):
        """Initialize the data combiner.
        
        Args:
            textract_processor: Processor for document text extraction
            excel_processor: Processor for Excel file handling
            deepseek_processor: Optional DeepSeek processor for name extraction
        """
        self.textract_processor = textract_processor
        self.excel_processor = excel_processor
        self.deepseek_processor = deepseek_processor
        self.DEFAULT_VALUE = '.'
        
        # Pre-initialize field mappings for better performance
        self._field_mapping = self._initialize_field_mapping()
        self._date_fields = self._initialize_date_fields()
        self._numeric_fields = self._initialize_numeric_fields()
        
        # Caching mechanism for template structure
        self._template_cache = {}
        self._template_cache_lock = threading.RLock()
        
    def _initialize_field_mapping(self) -> Dict[str, Any]:
        """Initialize comprehensive field mapping dictionary with better template matching."""
        return {
            # Personal identification fields
            'passport_no': {
                'priority': ['passport_number', 'passport_no', 'passport', 'Passport No'],
                'format': r'^[A-Z0-9]{6,12}$',
                'clean': lambda x: str(x).upper().strip()
            },
            'passport_number': ['passport_no', 'passport', 'passportnumber', 'Passport No'],
            'emirates_id': {
                'priority': ['emirates_id', 'eid', 'id_number', 'Emirates Id'],
                'format': r'^\d{3}-\d{4}-\d{7}-\d{1}$',
                'clean': lambda x: self._process_emirates_id(x)
            },
            'eid': ['emirates_id', 'emirates_id_number', 'id_number', 'uae_id', 'Emirates Id'],
            'unified_no': ['unified_number', 'unified', 'entry_permit_no', 'visa_number', 'Unified No'],
            'entry_permit_no': ['visa_number', 'visa_file_number', 'permit_number', 'unified_no', 'Visa File Number'],
            
            # Personal information fields
            'first_name': ['firstname', 'fname', 'given_name', 'given_names', 'name_first', 'first', 'First Name'],
            'middle_name': ['middlename', 'mname', 'name_middle', 'middle', 'Middle Name'],
            'last_name': ['lastname', 'lname', 'surname', 'name_last', 'family_name', 'last', 'Last Name'],
            'full_name': ['name', 'complete_name', 'person_name', 'customer_name'],
            'gender': ['sex', 'gender_type', 'Gender'],
            'dob': ['date_of_birth', 'birth_date', 'birthdate', 'birth_day', 'DOB', 'DateOfBirth'],
            'date_of_birth': ['dob', 'birth_date', 'birthdate', 'birth_day', 'DOB'],
            
            # Add more template column mappings...
            'contract_name': ['Contract Name'],
            'effective_date': ['Effective Date', 'start_date', 'coverage_start', 'policy_start', 'begin_date', 'EffectiveDate'],
            'marital_status': ['Marital Status', 'marriage_status', 'civil_status'],
            'category': ['Category'],
            'relation': ['Relation'],
            'principal_card_no': ['Principal Card No.'],
            'family_no': ['Family No.'],
            'staff_id': ['Staff ID', 'employee_id', 'employee_no', 'staff_number', 'worker_id'],
            'nationality': ['Nationality', 'nation', 'citizenship', 'country', 'country_of_birth'],
            'sub_nationality': ['Sub-Nationality'],
            'visa_file_number': ['Visa File Number'],
            'work_country': ['Work Country'],
            'work_emirate': ['Work Emirate'],
            'work_region': ['Work Region'],
            'residence_country': ['Residence Country'],
            'residence_emirate': ['Residence Emirate'],
            'residence_region': ['Residence Region'],
            'mobile_no': ['Mobile No', 'phone', 'phone_number', 'mobile', 'contact_number', 'cell'],
            'email': ['Email', 'email_address', 'mail', 'email_id'],
            'salary_band': ['Salary Band', 'salary_range', 'income_band', 'salary_bracket'],
            'passport_expiry_date': ['Passport Expiry Date'],
            'visa_expiry_date': ['Visa Expiry Date']
        }

    def _process_emirates_id(self, value: str) -> str:
        """Process Emirates ID with special handling for non-784 IDs."""
        try:
            if not value or pd.isna(value) or value == '' or value == self.DEFAULT_VALUE:
                return ''
            
            # Clean the value
            value_str = str(value).strip()
            cleaned = re.sub(r'[^0-9\-]', '', value_str)
            
            # Format with hyphens if needed and possible
            if '-' not in cleaned and len(cleaned) == 15:
                cleaned = f"{cleaned[:3]}-{cleaned[3:7]}-{cleaned[7:14]}-{cleaned[14]}"
            
            # Check if it starts with 784
            if not cleaned.startswith('784'):
                logger.info(f"Emirates ID '{cleaned}' doesn't start with 784, replacing with default value")
                return '111-1111-1111111-1'
            
            return cleaned
        except Exception as e:
            logger.error(f"Error processing Emirates ID: {str(e)}")
            # Return default on error
            return '111-1111-1111111-1'
    
    def _initialize_date_fields(self) -> Set[str]:
        """Initialize set of fields that should be formatted as dates."""
        return {
            'date_of_birth', 'dob', 'expiry_date', 'issue_date', 
            'effective_date', 'passport_expiry_date', 'visa_expiry_date', 
            'date_of_issue', 'date_of_expiry', 'employment_date'
        }
        
    def _initialize_numeric_fields(self) -> Set[str]:
        """Initialize set of fields that should be numeric."""
        return {
            'mobile_no', 'staff_id', 'premium', 'coverage_amount', 
            'salary', 'age', 'dependents', 'family_no'
        }

    @handle_errors(ErrorCategory.PROCESS, ErrorSeverity.MEDIUM)
    def combine_and_populate_template(self, template_path: str, output_path: str, 
                                    extracted_data: Dict, excel_data: Any = None, document_paths: Dict[str, Any] = None) -> Dict:
        """Combine data with better handling of multiple rows."""
        logger.info(f"Starting data combination with template: {template_path}")
        logger.info(f"Extracted data has {len(extracted_data)} fields: {list(extracted_data.keys())}")
        
        logger.info("===== DATA COMBINER INPUT =====")
        logger.info(f"Extracted data: {len(extracted_data)} fields")
        for key, value in extracted_data.items():
            if value != self.DEFAULT_VALUE:
                logger.info(f"  - {key}: {value}")
        logger.info(f"Excel data type: {type(excel_data)}")
        if isinstance(excel_data, list):
            logger.info(f"Excel data: {len(excel_data)} rows")
            for idx, row in enumerate(excel_data):
                logger.info(f"Row {idx} fields: {', '.join(row.keys())}")
        
        # Early validation of inputs
        if extracted_data is None:
            extracted_data = {}
            logger.warning("Extracted data is None, using empty dictionary")
        
        if document_paths is None:
            document_paths = {}
            logger.warning("Document paths is None, using empty dictionary")
            
        # Log excel data information
        if excel_data is not None:
            if isinstance(excel_data, pd.DataFrame):
                logger.info(f"Excel data has {len(excel_data)} rows")
            elif isinstance(excel_data, list):
                logger.info(f"Excel data has {len(excel_data)} rows")
            elif isinstance(excel_data, dict):
                logger.info(f"Excel data has 1 row with {len(excel_data)} fields")
            else:
                logger.warning(f"Excel data has unexpected type: {type(excel_data)}")
                
        start_time = time.time()
        try:
            # Validate template
            if not os.path.exists(template_path):
                raise FileNotFoundError(f"Template file not found: {template_path}")
                
            # Get template structure
            template_info = self._get_template_structure(template_path)
            template_columns = template_info['columns']
            
            # Log template type for diagnostics
            is_almadallah = any(col in template_columns for col in ['FIRSTNAME', 'MIDDLENAME', 'LASTNAME', 'FULLNAME', 'POLICYCATEGORY'])
            if is_almadallah:
                logger.info(f"Processing Al Madallah template: {template_path}")
                logger.info(f"Al Madallah template has {len(template_columns)} columns, including: {template_columns[:5]}...")
            else:
                logger.info(f"Processing standard template: {template_path}")
                logger.info(f"Template has {len(template_columns)} columns, including: {template_columns[:5]}...")
            
            # Initialize field mappings
            field_mappings = {}
            
            # Make sure template columns are properly understood
            logger.info(f"Template has {len(template_columns)} columns: {template_columns[:10]}...")
            
            # Process excel_data with robust error handling
            try:
                # Validate and convert excel_data
                if excel_data is not None:
                    logger.info(f"Original excel_data type: {type(excel_data)}")
                    if isinstance(excel_data, dict):
                        # Convert dictionary to DataFrame with a single row
                        excel_data = pd.DataFrame([excel_data])
                        logger.info("Converted dict to DataFrame with 1 row")
                    elif isinstance(excel_data, list):
                        if not excel_data:
                            # Empty list
                            logger.warning("Excel data is an empty list, using default DataFrame")
                            # Create a default DataFrame with basic structure for testing
                            excel_data = pd.DataFrame([
                                {"First Name": "Row 1", "Middle Name": ".", "Last Name": "Default", "Contract Name": " "},
                                {"First Name": "Row 2", "Middle Name": ".", "Last Name": "Default", "Contract Name": " "},
                                {"First Name": "Row 3", "Middle Name": ".", "Last Name": "Default", "Contract Name": " "}
                            ])
                        else:
                            # Convert list to DataFrame
                            excel_data = pd.DataFrame(excel_data)
                            logger.info(f"Converted list with {len(excel_data)} items to DataFrame")
                    elif isinstance(excel_data, pd.DataFrame):
                        logger.info(f"Excel data is already a DataFrame with {len(excel_data)} rows")
                    else:
                        # Invalid type
                        logger.warning(f"Excel data has invalid type {type(excel_data)}, using default DataFrame")
                        # Create a default DataFrame with basic structure for testing
                        excel_data = pd.DataFrame([
                            {"First Name": "Row 1", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""},
                            {"First Name": "Row 2", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""},
                            {"First Name": "Row 3", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""}
                        ])
                else:
                    # None value
                    logger.info("Excel data is None, using default DataFrame")
                    # Create a default DataFrame with basic structure for testing
                    excel_data = pd.DataFrame([
                        {"First Name": "Row 1", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""},
                        {"First Name": "Row 2", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""},
                        {"First Name": "Row 3", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""}
                    ])
                    
                # CRITICAL: Log the final Excel data structure
                logger.info(f"Final excel_data DataFrame has {len(excel_data)} rows and {len(excel_data.columns)} columns")
                logger.info(f"Excel columns: {list(excel_data.columns)}")
                
                # Log first few rows for debugging
                for idx, row in excel_data.head(3).iterrows():
                    logger.info(f"Excel row {idx}: {dict(row)}")
                    
            except Exception as e:
                logger.error(f"Error processing excel_data: {str(e)}", exc_info=True)
                # Create a default DataFrame with basic structure for testing
                excel_data = pd.DataFrame([
                    {"First Name": "Row 1", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""},
                    {"First Name": "Row 2", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""},
                    {"First Name": "Row 3", "Middle Name": ".", "Last Name": "Default", "Contract Name": ""}
                ])
            
            # Process data based on what we have
            try:
                if not excel_data.empty:
                    logger.info(f"Processing {len(excel_data)} rows with document data")
                    result_df = self._process_multiple_rows(extracted_data, excel_data, 
                                                    template_columns, field_mappings, document_paths)
                else:
                    logger.info("Using document data only")
                    result_df = self._process_single_row(extracted_data, template_columns, 
                                                field_mappings, document_paths)
                    # existing code...
            except Exception as e:
                logger.error(f"Error in data processing: {str(e)}", exc_info=True)
                
                # CRITICAL FIX: Instead of creating minimal data, preserve the original Excel data
                if isinstance(excel_data, pd.DataFrame) and not excel_data.empty:
                    logger.info("Preserving original Excel data despite processing error")
                    
                    # Create a result DataFrame from the original Excel
                    result_df = excel_data.copy()
                    
                    # Add any missing template columns
                    for col in template_columns:
                        if col not in result_df.columns:
                            result_df[col] = self.DEFAULT_VALUE
                    
                    # Add extracted data fields where possible
                    for field, value in extracted_data.items():
                        if value != self.DEFAULT_VALUE:
                            # Try to find corresponding column
                            for col in template_columns:
                                if col.lower() == field.lower() or col.lower().replace(' ', '_') == field.lower():
                                    result_df[col] = value
                                    logger.info(f"Applied extracted {field} to column {col}")
                    
                    # Make sure we have all template columns
                    result_df = result_df[template_columns]
                    
                    logger.info(f"Preserved {len(result_df)} rows from original Excel despite processing error")
                else:
                    # Fall back to creating simple rows if no Excel data
                    logger.info("Falling back to basic data processing")
                    
                    # Create 3 rows with extracted data
                    result_data_list = []
                    
                    for i in range(3):
                        result_data = {}
                        
                        # Set default Contract Name
                        result_data['Contract Name'] = ''
                        
                        # Try to map extraction directly to template columns
                        for col in template_columns:
                            col_lower = col.lower()
                            # Check for direct matches in extracted data
                            for key, value in extracted_data.items():
                                key_lower = key.lower()
                                if key_lower == col_lower or key_lower.replace('_', ' ') == col_lower:
                                    result_data[col] = value
                                    break
                            # If no match found, use default value
                            if col not in result_data:
                                result_data[col] = self.DEFAULT_VALUE
                        
                        # Customize row based on index
                        result_data['First Name'] = f"Row {i+1}" if i > 0 else result_data.get('First Name', 'Default')
                        result_data['Last Name'] = 'Default' if i > 0 else result_data.get('Last Name', 'Default')
                        
                        result_data_list.append(result_data)
                    
                    # Create a DataFrame with all 3 rows
                    result_df = pd.DataFrame(result_data_list)
                
            # Verify critical fields are preserved in final output
            for i in range(len(result_df)):
                row_dict = result_df.iloc[i].to_dict()
                # Create a copy of the extracted data for this row
                row_extracted = {}
                for field in ['unified_no', 'visa_file_number', 'emirates_id', 'passport_number']:
                    if field in extracted_data and extracted_data[field] != self.DEFAULT_VALUE:
                        row_extracted[field] = extracted_data[field]
                
                # Apply verification
                verified_row = self._verify_critical_fields(row_dict, row_extracted)
                
                # Update DataFrame with verified data
                for col, value in verified_row.items():
                    if col in result_df.columns:
                        result_df.at[i, col] = value

            logger.info("Applied critical field verification to final output")
            
            # Final verification of critical fields
            logger.info("=" * 80)
            logger.info("FINAL VERIFICATION OF CRITICAL FIELDS")
            logger.info("=" * 80)

            critical_fields = ['Unified No', 'Visa File Number', 'Emirates Id']
            missing_count = {field: 0 for field in critical_fields}

            for idx, row in result_df.iterrows():
                missing_fields = []
                for field in critical_fields:
                    if field not in row or pd.isna(row[field]) or row[field] == "" or row[field] == self.DEFAULT_VALUE:
                        missing_fields.append(field)
                        missing_count[field] += 1
                
                if missing_fields:
                    logger.warning(f"Row {idx+1} is missing critical fields: {', '.join(missing_fields)}")
                    
                    # Try to restore from extracted_data if possible
                    for field in missing_fields:
                        if field == 'Unified No' and 'unified_no' in extracted_data and extracted_data['unified_no'] != self.DEFAULT_VALUE:
                            result_df.at[idx, field] = extracted_data['unified_no']
                            logger.info(f"Restored {field} from extracted_data: {extracted_data['unified_no']}")
                        elif field == 'Visa File Number' and 'visa_file_number' in extracted_data and extracted_data['visa_file_number'] != self.DEFAULT_VALUE:
                            result_df.at[idx, field] = extracted_data['visa_file_number']
                            logger.info(f"Restored {field} from extracted_data: {extracted_data['visa_file_number']}")
                        elif field == 'Emirates Id' and 'emirates_id' in extracted_data and extracted_data['emirates_id'] != self.DEFAULT_VALUE:
                            result_df.at[idx, field] = extracted_data['emirates_id']
                            logger.info(f"Restored {field} from extracted_data: {extracted_data['emirates_id']}")

            logger.info(f"Missing field summary: {missing_count}")
            
            # Special handling for Al Madallah template
            is_almadallah = any(col in result_df.columns for col in ['FIRSTNAME', 'MIDDLENAME', 'LASTNAME', 'FULLNAME', 'POLICYCATEGORY', 'ESTABLISHMENTTYPE'])
            if is_almadallah:
                logger.info("Applying final adjustments for Al Madallah template")
                
                # Process each row to ensure all required fields are populated
                for idx, row in result_df.iterrows():
                    # Generate FULLNAME if not already populated
                    if 'FULLNAME' in result_df.columns:
                        first = row.get('FIRSTNAME', '')
                        middle = row.get('MIDDLENAME', '')
                        last = row.get('LASTNAME', '')
                        if pd.notna(first) and pd.notna(last):
                            full_name = f"{first} {middle} {last}".replace('  ', ' ').strip()
                            result_df.at[idx, 'FULLNAME'] = full_name
                            logger.info(f"Row {idx+1}: Set FULLNAME to {full_name}")
                    
                    # Set Subgroup Name from Contract Name if available
                    if 'Subgroup Name' in result_df.columns and ('Contract Name' in excel_data.columns or 'contract_name' in extracted_data):
                        contract_name = None
                        if 'Contract Name' in excel_data.columns and idx < len(excel_data):
                            contract_name = excel_data.at[idx, 'Contract Name']
                        elif 'contract_name' in extracted_data:
                            contract_name = extracted_data['contract_name']
                            
                        if contract_name and pd.notna(contract_name) and contract_name != self.DEFAULT_VALUE:
                            result_df.at[idx, 'Subgroup Name'] = contract_name
                            logger.info(f"Row {idx+1}: Set Subgroup Name from Contract Name: {contract_name}")
                        else:
                            # Default if no Contract Name
                            result_df.at[idx, 'Subgroup Name'] = 'GENERAL'
                            logger.info(f"Row {idx+1}: Set default Subgroup Name: GENERAL")
                    
                    # Set COMMISSION to "NO"
                    if 'COMMISSION' in result_df.columns:
                        result_df.at[idx, 'COMMISSION'] = 'NO'
                    
                    # Set ESTABLISHMENTTYPE to "Establishment"
                    if 'ESTABLISHMENTTYPE' in result_df.columns:
                        result_df.at[idx, 'ESTABLISHMENTTYPE'] = 'Establishment'
                    
                    # Copy Mobile No to COMPANYPHONENUMBER, LANDLINENO, and MOBILE
                    if 'Mobile No' in row:
                        mobile = row['Mobile No']
                        if pd.notna(mobile) and mobile != '' and mobile != self.DEFAULT_VALUE:
                            if 'COMPANYPHONENUMBER' in result_df.columns:
                                result_df.at[idx, 'COMPANYPHONENUMBER'] = mobile
                            if 'LANDLINENO' in result_df.columns:
                                result_df.at[idx, 'LANDLINENO'] = mobile
                            if 'MOBILE' in result_df.columns:
                                result_df.at[idx, 'MOBILE'] = mobile
                    
                    # Copy Email to COMPANYEMAILID and EMAIL
                    if 'Email' in row:
                        email = row['Email']
                        if pd.notna(email) and email != '' and email != self.DEFAULT_VALUE:
                            if 'COMPANYEMAILID' in result_df.columns:
                                result_df.at[idx, 'COMPANYEMAILID'] = email
                            if 'EMAIL' in result_df.columns:
                                result_df.at[idx, 'EMAIL'] = email
                
            # Save results
            try:
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                    
                # Make sure we have all template columns in the right order
                for col in template_columns:
                    if col not in result_df.columns:
                        result_df[col] = ''  # Add missing columns with empty values
                
                # Reorder columns to match template exactly
                result_df = result_df[template_columns]
                    
                # Final cleanup of date formats
                date_fields = ['effective_date', 'dob', 'passport_expiry_date', 'visa_expiry_date', 
                            'Effective Date', 'DOB', 'Passport Expiry Date', 'Visa Expiry Date']
                for field in date_fields:
                    if field in result_df.columns:
                        result_df[field] = result_df[field].apply(
                            lambda x: self._format_date_value(x) if pd.notna(x) and x != '' and x != self.DEFAULT_VALUE else x
                        )
                
                # Apply special formatting for default values
                for col in result_df.columns:
                    col_lower = col.lower()
                    # Only Middle Name gets '.' default
                    if col_lower == 'middle name' or col == 'Middle Name':
                        result_df[col] = result_df[col].apply(
                            lambda x: '.' if pd.isna(x) or x == '' or x == self.DEFAULT_VALUE else x
                        )
                    else:
                        # All other fields get empty string
                        result_df[col] = result_df[col].apply(
                            lambda x: '' if pd.isna(x) or x == '' or x == self.DEFAULT_VALUE else x
                        )
                
                # Ensure Contract Name is populated
                if 'Contract Name' in result_df.columns and (result_df['Contract Name'].isna().all() or (result_df['Contract Name'] == '').all()):
                    result_df['Contract Name'] = ''
                    logger.info("Setting default Contract Name")
                
                # *** NEW CODE: Final check for Effective Date ***
                result_df = self._ensure_effective_date(result_df)
                
                # Log key columns before saving
                for col in ['First Name', 'Last Name', 'Nationality', 'Passport No', 'Emirates Id', 'Unified No', 'Contract Name', 'Effective Date']:
                    if col in result_df.columns:
                        values = result_df[col].tolist()
                        logger.info(f"Column {col} values: {values}")
                
                # CRITICAL FIX: Ensure Contract Name is properly preserved from Excel
                if isinstance(excel_data, pd.DataFrame) and not excel_data.empty and 'Contract Name' in excel_data.columns:
                    # Find first non-empty Contract Name from Excel
                    contract_names = [name for name in excel_data['Contract Name'] if pd.notna(name) and name != '' and name != self.DEFAULT_VALUE]
                    if contract_names:
                        default_contract = contract_names[0]
                        logger.info(f"Found Contract Name in Excel: {default_contract}")
                        
                        # Apply to all rows in result_df
                        if 'Contract Name' in result_df.columns:
                            result_df['Contract Name'] = result_df['Contract Name'].apply(
                                lambda x: default_contract if not x or x == '' or x == self.DEFAULT_VALUE else x
                            )
                            logger.info(f"Applied Contract Name '{default_contract}' to all rows")
                
                # Save to Excel with columns in exact template order
                result_df.to_excel(output_path, index=False)
                
                processing_time = time.time() - start_time
                logger.info(f"Data combined successfully in {processing_time:.2f}s: "
                        f"{len(result_df)} rows, {len(template_columns)} columns")
                
                return {
                    'status': 'success',
                    'output_path': output_path,
                    'rows_processed': len(result_df),
                    'processing_time': processing_time,
                    'field_mappings': field_mappings
                }
            except Exception as e:
                logger.error(f"Error saving results: {str(e)}", exc_info=True)
                raise ServiceError(f"Failed to save combined data: {str(e)}")

            
        except Exception as e:
            logger.error(f"Error combining data: {str(e)}", exc_info=True)
            raise ServiceError(f"Data combination failed: {str(e)}")

    @lru_cache(maxsize=10)
    def _get_template_structure(self, template_path: str) -> Dict[str, Any]:
        """Get template structure with caching for better performance."""
        with self._template_cache_lock:
            # Check cache first
            if template_path in self._template_cache:
                return self._template_cache[template_path]
                
            # Read template
            logger.info(f"Reading template structure from {template_path}")
            template_df = pd.read_excel(template_path)
            
            # Analyze template
            column_info = {}
            for col in template_df.columns:
                # Check if column has dropdown validation
                column_info[col] = {
                    'normalized_name': self._normalize_column_name(col),
                    'required': col.endswith('*')
                }
            
            # Check for duplicate field names with different case
            column_names = template_df.columns.tolist()
            lowercase_map = {}
            for col in column_names:
                col_lower = col.lower()
                if col_lower in lowercase_map:
                    logger.warning(f"Template has duplicate field names with different case: '{col}' and '{lowercase_map[col_lower]}'")
                    # Record which one appears first (lower index) to keep that one
                    if column_names.index(col) < column_names.index(lowercase_map[col_lower]):
                        lowercase_map[col_lower] = col
                else:
                    lowercase_map[col_lower] = col
            
            # Create template info dictionary
            template_info = {
                'columns': template_df.columns.tolist(),
                'column_info': column_info,
                'column_count': len(template_df.columns),
                'last_modified': os.path.getmtime(template_path),
                'case_normalized_columns': lowercase_map  # Add the new field here
            }
            
            # Cache the result
            self._template_cache[template_path] = template_info
            return template_info

    def _process_multiple_rows(self, extracted_data: Dict, excel_data: pd.DataFrame, 
                template_columns: List[str], field_mappings: Dict,
                document_paths: Dict[str, Any] = None) -> pd.DataFrame:
        """Process multiple rows with intelligent document matching."""
        # Store DEFAULT_VALUE locally
        DEFAULT_VALUE = self.DEFAULT_VALUE
        
        # Add detailed logging for debugging
        logger.info("=" * 80)
        logger.info("MULTI-ROW PROCESSING DIAGNOSTICS")
        logger.info("=" * 80)
        logger.info(f"Excel data: {len(excel_data)} rows")
        logger.info(f"Extracted data: {len(extracted_data)} fields")
        
        # Initialize data structure to hold per-document extracted data
        documents_data = {}
        
        # CRITICAL: Handle multiple documents and store data for each document separately
        if document_paths:
            try:
                logger.info(f"Processing document_paths with {len(document_paths)} document types")
                
                # Process each document type
                for doc_type, paths in document_paths.items():
                    if isinstance(paths, list):
                        # Handle list of paths (new structure)
                        logger.info(f"Processing {doc_type} with {len(paths)} documents")
                        for path in paths:
                            try:
                                # Extract data from this document with GPT or Textract
                                doc_data = None
                                file_name = os.path.basename(path)
                                
                                # Try GPT first if available
                                if self.deepseek_processor:
                                    try:
                                        doc_data = self.deepseek_processor.process_document(path, doc_type)
                                        logger.info(f"GPT extracted data from {file_name}: {doc_data}")
                                    except Exception as e:
                                        logger.error(f"GPT extraction failed for {file_name}: {str(e)}")
                                
                                # Fallback to Textract if GPT failed or is not available
                                if not doc_data and hasattr(self, 'textract_processor') and self.textract_processor:
                                    try:
                                        doc_data = self.textract_processor.process_document(path, doc_type)
                                        logger.info(f"Textract extracted data from {file_name}: {doc_data}")
                                    except Exception as e:
                                        logger.error(f"Textract extraction failed for {file_name}: {str(e)}")
                                
                                # Store document data if we got any
                                if doc_data and isinstance(doc_data, dict):
                                    # Create a unique key for this document
                                    doc_key = f"{doc_type}_{os.path.basename(path)}"
                                    documents_data[doc_key] = {
                                        'type': doc_type,
                                        'path': path,
                                        'data': doc_data,
                                        'file_name': file_name
                                    }
                                    logger.info(f"Added document data for {doc_key}")
                            except Exception as e:
                                logger.error(f"Error processing document {path}: {str(e)}")
                    
                    elif paths is not None:  # Handle single path (old structure)
                        try:
                            # Extract data from this document
                            doc_data = None
                            file_name = os.path.basename(paths)
                            
                            # Try GPT first if available
                            if self.deepseek_processor:
                                try:
                                    doc_data = self.deepseek_processor.process_document(paths, doc_type)
                                    logger.info(f"GPT extracted data from {file_name}: {doc_data}")
                                except Exception as e:
                                    logger.error(f"GPT extraction failed for {file_name}: {str(e)}")
                            
                            # Fallback to Textract if GPT failed or is not available
                            if not doc_data and hasattr(self, 'textract_processor') and self.textract_processor:
                                try:
                                    doc_data = self.textract_processor.process_document(paths, doc_type)
                                    logger.info(f"Textract extracted data from {file_name}: {doc_data}")
                                except Exception as e:
                                    logger.error(f"Textract extraction failed for {file_name}: {str(e)}")
                            
                            # Store document data if we got any
                            if doc_data and isinstance(doc_data, dict):
                                # Create a unique key for this document
                                doc_key = f"{doc_type}_{os.path.basename(paths)}"
                                documents_data[doc_key] = {
                                    'type': doc_type,
                                    'path': paths,
                                    'data': doc_data,
                                    'file_name': file_name
                                }
                                logger.info(f"Added document data for {doc_key}")
                        except Exception as e:
                            logger.error(f"Error processing document {paths}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing document_paths: {str(e)}")
        
        # Log documents data collected
        logger.info(f"Collected data from {len(documents_data)} documents")
        for doc_key, doc_info in documents_data.items():
            logger.info(f"Document: {doc_key}")
            # Log meaningful data (passport, name, etc.)
            for field in ['passport_number', 'passport_no', 'full_name', 'name', 'emirates_id', 'unified_no', 'visa_file_number']:
                if field in doc_info['data'] and doc_info['data'][field] != DEFAULT_VALUE:
                    logger.info(f"  - {field}: {doc_info['data'][field]}")
        
        # Log Excel rows for matching
        logger.info("Excel rows for matching:")
        excel_rows_info = []
        for idx, row in excel_data.iterrows():
            row_dict = row.to_dict()
            row_info = {
                'index': idx,
                'data': row_dict,
                'identifiers': {}
            }
            
            # Extract key identifiers from row
            # Name
            first_name = ""
            last_name = ""
            for field in ['first_name', 'First Name', 'firstname', 'FirstName']:
                if field in row_dict and pd.notna(row_dict[field]):
                    first_name = str(row_dict[field]).strip()
                    break
            
            for field in ['last_name', 'Last Name', 'lastname', 'LastName']:
                if field in row_dict and pd.notna(row_dict[field]):
                    last_name = str(row_dict[field]).strip()
                    break
            
            if first_name or last_name:
                full_name = f"{first_name} {last_name}".strip()
                row_info['identifiers']['name'] = full_name
                logger.info(f"Row {idx+1}: name = {full_name}")
            
            # Passport
            for field in ['passport_no', 'Passport No', 'passport_number', 'PassportNo']:
                if field in row_dict and pd.notna(row_dict[field]) and row_dict[field] != DEFAULT_VALUE:
                    row_info['identifiers']['passport'] = str(row_dict[field]).strip()
                    logger.info(f"Row {idx+1}: passport = {row_info['identifiers']['passport']}")
                    break
            
            # Emirates ID
            for field in ['emirates_id', 'Emirates Id', 'eid', 'EmiratesId']:
                if field in row_dict and pd.notna(row_dict[field]) and row_dict[field] != DEFAULT_VALUE:
                    row_info['identifiers']['emirates_id'] = str(row_dict[field]).strip()
                    logger.info(f"Row {idx+1}: emirates_id = {row_info['identifiers']['emirates_id']}")
                    break
            
            excel_rows_info.append(row_info)
        
        # Match documents to rows
        matches = self._match_documents_to_rows(documents_data, excel_rows_info)
        
        # CRITICAL FIX: Initialize result_df early to avoid UnboundLocalError
        # Start with a copy of Excel data before any processing
        result_df = excel_data.copy()
        for col in template_columns:
            if col not in result_df.columns:
                result_df[col] = self.DEFAULT_VALUE
        logger.info(f"Pre-initialized result_df with {len(result_df)} rows from input Excel")
        
        # Process each Excel row - COMPLETE VERSION
        result_rows = []

        for row_idx, row_info in enumerate(excel_rows_info):
            row_dict = row_info['data']
            
            # CRITICAL FIX: Preserve original Excel data before cleaning
            original_excel_data = copy.deepcopy(row_dict)
            
            # Convert NaN values to empty strings but preserve actual values
            preserved_excel_data = {}
            for key, value in original_excel_data.items():
                if pd.isna(value):
                    preserved_excel_data[key] = ""
                else:
                    preserved_excel_data[key] = str(value).strip()
            
            # Clean Excel data (but keep original preserved)
            cleaned_excel = self._clean_excel_data(row_dict)
            
            # Log what we have for this row
            logger.info(f"Processing Excel row {row_idx+1}:")
            important_fields = ['FIRSTNAME', 'LASTNAME', 'EMPLOYEEID', 'FULLNAME', 'First Name', 'Last Name', 'Staff ID']
            for field in important_fields:
                if field in preserved_excel_data and preserved_excel_data[field]:
                    logger.info(f"  {field}: '{preserved_excel_data[field]}'")
            
            # Get matched documents for this row
            row_matches = matches.get(row_idx, [])
            
            if row_matches:
                logger.info(f"Row {row_idx+1} matched with {len(row_matches)} documents")
                
                # Create a combined extracted data dictionary for this row
                row_extracted = {}
                
                # Process each matched document
                for doc_key in row_matches:
                    doc_data = documents_data[doc_key]['data']
                    doc_type = documents_data[doc_key]['type']
                    
                    logger.info(f"  - Applying data from {doc_key} (type: {doc_type})")
                    
                    # Special handling for visa documents
                    if doc_type.lower() == 'visa':
                        # Preserve visa file number exactly
                        if 'visa_file_number' in doc_data and doc_data['visa_file_number'] != DEFAULT_VALUE:
                            row_extracted['visa_file_number'] = doc_data['visa_file_number']
                            logger.info(f"PRESERVED VISA FILE NUMBER: {doc_data['visa_file_number']}")
                        
                        # Preserve unified number exactly
                        if 'unified_no' in doc_data and doc_data['unified_no'] != DEFAULT_VALUE:
                            # Validate format - should not contain slashes
                            if '/' not in doc_data['unified_no']:
                                row_extracted['unified_no'] = doc_data['unified_no']
                                logger.info(f"PRESERVED UNIFIED NUMBER: {doc_data['unified_no']}")
                    
                    # Special handling for Emirates ID documents
                    if doc_type.lower() == 'emirates_id':
                        if 'emirates_id' in doc_data and doc_data['emirates_id'] != DEFAULT_VALUE:
                            row_extracted['emirates_id'] = doc_data['emirates_id']
                            logger.info(f"PRESERVED EMIRATES ID: {doc_data['emirates_id']}")
                    
                    # Copy all non-default values from this document
                    for field, value in doc_data.items():
                        if field not in row_extracted or (value != DEFAULT_VALUE and row_extracted[field] == DEFAULT_VALUE):
                            row_extracted[field] = value
                
                # Clean the extracted data
                cleaned_extracted = self._clean_extracted_data(row_extracted)
                
                # Combine with Excel data
                row_data = self._combine_row_data(cleaned_extracted, cleaned_excel, None)
            else:
                logger.info(f"Row {row_idx+1} had no document matches, using Excel data only")
                row_data = cleaned_excel
            
            # Map to template
            mapped_row = self._map_to_template(row_data, template_columns, field_mappings)
            
            # CRITICAL FIX: Restore Excel data that might have been lost in mapping
            if mapped_row is not None:
                logger.info(f"Restoring Excel data for row {row_idx+1}:")
                
                # UNIVERSAL field mapping - works for BOTH templates
                # Check if this is Al Madallah or NAS template
                is_almadallah = 'FIRSTNAME' in template_columns
                is_nas = 'First Name' in template_columns
                
                if is_almadallah:
                    # Al Madallah template field mapping
                    excel_to_template_mapping = {
                        'FIRSTNAME': 'FIRSTNAME',
                        'MIDDLENAME': 'MIDDLENAME', 
                        'LASTNAME': 'LASTNAME',
                        'FULLNAME': 'FULLNAME',
                        'EMPLOYEEID': 'EMPLOYEEID',
                        'DOB': 'DOB',
                        'GENDER': 'GENDER',
                        'NATIONALITY': 'NATIONALITY',
                        'EMIRATESID': 'EMIRATESID',
                        'UIDNO': 'UIDNO',
                        'VISAFILEREF': 'VISAFILEREF',
                        'PASSPORTNO': 'PASSPORTNO',
                        'COMPANYPHONENUMBER': 'COMPANYPHONENUMBER',
                        'COMPANYEMAILID': 'COMPANYEMAILID',
                        'MOBILE': 'MOBILE',
                        'EMAIL': 'EMAIL'
                    }
                elif is_nas:
                    # NAS template field mapping  
                    excel_to_template_mapping = {
                        'First Name': 'First Name',
                        'Middle Name': 'Middle Name',
                        'Last Name': 'Last Name',
                        'Staff ID': 'Staff ID',
                        'DOB': 'DOB',
                        'Gender': 'Gender',
                        'Nationality': 'Nationality',
                        'Emirates Id': 'Emirates Id',
                        'Unified No': 'Unified No',
                        'Visa File Number': 'Visa File Number',
                        'Passport No': 'Passport No',
                        'Mobile No': 'Mobile No',
                        'Email': 'Email',
                        'Contract Name': 'Contract Name'
                    }
                else:
                    # Generic mapping
                    excel_to_template_mapping = {}
                    for col in template_columns:
                        if col in preserved_excel_data:
                            excel_to_template_mapping[col] = col
                
                # Restore Excel values to mapped row
                for excel_field, template_field in excel_to_template_mapping.items():
                    if (excel_field in preserved_excel_data and 
                        preserved_excel_data[excel_field] and 
                        preserved_excel_data[excel_field] != "" and
                        template_field in template_columns):
                        
                        mapped_row[template_field] = preserved_excel_data[excel_field]
                        logger.info(f"  Restored {template_field}: '{preserved_excel_data[excel_field]}'")
                
                # Apply extracted document data if we have matches
                if row_matches:
                    logger.info(f"Applying extracted data from {len(row_matches)} matched documents:")
                    
                    # Get all extracted data from matched documents
                    combined_extracted = {}
                    for doc_key in row_matches:
                        if doc_key in documents_data:
                            doc_data = documents_data[doc_key]['data']
                            for field, value in doc_data.items():
                                if value != DEFAULT_VALUE and value != "" and value is not None:
                                    combined_extracted[field] = value
                    
                    # Apply extracted data to the correct template fields
                    if is_almadallah:
                        # Al Madallah field mappings
                        extracted_mappings = {
                            'emirates_id': 'EMIRATESID',
                            'unified_no': 'UIDNO',
                            'visa_file_number': 'VISAFILEREF',
                            'passport_number': 'PASSPORTNO',
                            'nationality': 'NATIONALITY',
                            'date_of_birth': 'DOB',
                            'gender': 'GENDER',
                            'mobile_no': 'MOBILE',
                            'email': 'EMAIL'
                        }
                    elif is_nas:
                        # NAS field mappings
                        extracted_mappings = {
                            'emirates_id': 'Emirates Id',
                            'unified_no': 'Unified No',
                            'visa_file_number': 'Visa File Number',
                            'passport_number': 'Passport No',
                            'nationality': 'Nationality',
                            'date_of_birth': 'DOB',
                            'gender': 'Gender',
                            'mobile_no': 'Mobile No',
                            'email': 'Email'
                        }
                    else:
                        # Generic mapping
                        extracted_mappings = {}
                        for field in combined_extracted:
                            if field in template_columns:
                                extracted_mappings[field] = field
                    
                    # Apply the extracted data
                    for extracted_field, template_field in extracted_mappings.items():
                        if (extracted_field in combined_extracted and 
                            template_field in template_columns):
                            
                            value = combined_extracted[extracted_field]
                            
                            # Only apply if the template field is currently empty
                            if not mapped_row.get(template_field) or mapped_row.get(template_field) == "":
                                mapped_row[template_field] = value
                                logger.info(f"  Applied extracted {template_field}: '{value}'")
                
                # Generate FULLNAME if needed (Al Madallah template)
                if (is_almadallah and 'FULLNAME' in template_columns and 
                    (not mapped_row.get('FULLNAME') or mapped_row.get('FULLNAME') == "")):
                    
                    first = mapped_row.get('FIRSTNAME', '')
                    middle = mapped_row.get('MIDDLENAME', '')
                    last = mapped_row.get('LASTNAME', '')
                    
                    if first or last:
                        fullname_parts = [first, middle, last]
                        fullname = ' '.join([part for part in fullname_parts if part and part != ""]).strip()
                        if fullname:
                            mapped_row['FULLNAME'] = fullname
                            logger.info(f"  Generated FULLNAME: '{fullname}'")
                
                # Ensure standard fields are set
                self._apply_standard_fields(mapped_row)
                
                # CRITICAL FIX: Force set Effective Date for EVERY row
                # Use today's date in DD/MM/YYYY format
                today_date = datetime.now().strftime('%d/%m/%Y')
                if 'EFFECTIVEDATE' in template_columns:
                    mapped_row['EFFECTIVEDATE'] = today_date
                if 'Effective Date' in template_columns:
                    mapped_row['Effective Date'] = today_date
                logger.info(f"Set Effective Date to today: {today_date} for row {row_idx+1}")
                
                # Format Emirates ID if present
                if 'EMIRATESID' in mapped_row and mapped_row['EMIRATESID'] != DEFAULT_VALUE and mapped_row['EMIRATESID']:
                    mapped_row['EMIRATESID'] = self._format_emirates_id(mapped_row['EMIRATESID'])
                if 'Emirates Id' in mapped_row and mapped_row['Emirates Id'] != DEFAULT_VALUE and mapped_row['Emirates Id']:
                    mapped_row['Emirates Id'] = self._format_emirates_id(mapped_row['Emirates Id'])
                
                # Add row to results
                result_rows.append(mapped_row)
            else:
                logger.error(f"mapped_row is None for row {row_idx+1}! Creating fallback row.")
                
                # Create fallback row directly from Excel data
                fallback_row = {}
                for col in template_columns:
                    if col in preserved_excel_data and preserved_excel_data[col]:
                        fallback_row[col] = preserved_excel_data[col]
                    else:
                        fallback_row[col] = ""
                
                # Set today's date
                today_date = datetime.now().strftime('%d/%m/%Y')
                if 'EFFECTIVEDATE' in fallback_row:
                    fallback_row['EFFECTIVEDATE'] = today_date
                if 'Effective Date' in fallback_row:
                    fallback_row['Effective Date'] = today_date
                
                result_rows.append(fallback_row)
        
        # Create DataFrame from rows
        result_df = pd.DataFrame(result_rows)
        
        # Make sure all template columns exist in result
        for col in template_columns:
            if col not in result_df.columns:
                result_df[col] = ""
        
        # Ensure the columns are in the EXACT order as the template
        result_df = result_df[template_columns]
        
        # Apply final formatting for output
        for col in result_df.columns:
            col_lower = col.lower()
            # Only Middle Name gets '.' default
            if col_lower == 'middle name' or col == 'Middle Name':
                result_df[col] = result_df[col].apply(
                    lambda x: '.' if pd.isna(x) or x == '' or x == DEFAULT_VALUE else x
                )
            else:
                # All other fields get empty string
                result_df[col] = result_df[col].apply(
                    lambda x: '' if pd.isna(x) or x == '' or x == DEFAULT_VALUE else x
                )
        
        # Preserve Contract Name for each row, only fill empty ones with defaults
        if 'Contract Name' in result_df.columns:
            # Get first non-empty Contract Name as fallback default
            contract_names = [name for name in result_df['Contract Name'] if name and name != self.DEFAULT_VALUE]
            if contract_names:
                default_contract = contract_names[0]
                # Only set default for empty Contract Names
                result_df['Contract Name'] = result_df['Contract Name'].apply(
                    lambda x: default_contract if not x or x == self.DEFAULT_VALUE or x == '' else x
                )
                logger.info(f"Used default Contract Name '{default_contract}' for rows with empty values")
            else:
                # If no row has a Contract Name, set a reasonable default
                result_df['Contract Name'] = ''
                logger.info("No Contract Name found in any row, setting to empty string")
                
            logger.info(f"Ensured Contract Name is populated for all rows: {result_df['Contract Name'].iloc[0]}")
        
        # Final verification of critical fields
        logger.info("=" * 80)
        logger.info("FINAL VERIFICATION OF CRITICAL FIELDS")
        logger.info("=" * 80)

        critical_fields = ['Unified No', 'Visa File Number', 'Emirates Id']
        missing_count = {field: 0 for field in critical_fields}

        for idx, row in result_df.iterrows():
            missing_fields = []
            for field in critical_fields:
                if field not in row or pd.isna(row[field]) or row[field] == "" or row[field] == DEFAULT_VALUE:
                    missing_fields.append(field)
                    missing_count[field] += 1
            
            if missing_fields:
                logger.warning(f"Row {idx+1} is missing critical fields: {', '.join(missing_fields)}")
                
                # Try to restore from extracted_data if possible
                for field in missing_fields:
                    if field == 'Unified No' and 'unified_no' in extracted_data and extracted_data['unified_no'] != DEFAULT_VALUE:
                        result_df.at[idx, field] = extracted_data['unified_no']
                        logger.info(f"Restored {field} from extracted_data: {extracted_data['unified_no']}")
                    elif field == 'Visa File Number' and 'visa_file_number' in extracted_data and extracted_data['visa_file_number'] != DEFAULT_VALUE:
                        result_df.at[idx, field] = extracted_data['visa_file_number']
                        logger.info(f"Restored {field} from extracted_data: {extracted_data['visa_file_number']}")
                    elif field == 'Emirates Id' and 'emirates_id' in extracted_data and extracted_data['emirates_id'] != DEFAULT_VALUE:
                        result_df.at[idx, field] = extracted_data['emirates_id']
                        logger.info(f"Restored {field} from extracted_data: {extracted_data['emirates_id']}")

        logger.info(f"Missing field summary: {missing_count}")
        
        # CRITICAL FIX: Add fallback logic for recovering Unified No
        logger.info("Applying fallback logic for Unified No recovery")
        for idx, row in result_df.iterrows():
            # Check if Unified No is missing or empty
            if ('Unified No' not in row) or pd.isna(row['Unified No']) or (row['Unified No'] == '') or (row['Unified No'] == self.DEFAULT_VALUE):
                # Try deriving from Emirates Id if available
                if 'Emirates Id' in row and pd.notna(row['Emirates Id']) and row['Emirates Id'] != '' and row['Emirates Id'] != self.DEFAULT_VALUE:
                    emirates_id = row['Emirates Id']
                    
                    # Extract digits from Emirates ID
                    eid_digits = ''.join(filter(str.isdigit, str(emirates_id)))
                    
                    if len(eid_digits) >= 8:
                        logger.info(f"Row {idx+1}: Derived Unified No from Emirates Id as fallback: {eid_digits}")
                        result_df.at[idx, 'Unified No'] = eid_digits
                
                # If still missing, try using a portion of the visa file number as a last resort
                elif 'Visa File Number' in row and pd.notna(row['Visa File Number']) and row['Visa File Number'] != '' and row['Visa File Number'] != self.DEFAULT_VALUE:
                    visa_number = row['Visa File Number']
                    
                    # If it has the typical format with slashes
                    if '/' in visa_number:
                        parts = visa_number.split('/')
                        if len(parts) >= 3:
                            # Try using the last part (most unique) with the first part as prefix
                            unified_candidate = parts[0] + parts[2]
                            if len(unified_candidate) >= 8:
                                logger.info(f"Row {idx+1}: Created fallback Unified No from Visa File Number parts: {unified_candidate}")
                                result_df.at[idx, 'Unified No'] = unified_candidate
        
        # CRITICAL FIX: Apply document data directly to the Excel result DataFrame                
        logger.info("Directly applying document data to result DataFrame")
        for doc_key, doc_info in documents_data.items():
            doc_type = doc_info['type']
            doc_data = doc_info['data']
            
            # Find which row(s) this document was matched with
            matched_rows = []
            for row_idx, doc_keys in matches.items():
                if doc_key in doc_keys:
                    matched_rows.append(row_idx)
            
            # Apply extracted document data to matched rows
            for row_idx in matched_rows:
                logger.info(f"Directly applying critical data from {doc_key} to row {row_idx+1}")
                
                # Apply key fields directly to the final DataFrame
                if 'passport_number' in doc_data and doc_data['passport_number'] != DEFAULT_VALUE:
                    result_df.at[row_idx, 'Passport No'] = doc_data['passport_number']
                    logger.info(f"DIRECT SET: Passport No for row {row_idx+1}: {doc_data['passport_number']}")
                    
                if 'emirates_id' in doc_data and doc_data['emirates_id'] != DEFAULT_VALUE:
                    # Format and validate Emirates ID
                    eid_value = self._process_emirates_id(doc_data['emirates_id'])
                    result_df.at[row_idx, 'Emirates Id'] = eid_value
                    logger.info(f"DIRECT SET: Emirates Id for row {row_idx+1}: {eid_value}")
                    
                if 'unified_no' in doc_data and doc_data['unified_no'] != DEFAULT_VALUE:
                    # Verify it's not just visa_file_number without slashes
                    is_valid = True
                    if 'visa_file_number' in doc_data and doc_data['visa_file_number'] != DEFAULT_VALUE:
                        visa_no_slashes = doc_data['visa_file_number'].replace('/', '')
                        if doc_data['unified_no'] == visa_no_slashes:
                            is_valid = False
                    
                    if is_valid:
                        result_df.at[row_idx, 'Unified No'] = doc_data['unified_no']
                        logger.info(f"DIRECT SET: Unified No for row {row_idx+1}: {doc_data['unified_no']}")
                    
                if 'visa_file_number' in doc_data and doc_data['visa_file_number'] != DEFAULT_VALUE:
                    result_df.at[row_idx, 'Visa File Number'] = doc_data['visa_file_number']
                    logger.info(f"DIRECT SET: Visa File Number for row {row_idx+1}: {doc_data['visa_file_number']}")
                    
                # Apply other essential fields
                field_mapping = {
                    'nationality': 'Nationality',
                    'date_of_birth': 'DOB',
                    'gender': 'Gender',
                    'passport_expiry_date': 'Passport Expiry Date',
                    'visa_expiry_date': 'Visa Expiry Date',
                    'profession': 'Occupation'
                }
                
                for doc_field, result_field in field_mapping.items():
                    if doc_field in doc_data and doc_data[doc_field] != DEFAULT_VALUE:
                        result_df.at[row_idx, result_field] = doc_data[doc_field]
                        logger.info(f"DIRECT SET: {result_field} for row {row_idx+1}: {doc_data[doc_field]}")
        
        # FINAL SAFETY CHECK: Make sure names are never lost from original Excel
        logger.info("FINAL SAFETY CHECK: Ensuring names are preserved from original Excel")
        for idx, row in excel_data.iterrows():
            # Check if row exists in result_df (should always be true if index matches)
            if idx < len(result_df):
                # Check critical fields
                for field in ['First Name', 'Middle Name', 'Last Name', 'Contract Name', 'DOB', 'Gender', 'Nationality', 'Emirates Id', 'Unified No', 'Passport No', 'Visa File Number']:
                    if field in row and field in result_df.columns:
                        excel_value = row[field]
                        if pd.notna(excel_value) and excel_value != '' and excel_value != self.DEFAULT_VALUE:
                            if result_df.at[idx, field] != excel_value:
                                logger.info(f"Row {idx+1}: Forcing preservation of Excel {field}: {excel_value}")
                                result_df.at[idx, field] = excel_value
        
        return result_df


    def _match_documents_to_rows(self, documents_data: Dict, excel_rows_info: List[Dict]) -> Dict[int, List[str]]:
        """Match documents to Excel rows - works for both NAS and Al Madallah templates."""
        row_matches = {}
        match_details = {}
        
        # Initialize row matches
        for row_idx, _ in enumerate(excel_rows_info):
            row_matches[row_idx] = []
            match_details[row_idx] = []
        
        # If no data to match, return empty matches
        if not documents_data or not excel_rows_info:
            logger.warning("No documents or Excel rows to match")
            return row_matches
        
        # Extract names from Excel rows - UNIVERSAL approach for both templates
        excel_names = []
        for row_idx, row_info in enumerate(excel_rows_info):
            row_data = row_info.get('data', {})
            
            # Try BOTH template field name formats
            # Al Madallah format: FIRSTNAME, MIDDLENAME, LASTNAME, FULLNAME
            # NAS format: First Name, Middle Name, Last Name, full_name
            
            first_name = ""
            middle_name = ""
            last_name = ""
            full_name = ""
            
            # Check Al Madallah format first
            if 'FIRSTNAME' in row_data:
                first_name = str(row_data.get('FIRSTNAME', '')).strip()
            elif 'First Name' in row_data:
                first_name = str(row_data.get('First Name', '')).strip()
            elif 'first_name' in row_data:
                first_name = str(row_data.get('first_name', '')).strip()
                
            if 'MIDDLENAME' in row_data:
                middle_name = str(row_data.get('MIDDLENAME', '')).strip()
            elif 'Middle Name' in row_data:
                middle_name = str(row_data.get('Middle Name', '')).strip()
            elif 'middle_name' in row_data:
                middle_name = str(row_data.get('middle_name', '')).strip()
                
            if 'LASTNAME' in row_data:
                last_name = str(row_data.get('LASTNAME', '')).strip()
            elif 'Last Name' in row_data:
                last_name = str(row_data.get('Last Name', '')).strip()
            elif 'last_name' in row_data:
                last_name = str(row_data.get('last_name', '')).strip()
                
            if 'FULLNAME' in row_data:
                full_name = str(row_data.get('FULLNAME', '')).strip()
            elif 'Full Name' in row_data:
                full_name = str(row_data.get('Full Name', '')).strip()
            elif 'full_name' in row_data:
                full_name = str(row_data.get('full_name', '')).strip()
            
            # Create name variants for matching
            name_variants = []
            if full_name:
                name_variants.append(full_name.upper())
            if first_name:
                name_variants.append(first_name.upper())
            if last_name:
                name_variants.append(last_name.upper())
            if first_name and last_name:
                name_variants.append(f"{first_name} {last_name}".upper())
            if first_name and middle_name and last_name:
                name_variants.append(f"{first_name} {middle_name} {last_name}".upper())
            
            excel_names.append({
                'row_idx': row_idx,
                'variants': name_variants,
                'first': first_name.upper(),
                'last': last_name.upper(),
                'full': full_name.upper()
            })
            
            logger.info(f"Excel Row {row_idx+1} name variants: {name_variants}")
        
        # Match each document to Excel rows
        for doc_key, doc_info in documents_data.items():
            doc_type = doc_info['type']
            doc_data = doc_info['data']
            file_name = doc_info['file_name'].upper()
            
            # Skip matching if doc_data is None or empty
            if not doc_data:
                continue
                
            # Extract key identifiers from document
            doc_identifiers = {}
            
            # Extract passport number
            for field in ['passport_number', 'passport_no']:
                if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                    doc_identifiers['passport'] = re.sub(r'\s+', '', str(doc_data[field])).upper()
                    logger.info(f"Document {doc_key} has passport: {doc_identifiers['passport']}")
                    break
                    
            # Extract Emirates ID
            for field in ['emirates_id', 'emirates_id_number']:
                if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                    doc_identifiers['emirates_id'] = re.sub(r'[^0-9]', '', str(doc_data[field]))
                    logger.info(f"Document {doc_key} has Emirates ID: {doc_identifiers['emirates_id']}")
                    break
                    
            # Extract unified number
            for field in ['unified_no', 'unified_number']:
                if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                    doc_identifiers['unified_no'] = str(doc_data[field])
                    logger.info(f"Document {doc_key} has unified number: {doc_identifiers['unified_no']}")
                    break
                    
            # Extract name
            for field in ['full_name', 'name', 'name_en']:
                if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                    doc_identifiers['name'] = doc_data[field].lower()
                    logger.info(f"Document {doc_key} has name: {doc_identifiers['name']}")
                    break
            
            # Try to match with each Excel row
            best_match_idx = None
            best_match_score = 0
            best_match_reasons = []
            
            for excel_name_info in excel_names:
                row_idx = excel_name_info['row_idx']
                match_score = 0
                match_reasons = []
                
                # Get row identifiers for matching
                row_data = excel_rows_info[row_idx]['data']
                row_identifiers = {}
                
                # Extract row passport (check both formats)
                for field in ['passport_no', 'Passport No', 'PassportNo', 'PASSPORTNO']:
                    if field in row_data and row_data[field] and str(row_data[field]).strip() != '':
                        row_identifiers['passport'] = re.sub(r'\s+', '', str(row_data[field])).upper()
                        break
                        
                # Extract row Emirates ID (check both formats)
                for field in ['emirates_id', 'Emirates Id', 'EID', 'EMIRATESID']:
                    if field in row_data and row_data[field] and str(row_data[field]).strip() != '':
                        row_identifiers['emirates_id'] = re.sub(r'[^0-9]', '', str(row_data[field]))
                        break
                
                # Passport match (worth 100 points)
                if 'passport' in doc_identifiers and 'passport' in row_identifiers:
                    doc_passport = doc_identifiers['passport']
                    row_passport = row_identifiers['passport']
                    
                    if doc_passport == row_passport:
                        match_score += 100
                        match_reasons.append(f"Passport exact match: {doc_passport}")
                    elif doc_passport and row_passport and (doc_passport in row_passport or row_passport in doc_passport):
                        match_score += 50
                        match_reasons.append(f"Passport partial match: {doc_passport} vs {row_passport}")
                        
                # Emirates ID match (worth 100 points)
                if 'emirates_id' in doc_identifiers and 'emirates_id' in row_identifiers:
                    doc_eid = doc_identifiers['emirates_id']
                    row_eid = row_identifiers['emirates_id']
                    
                    if doc_eid == row_eid:
                        match_score += 100
                        match_reasons.append(f"Emirates ID exact match: {doc_eid}")
                    elif doc_eid and row_eid and len(doc_eid) >= 8 and len(row_eid) >= 8:
                        # Try last 8 digits matching
                        if doc_eid[-8:] == row_eid[-8:]:
                            match_score += 80
                            match_reasons.append(f"Emirates ID last 8 digits match")
                
                # Name matching (worth up to 50 points)
                if 'name' in doc_identifiers:
                    doc_name = doc_identifiers['name']
                    excel_variants = excel_name_info['variants']
                    
                    for excel_variant in excel_variants:
                        if excel_variant:
                            # Calculate name similarity
                            doc_words = set(re.findall(r'\b\w+\b', doc_name))
                            excel_words = set(re.findall(r'\b\w+\b', excel_variant.lower()))
                            
                            if doc_words and excel_words:
                                common_words = doc_words.intersection(excel_words)
                                
                                if common_words:
                                    similarity = len(common_words) / max(len(doc_words), len(excel_words))
                                    name_score = int(similarity * 50)
                                    match_score += name_score
                                    match_reasons.append(f"Name similarity: {similarity:.2f} ({doc_name} ≈ {excel_variant})")
                
                # Update best match if better score
                if match_score > best_match_score:
                    best_match_score = match_score
                    best_match_idx = row_idx
                    best_match_reasons = match_reasons
            
            # Assign document to best match if score is high enough
            if best_match_idx is not None and best_match_score >= 50:
                row_matches[best_match_idx].append(doc_key)
                match_details[best_match_idx].append({
                    'doc_key': doc_key,
                    'score': best_match_score,
                    'reasons': best_match_reasons
                })
                logger.info(f"Matched {doc_key} to row {best_match_idx+1} with score {best_match_score}")
                for reason in best_match_reasons:
                    logger.info(f"  - {reason}")
            else:
                # If no match based on identifiers, try filename matching as last resort
                logger.warning(f"No identifier match found for {doc_key}, trying filename matching")
                best_match_idx = None
                best_match_score = 0
                filename = doc_info['file_name'].lower()
                
                for excel_name_info in excel_names:
                    row_idx = excel_name_info['row_idx']
                    excel_variants = excel_name_info['variants']
                    
                    # Score based on name parts in filename
                    score = 0
                    matched_parts = []
                    
                    for variant in excel_variants:
                        if variant:
                            name_parts = set(re.findall(r'\b\w+\b', variant.lower()))
                            for part in name_parts:
                                if len(part) >= 3 and part in filename:
                                    score += 10  # 10 points per name part match
                                    matched_parts.append(part)
                    
                    if score > best_match_score:
                        best_match_score = score
                        best_match_idx = row_idx
                
                if best_match_idx is not None and best_match_score >= 10:
                    row_matches[best_match_idx].append(doc_key)
                    logger.info(f"Last-resort filename match for {doc_key} to row {best_match_idx+1} (score: {best_match_score})")
        
        # Save match details for debugging
        try:
            with open('document_match_details.json', 'w') as f:
                json.dump(match_details, f, indent=2, default=str)
        except:
            pass
        
        return row_matches


    def _create_document_matchers(self, extracted_data: Dict) -> Dict[str, str]:
        """Create document matchers for improved matching between document data and Excel rows."""
        matchers = {}
        
        # Passport number matching (highest priority)
        for field in ['passport_number', 'passport_no']:
            if field in extracted_data and extracted_data[field] != self.DEFAULT_VALUE:
                clean_val = re.sub(r'\s+', '', str(extracted_data[field])).upper()
                if clean_val:
                    matchers['passport'] = clean_val
                    logger.info(f"Added passport matcher: {clean_val}")
                    break
        
        # Emirates ID matching (high priority)
        for field in ['emirates_id', 'eid']:
            if field in extracted_data and extracted_data[field] != self.DEFAULT_VALUE:
                # Clean the Emirates ID - remove all non-digits
                clean_val = re.sub(r'[^0-9]', '', str(extracted_data[field]))
                if clean_val:
                    matchers['emirates_id'] = clean_val
                    logger.info(f"Added Emirates ID matcher: {clean_val}")
                    break
        
        # Name matching (full name preferred)
        if 'full_name' in extracted_data and extracted_data['full_name'] != self.DEFAULT_VALUE:
            matchers['full_name'] = extracted_data['full_name'].lower()
            logger.info(f"Added full name matcher: {matchers['full_name']}")
        
        # First and last name matching
        first_name = extracted_data.get('first_name', self.DEFAULT_VALUE)
        last_name = extracted_data.get('last_name', self.DEFAULT_VALUE)
        if first_name != self.DEFAULT_VALUE and last_name != self.DEFAULT_VALUE:
            matchers['first_name'] = first_name.lower()
            matchers['last_name'] = last_name.lower()
            logger.info(f"Added first name matcher: {first_name}")
            logger.info(f"Added last name matcher: {last_name}")
        
        # Unified number matching
        if 'unified_no' in extracted_data and extracted_data['unified_no'] != self.DEFAULT_VALUE:
            unified = re.sub(r'\s+', '', str(extracted_data['unified_no']))
            if unified:
                matchers['unified_no'] = unified
                logger.info(f"Added unified_no matcher: {unified}")
        
        return matchers

    def _calculate_document_match_score(self, excel_row: Dict, document_matchers: Dict) -> Tuple[int, List[str]]:
        """Calculate match score between Excel row and document matchers."""
        score = 0
        match_details = []
        
        # Skip matching if no matchers available
        if not document_matchers:
            return 0, []
        
        # Passport matching (worth 100 points - exact match required)
        if 'passport' in document_matchers:
            doc_passport = document_matchers['passport']
            # Check different possible field names in Excel
            for field in ['passport_no', 'Passport No', 'passport_number', 'PassportNo']:
                if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                    excel_passport = re.sub(r'\s+', '', str(excel_row[field])).upper()
                    if excel_passport and excel_passport == doc_passport:
                        score += 100
                        match_details.append(f"Passport matched: {doc_passport}")
                        break
        
        # Emirates ID matching (worth 100 points - exact match required)
        if 'emirates_id' in document_matchers:
            doc_eid = document_matchers['emirates_id']
            # Check different possible field names in Excel
            for field in ['emirates_id', 'Emirates Id', 'eid', 'EmiratesId']:
                if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                    # Clean the Excel Emirates ID - remove all non-digits
                    excel_eid = re.sub(r'[^0-9]', '', str(excel_row[field]))
                    if excel_eid and excel_eid == doc_eid:
                        score += 100
                        match_details.append(f"Emirates ID matched: {doc_eid}")
                        break
        
        # Unified number matching (worth 80 points - exact match required)
        if 'unified_no' in document_matchers:
            doc_unified = document_matchers['unified_no']
            # Check different possible field names in Excel
            for field in ['unified_no', 'Unified No', 'uid', 'Unified No.']:
                if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                    excel_unified = re.sub(r'\s+', '', str(excel_row[field]))
                    if excel_unified and excel_unified == doc_unified:
                        score += 80
                        match_details.append(f"Unified number matched: {doc_unified}")
                        break
        
        # Full name matching (worth 60 points for exact match, or partial based on word similarity)
        if 'full_name' in document_matchers:
            doc_name = document_matchers['full_name']
            # Try to construct full name from Excel
            excel_full_name = None
            
            # First try direct name field
            for field in ['full_name', 'Full Name', 'name', 'Name']:
                if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                    excel_full_name = str(excel_row[field]).lower()
                    break
            
            # If no direct name field, try to construct from first/last name
            if not excel_full_name:
                first = ""
                last = ""
                
                for field in ['first_name', 'First Name', 'firstname', 'FirstName']:
                    if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                        first = str(excel_row[field]).strip()
                        break
                
                for field in ['last_name', 'Last Name', 'lastname', 'LastName']:
                    if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                        last = str(excel_row[field]).strip()
                        break
                
                if first or last:
                    excel_full_name = f"{first} {last}".lower().strip()
            
            # Only run name matching if we have an Excel name
            if excel_full_name:
                # Check for exact match (60 points)
                if doc_name == excel_full_name:
                    score += 60
                    match_details.append(f"Full name exact match: {doc_name}")
                else:
                    # Check for partial word matches (up to 40 points)
                    doc_words = set(doc_name.lower().split())
                    excel_words = set(excel_full_name.lower().split())
                    
                    # Calculate intersection
                    matching_words = doc_words.intersection(excel_words)
                    
                    if matching_words:
                        # Calculate percentage of matching words
                        match_percentage = len(matching_words) / max(len(doc_words), len(excel_words))
                        word_score = int(match_percentage * 40)
                        score += word_score
                        match_details.append(f"Name partial match ({match_percentage:.2f}): {', '.join(matching_words)}")
        
        # First/Last name matching (worth up to 50 points)
        elif 'first_name' in document_matchers and 'last_name' in document_matchers:
            doc_first = document_matchers['first_name']
            doc_last = document_matchers['last_name']
            excel_first = None
            excel_last = None
            
            # Get Excel first name
            for field in ['first_name', 'First Name', 'firstname', 'FirstName']:
                if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                    excel_first = str(excel_row[field]).lower().strip()
                    break
            
            # Get Excel last name
            for field in ['last_name', 'Last Name', 'lastname', 'LastName']:
                if field in excel_row and excel_row[field] != self.DEFAULT_VALUE:
                    excel_last = str(excel_row[field]).lower().strip()
                    break
            
            # Match first name (worth 20 points)
            if excel_first and doc_first:
                if doc_first.lower() == excel_first.lower():
                    score += 20
                    match_details.append(f"First name matched: {doc_first}")
            
            # Match last name (worth 30 points - weighted higher as less likely to match randomly)
            if excel_last and doc_last:
                if doc_last.lower() == excel_last.lower():
                    score += 30
                    match_details.append(f"Last name matched: {doc_last}")
        
        return score, match_details

    def _clean_extracted_data(self, data: Dict) -> Dict:
        """Clean extracted data with standardization."""
        cleaned = {}
        for key, value in data.items():
            # Skip None values
            if value is None:
                continue
                
            normalized_key = self._normalize_field_name(key)
            
            # Special handling for specific fields
            if 'passport' in normalized_key and ('no' in normalized_key or 'number' in normalized_key):
                normalized_key = 'passport_number'
            elif 'emirates' in normalized_key and 'id' in normalized_key:
                normalized_key = 'emirates_id'
            elif 'dob' in normalized_key or ('date' in normalized_key and 'birth' in normalized_key):
                normalized_key = 'date_of_birth'
            elif 'unified' in normalized_key or 'uid' in normalized_key:
                normalized_key = 'unified_no'
            elif 'visa' in normalized_key and 'file' in normalized_key:
                normalized_key = 'visa_file_number'
            
            if isinstance(value, str):
                # Remove extra whitespace, normalize case
                cleaned_value = value.strip()
                cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
                
                # Format appropriately based on field type
                if normalized_key in self._date_fields:
                    cleaned_value = self._format_date_value(cleaned_value)
                elif normalized_key in self._numeric_fields:
                    cleaned_value = self._format_numeric_value(cleaned_value)
                    
                cleaned[normalized_key] = cleaned_value if cleaned_value else self.DEFAULT_VALUE
            else:
                # Handle non-string values
                if value is None:
                    cleaned[normalized_key] = self.DEFAULT_VALUE
                elif isinstance(value, (int, float)):
                    # Format appropriately for numbers
                    if normalized_key in self._date_fields:
                        # Handle Excel date numbers
                        cleaned[normalized_key] = self._format_excel_date(value)
                    else:
                        cleaned[normalized_key] = str(value)
                else:
                    # Convert other types to string
                    cleaned[normalized_key] = str(value)
                        
        return cleaned
    
    def _format_output_value(self, value, field_name):
        """Format output value with proper default handling."""
        # Only Middle Name can have '.' as default
        if field_name.lower() == 'middle_name' and (value is None or value == '' or value == self.DEFAULT_VALUE):
            return self.DEFAULT_VALUE
            
        # For other fields, use empty string instead of '.'
        if value == self.DEFAULT_VALUE:
            return ''
            
        return value

    def _clean_excel_data(self, data: Dict) -> Dict:
        """Clean Excel data with better handling of special values and dates."""
        cleaned = {}
        date_fields = ['effective_date', 'dob', 'passport_expiry_date', 'visa_expiry_date']
        
        for key, value in data.items():
            # Normalize key
            normalized_key = self._normalize_field_name(key)
            
            # Skip unnamed columns
            if normalized_key.startswith('unnamed'):
                continue
                
            # Handle different value types
            if pd.isna(value) or value == '' or str(value).lower() == 'nan':
                cleaned[normalized_key] = self.DEFAULT_VALUE
            elif isinstance(value, (int, float)):
                if normalized_key in date_fields and isinstance(value, float):
                    # Convert Excel date number to date string
                    cleaned[normalized_key] = self._format_excel_date(value)
                elif np.isclose(value, int(value)):
                    cleaned[normalized_key] = str(int(value))
                else:
                    cleaned[normalized_key] = str(value)
            elif isinstance(value, datetime):
                if normalized_key in date_fields:
                    cleaned[normalized_key] = value.strftime('%d/%m/%Y')
                else:
                    cleaned[normalized_key] = value.strftime('%Y-%m-%d')
            else:
                str_value = str(value).strip()
                
                # Handle date fields specially
                if normalized_key in date_fields:
                    try:
                        # Try to parse as date
                        date_obj = pd.to_datetime(str_value, dayfirst=True)
                        cleaned[normalized_key] = date_obj.strftime('%d/%m/%Y')
                    except:
                        cleaned[normalized_key] = str_value
                else:
                    cleaned[normalized_key] = str_value
                    
        return cleaned
    
    def _format_emirates_id(self, eid: str) -> str:
        """Format Emirates ID to include hyphens and validate format."""
        if not eid or eid == self.DEFAULT_VALUE or pd.isna(eid):
            return ''
            
        # Remove any non-digit or non-hyphen characters
        cleaned = re.sub(r'[^0-9\-]', '', str(eid))
        
        # Check if it already has hyphens
        if '-' not in cleaned and len(cleaned) == 15:
            # Format with hyphens in correct positions
            cleaned = f"{cleaned[:3]}-{cleaned[3:7]}-{cleaned[7:14]}-{cleaned[14]}"
        
        # Check if it starts with 784
        if not cleaned.startswith('784'):
            logger.info(f"Emirates ID '{cleaned}' doesn't start with 784, replacing with default ID")
            return '111-1111-1111111-1'
        
        return cleaned
    
    def _format_visa_file_number(self, value: str) -> str:
        """Ensures visa file number is in the proper format (XXX/YYYY/ZZZZZZ)."""
        if not value or value == self.DEFAULT_VALUE or pd.isna(value):
            return self.DEFAULT_VALUE
            
        value_str = str(value).strip()
        
        # If it already has the right format (contains slashes), return as is
        if '/' in value_str:
            return value_str
            
        # Try to format as visa file number if it's a long numeric string
        digits = ''.join(filter(str.isdigit, value_str))
        if len(digits) >= 10:
            # Extract potential 3-digit prefix (20 for Dubai, 10 for Abu Dhabi)
            prefix = digits[:3]
            if prefix.startswith('20') or prefix.startswith('10'):
                # Try to determine year part (4 digits) - typically current or previous year
                current_year = datetime.now().year
                possible_years = [str(y) for y in range(current_year-5, current_year+1)]
                
                # Look for one of these years in the digits
                year_part = ""
                for year in possible_years:
                    if year in digits[3:]:
                        year_part = year
                        year_pos = digits.find(year, 3)
                        remaining = digits[year_pos + 4:]
                        
                        # Format as XXX/YYYY/ZZZZZZ
                        return f"{prefix}/{year_part}/{remaining}"
        
        # If we can't format it properly, return as is
        return value_str


    def _combine_row_data(self, extracted: Dict, excel: Dict, document_paths: Dict[str, Any] = None) -> Dict:
        """Combine data with improved priority rules and field mapping."""
        # Start with a deep copy of Excel data
        combined = copy.deepcopy(excel)
        
        # CRITICAL FIX: Save all original Excel values for preservation
        original_excel_values = {
            'first_name': excel.get('first_name', self.DEFAULT_VALUE),
            'middle_name': excel.get('middle_name', self.DEFAULT_VALUE),
            'last_name': excel.get('last_name', self.DEFAULT_VALUE),
            'First Name': excel.get('First Name', self.DEFAULT_VALUE),
            'Middle Name': excel.get('Middle Name', self.DEFAULT_VALUE),
            'Last Name': excel.get('Last Name', self.DEFAULT_VALUE),
            'Staff ID': excel.get('Staff ID', self.DEFAULT_VALUE),
            'Family No.': excel.get('Family No.', self.DEFAULT_VALUE),
            'Email': excel.get('Email', self.DEFAULT_VALUE),
            'Mobile No': excel.get('Mobile No', self.DEFAULT_VALUE),
            'Contract Name': excel.get('Contract Name', self.DEFAULT_VALUE),
            'DOB': excel.get('DOB', self.DEFAULT_VALUE),
            'Gender': excel.get('Gender', self.DEFAULT_VALUE),
            'Nationality': excel.get('Nationality', self.DEFAULT_VALUE),
            'Emirates Id': excel.get('Emirates Id', self.DEFAULT_VALUE),
            'Unified No': excel.get('Unified No', self.DEFAULT_VALUE),
            'Passport No': excel.get('Passport No', self.DEFAULT_VALUE),
            'Visa File Number': excel.get('Visa File Number', self.DEFAULT_VALUE)
        }

        # Log the preserved Excel values for debugging
        preserved_fields = {k: v for k, v in original_excel_values.items() if v != self.DEFAULT_VALUE and v != ''}
        if preserved_fields:
            logger.info("Preserving original Excel values:")
            for field, value in preserved_fields.items():
                logger.info(f"  - {field}: {value}")
        
        # CRITICAL: Save original name fields from Excel to preserve them
        original_names = {
            'first_name': combined.get('first_name', self.DEFAULT_VALUE),
            'middle_name': combined.get('middle_name', self.DEFAULT_VALUE),
            'last_name': combined.get('last_name', self.DEFAULT_VALUE),
            'First Name': combined.get('First Name', self.DEFAULT_VALUE),
            'Middle Name': combined.get('Middle Name', self.DEFAULT_VALUE),
            'Last Name': combined.get('Last Name', self.DEFAULT_VALUE)
        }

        # Log original names for debugging
        if any(v != self.DEFAULT_VALUE for v in original_names.values()):
            logger.info("Original name fields from Excel:")
            for field, value in original_names.items():
                if value != self.DEFAULT_VALUE:
                    logger.info(f"  - {field}: {value}")
        
        # CRITICAL: Save original extracted values for logging and debugging
        original_extracted = {
            'unified_no': extracted.get('unified_no', self.DEFAULT_VALUE),
            'visa_file_number': extracted.get('visa_file_number', self.DEFAULT_VALUE),
            'emirates_id': extracted.get('emirates_id', self.DEFAULT_VALUE),
            'passport_number': extracted.get('passport_number', self.DEFAULT_VALUE)
        }
        
        logger.info("=" * 80)
        logger.info("FIELD PRESERVATION CHECK")
        logger.info("=" * 80)
        for field, value in original_extracted.items():
            if value != self.DEFAULT_VALUE:
                logger.info(f"Original extracted {field}: {value}")
        
        # HIGHEST PRIORITY: Apply critical fields directly with specific mapping
        critical_fields = {
            'unified_no': ['unified_no', 'Unified No'],
            'visa_file_number': ['visa_file_number', 'Visa File Number'],
            'emirates_id': ['emirates_id', 'Emirates Id'],
            'passport_number': ['passport_no', 'Passport No', 'passport_number', 'Passport Number']
        }
        
        # Apply critical fields first before any other processing
        for source_field, target_fields in critical_fields.items():
            if source_field in extracted and extracted[source_field] != self.DEFAULT_VALUE:
                value = extracted[source_field]
                
                # Apply special formatting if needed
                if source_field == 'emirates_id':
                    value = self._process_emirates_id(value)
                elif source_field == 'visa_file_number':
                    # Make sure visa file number format is preserved
                    if '/' not in value:
                        logger.warning(f"Visa file number has incorrect format (no slashes): {value}")
                    
                # Set all target fields to ensure consistency
                for target_field in target_fields:
                    combined[target_field] = value
                    logger.info(f"DIRECT MAPPING: {source_field} → {target_field}: {value}")
        
        # Document-specific priority rules based on document type
        passport_priority_fields = ['passport_number', 'passport_no', 'surname', 'given_names', 
                                'first_name', 'last_name', 'nationality', 'date_of_birth', 'gender', 'sex']
                                
        visa_priority_fields = ['entry_permit_no', 'unified_no', 'visa_file_number', 'full_name',
                            'sponsor_name', 'profession', 'visa_type']
        
        # Determine document type from extracted fields
        doc_type = None
        if 'passport_number' in extracted and extracted['passport_number'] != self.DEFAULT_VALUE:
            doc_type = 'passport'
        elif 'emirates_id' in extracted and extracted['emirates_id'] != self.DEFAULT_VALUE:
            doc_type = 'emirates_id'
        elif any(key in extracted and extracted[key] != self.DEFAULT_VALUE for key in ['entry_permit_no', 'visa_file_number', 'unified_no']):
            doc_type = 'visa'
            
        logger.info(f"Detected document type: {doc_type}")
        
        # Apply document-specific priorities
        if doc_type == 'passport':
            # Give passport fields highest priority
            for field in passport_priority_fields:
                if field in extracted and extracted[field] != self.DEFAULT_VALUE:
                    # Map field to Excel columns
                    if field == 'passport_number':
                        combined['passport_no'] = extracted[field]
                        combined['Passport No'] = extracted[field]
                    elif field == 'passport_no':
                        combined['passport_number'] = extracted[field]
                        combined['Passport No'] = extracted[field]
                    elif field == 'given_names':
                        combined['first_name'] = extracted[field]
                        combined['First Name'] = extracted[field]
                    elif field == 'surname':
                        combined['last_name'] = extracted[field]
                        combined['Last Name'] = extracted[field]
                    elif field == 'date_of_birth':
                        combined['dob'] = extracted[field]
                        combined['DOB'] = extracted[field]
                    elif field == 'gender' or field == 'sex':
                        combined['gender'] = extracted[field]
                        combined['Gender'] = extracted[field]
                    elif field == 'nationality':
                        combined['nationality'] = extracted[field]
                        combined['Nationality'] = extracted[field]
                    else:
                        # Direct field mapping
                        combined[field] = extracted[field]
                        # Also try Excel column names
                        excel_field = field.replace('_', ' ').title()
                        combined[excel_field] = extracted[field]
        
        elif doc_type == 'visa':
            # Ensure visa file number and unified no are set from any available source properly
            
            # First, properly handle entry_permit_no - based on its format
            if 'entry_permit_no' in extracted and extracted['entry_permit_no'] != self.DEFAULT_VALUE:
                entry_permit = extracted['entry_permit_no']
                
                # Validate format to determine if it's a visa file number or unified number
                if '/' in entry_permit:
                    # Entry permit with slashes should go to visa_file_number
                    if 'visa_file_number' not in combined or combined['visa_file_number'] == self.DEFAULT_VALUE:
                        combined['visa_file_number'] = entry_permit
                        combined['Visa File Number'] = entry_permit
                        logger.info(f"Combined data: Set visa_file_number from entry_permit_no: {entry_permit}")
                elif entry_permit.isdigit() and len(entry_permit) >= 8:
                    # Entry permit with only digits should go to unified_no
                    if 'unified_no' not in combined or combined['unified_no'] == self.DEFAULT_VALUE:
                        combined['unified_no'] = entry_permit
                        combined['Unified No'] = entry_permit
                        logger.info(f"Combined data: Set unified_no from digit-only entry_permit_no: {entry_permit}")
                else:
                    # If format is ambiguous, default to visa_file_number
                    if 'visa_file_number' not in combined or combined['visa_file_number'] == self.DEFAULT_VALUE:
                        combined['visa_file_number'] = entry_permit
                        combined['Visa File Number'] = entry_permit
                        logger.info(f"Combined data: Set visa_file_number from ambiguous entry_permit_no: {entry_permit}")
            
            # Next, check for file fields and use for visa_file_number if needed
            for field in ['file', 'file_no', 'file_number']:
                if field in extracted and extracted[field] != self.DEFAULT_VALUE:
                    file_value = extracted[field]
                    
                    # Files should always have slashes for visa file number
                    if 'visa_file_number' not in combined or combined['visa_file_number'] == self.DEFAULT_VALUE:
                        combined['visa_file_number'] = file_value
                        combined['Visa File Number'] = file_value
                        logger.info(f"Combined data: Set visa_file_number from {field}: {file_value}")
            
            # Check for unified number variants - must be digits only
            for field in ['unified_no', 'uid', 'u.i.d._no.', 'unified_number', 'unified']:
                if field in extracted and extracted[field] != self.DEFAULT_VALUE:
                    unified_value = extracted[field]
                    
                    # Validate unified number format - should not contain slashes
                    if '/' in unified_value:
                        logger.warning(f"Invalid format for unified_no from {field}: {unified_value} - contains slashes")
                        
                        # Move to visa_file_number if appropriate
                        if 'visa_file_number' not in combined or combined['visa_file_number'] == self.DEFAULT_VALUE:
                            combined['visa_file_number'] = unified_value
                            combined['Visa File Number'] = unified_value
                            logger.info(f"Combined data: Moved slash-containing value to visa_file_number: {unified_value}")
                        
                        # Extract digits for unified_no
                        digits = ''.join(filter(str.isdigit, unified_value))
                        if len(digits) >= 8:
                            combined['unified_no'] = digits
                            combined['Unified No'] = digits
                            logger.info(f"Combined data: Set unified_no using extracted digits: {digits}")
                    else:
                        # Valid format with no slashes
                        combined['unified_no'] = unified_value
                        combined['Unified No'] = unified_value
                        logger.info(f"Combined data: Set unified_no from {field}: {unified_value}")
            
            # For common fields that appear in both passport and visa, use visa data only if passport data not available
            common_fields = ['passport_number', 'nationality', 'date_of_birth', 'gender']
            for field in common_fields:
                if field in extracted and extracted[field] != self.DEFAULT_VALUE:
                    # Only use if not already populated by passport
                    if field not in combined or combined[field] == self.DEFAULT_VALUE:
                        if field == 'passport_number':
                            combined['passport_no'] = extracted[field]
                            combined['Passport No'] = extracted[field]
                        elif field == 'date_of_birth':
                            combined['dob'] = extracted[field]
                            combined['DOB'] = extracted[field]
                        elif field == 'nationality':
                            combined['nationality'] = extracted[field]
                            combined['Nationality'] = extracted[field]
                        else:
                            combined[field] = extracted[field]
                            excel_field = field.replace('_', ' ').title()
                            combined[excel_field] = extracted[field]
        
        # First, format Emirates ID in Excel data if present
        if 'emirates_id' in combined and combined['emirates_id'] != self.DEFAULT_VALUE:
            combined['emirates_id'] = self._format_emirates_id(combined['emirates_id'])
                
        # Make sure Emirates ID is properly processed
        if 'Emirates Id' in combined and combined['Emirates Id'] != self.DEFAULT_VALUE:
            combined['Emirates Id'] = self._process_emirates_id(combined['Emirates Id'])
        
        # Identify special fields that require custom handling
        name_fields = ['first_name', 'middle_name', 'last_name', 'full_name']
        id_fields = ['passport_number', 'passport_no', 'emirates_id', 'entry_permit_no', 'unified_no']
        date_fields = ['dob', 'date_of_birth', 'effective_date', 'passport_expiry_date', 'visa_expiry_date']
        
        # Explicitly map Textract fields to Excel fields - critical for proper mapping
        critical_mappings = {
            'nationality': ['Nationality', 'Country'],
            'passport_number': ['Passport No', 'PassportNum'],
            'emirates_id': ['Emirates Id', 'EIDNumber'],
            'visa_file_number': ['Visa File Number', 'ResidentFileNumber'],
            'visa_number': ['Visa File Number', 'ResidentFileNumber'],
            'unified_no': ['Unified No', 'UIDNo'],
            'u.i.d._no.': ['Unified No', 'UIDNo'],
            'profession': 'Occupation',
            'date_of_birth': 'DOB',
            'name': 'First Name',  # Map full name to first name if needed
        }
        
        # Apply all critical mappings with direct field names
        for ext_field, excel_fields in critical_mappings.items():
            if ext_field in extracted and extracted[ext_field] != self.DEFAULT_VALUE:
                # Handle both single field and list of fields
                if isinstance(excel_fields, list):
                    for excel_field in excel_fields:
                        if excel_field not in combined or combined[excel_field] == self.DEFAULT_VALUE or pd.isna(combined[excel_field]):
                            combined[excel_field] = extracted[ext_field]
                            logger.info(f"Critical mapping: {ext_field} -> {excel_field}: {extracted[ext_field]}")
                else:
                    excel_field = excel_fields
                    if excel_field not in combined or combined[excel_field] == self.DEFAULT_VALUE or pd.isna(combined[excel_field]):
                        combined[excel_field] = extracted[ext_field]
                        logger.info(f"Critical mapping: {ext_field} -> {excel_field}: {extracted[ext_field]}")
        
        # DOB handling with more explicit handling
        if 'dob' in excel and excel['dob'] not in [self.DEFAULT_VALUE, '', None, 'nan']:
            combined['DOB'] = self._format_date_value(excel['dob'])
        elif 'DOB' in excel and excel['DOB'] not in [self.DEFAULT_VALUE, '', None, 'nan']:
            combined['DOB'] = self._format_date_value(excel['DOB'])
        elif 'date_of_birth' in extracted and extracted['date_of_birth'] != self.DEFAULT_VALUE:
            combined['DOB'] = self._format_date_value(extracted['date_of_birth'])
        
        # First, handle date fields from Excel data to ensure proper format
        for field in date_fields:
            if field in combined and combined[field] not in [self.DEFAULT_VALUE, '', None, 'nan']:
                combined[field] = self._format_date_value(combined[field])
        
        # Process special name fields
        if 'full_name' in extracted and extracted['full_name'] != self.DEFAULT_VALUE:
            self._split_full_name(extracted['full_name'], combined)
        
        # Process name components if available
        if 'given_names' in extracted and 'surname' in extracted:
            if extracted['given_names'] != self.DEFAULT_VALUE and extracted['surname'] != self.DEFAULT_VALUE:
                self._handle_passport_names(extracted['given_names'], extracted['surname'], combined)
        
        # Handle name from extraction if Excel has it
        if 'name' in extracted and extracted['name'] != self.DEFAULT_VALUE:
            # Check Excel fields
            if 'First Name' in combined and 'Last Name' in combined:
                # If both fields are empty or default, use extracted name
                first_empty = not combined['First Name'] or combined['First Name'] == self.DEFAULT_VALUE
                last_empty = not combined['Last Name'] or combined['Last Name'] == self.DEFAULT_VALUE
                
                if first_empty and last_empty:
                    name_parts = extracted['name'].split()
                    if len(name_parts) >= 2:
                        combined['First Name'] = name_parts[0]
                        combined['Last Name'] = name_parts[-1]
                        if len(name_parts) > 2:
                            combined['Middle Name'] = ' '.join(name_parts[1:-1])
                    else:
                        combined['First Name'] = extracted['name']
        
        # Priority check for ID fields - but preserve Excel data if extracted is empty
        for id_field in id_fields:
            if id_field in extracted and extracted[id_field] != self.DEFAULT_VALUE:
                value = extracted[id_field]
                if id_field == 'emirates_id':
                    value = self._format_emirates_id(value)
                
                # Only use OCR data if Excel data is empty or if it's a passport/visa number
                if (id_field in combined and combined[id_field] == self.DEFAULT_VALUE) or \
                (id_field not in ['emirates_id']):  # Don't override Emirates ID from Excel
                    combined[id_field] = value
        
        # Make sure DOB and date_of_birth are synchronized
        if 'DOB' in combined and combined['DOB'] != self.DEFAULT_VALUE:
            if 'date_of_birth' not in combined or combined['date_of_birth'] == self.DEFAULT_VALUE:
                combined['date_of_birth'] = combined['DOB']
        elif 'date_of_birth' in combined and combined['date_of_birth'] != self.DEFAULT_VALUE:
            combined['DOB'] = combined['date_of_birth']
                    
        if 'first_name' in combined and combined['first_name'] != self.DEFAULT_VALUE:
            # Check if we have a combined name scenario
            combined_name = combined['first_name']
            
            # If this looks like a combined name, and either middle or last name is missing
            if (len(combined_name.split()) > 1 and
                (('middle_name' not in combined or combined['middle_name'] == self.DEFAULT_VALUE) or
                ('last_name' not in combined or combined['last_name'] == self.DEFAULT_VALUE))):
                
                logger.info(f"Detected potential combined name in first_name field: {combined_name}")
                first, middle, last = self._split_combined_name(combined_name)
                
                # Always update first name with the proper value
                combined['first_name'] = first
                
                # Only update middle and last if they're missing or default
                if 'middle_name' not in combined or combined['middle_name'] == self.DEFAULT_VALUE:
                    combined['middle_name'] = middle
                    if middle != self.DEFAULT_VALUE:
                        logger.info(f"Set middle_name to: {middle}")
                        
                if 'last_name' not in combined or combined['last_name'] == self.DEFAULT_VALUE:
                    combined['last_name'] = last
                    if last != self.DEFAULT_VALUE:
                        logger.info(f"Set last_name to: {last}")
                        
                logger.info(f"Split name into first: {first}, middle: {middle}, last: {last}")
        
        # Same for First Name/Last Name (Excel column names)
        if 'First Name' in combined and combined['First Name'] != self.DEFAULT_VALUE:
            combined_name = combined['First Name']
            
            # If this looks like a combined name, and Last Name is missing
            if len(combined_name.split()) > 1 and ('Last Name' not in combined or combined['Last Name'] == self.DEFAULT_VALUE):
                name_parts = combined_name.split()
                combined['First Name'] = name_parts[0]
                
                if len(name_parts) > 2:
                    combined['Middle Name'] = ' '.join(name_parts[1:-1])
                    combined['Last Name'] = name_parts[-1]
                else:
                    combined['Last Name'] = name_parts[-1]
                        
        # CRITICAL FIX: Always preserve original Contract Name from Excel data
        original_contract_name = None

        # First check if the Excel data directly contains Contract Name
        if 'Contract Name' in excel and excel['Contract Name'] and excel['Contract Name'] != self.DEFAULT_VALUE:
            original_contract_name = excel['Contract Name']
            logger.info(f"Found Contract Name in Excel (direct): {original_contract_name}")
        # Also check lowercase version as fallback
        elif 'contract_name' in excel and excel['contract_name'] and excel['contract_name'] != self.DEFAULT_VALUE:
            original_contract_name = excel['contract_name']
            logger.info(f"Found contract_name in Excel (lowercase): {original_contract_name}")

        # If we have an original Contract Name from Excel, use it (highest priority)
        if original_contract_name:
            if 'Contract Name' in combined:
                combined['Contract Name'] = original_contract_name
            if 'contract_name' in combined:
                combined['contract_name'] = original_contract_name
            logger.info(f"PRESERVED original Contract Name from Excel: {original_contract_name}")
        # Otherwise, try to set from sponsor_name if available
        elif 'sponsor_name' in extracted and extracted['sponsor_name'] != self.DEFAULT_VALUE:
            employer_name = extracted['sponsor_name']
            valid_contracts = [
                "Farnek Services - LSB",
                "Dreshak Maintenance LLC",
                "Farnek Manpower Supply Services",
                "Farnek Middle East LLC",
                "Farnek Security Services LLC -Dubai",
                "Farnek Security Systems Consultancy LLC",
                "Farnek Services LLC",
                "Farnek Services LLC Branch",
                "Smashing Cleaning Services LLC"
            ]
            
            if 'FARNEK' in employer_name.upper():
                for contract in valid_contracts:
                    if 'FARNEK' in contract.upper():
                        if 'Contract Name' in combined:
                            combined['Contract Name'] = contract
                        if 'contract_name' in combined:
                            combined['contract_name'] = contract
                        logger.info(f"Set Contract Name from sponsor_name: {contract}")
                        break
            elif 'DRESHAK' in employer_name.upper():
                if 'Contract Name' in combined:
                    combined['Contract Name'] = "Dreshak Maintenance LLC"
                if 'contract_name' in combined:
                    combined['contract_name'] = "Dreshak Maintenance LLC"
                logger.info(f"Set Contract Name from sponsor_name: Dreshak Maintenance LLC")
                        
        # Check for visa file number and set visa issuance emirate
        if 'visa_file_number' in combined and combined['visa_file_number'] != self.DEFAULT_VALUE:
            visa_number = combined['visa_file_number']
            
            # Remove any non-digit characters to extract just the numbers
            digits = ''.join(filter(str.isdigit, str(visa_number)))
            
            # Check if it starts with specific digits
            if digits.startswith('20'):
                logger.info(f"Visa file number {visa_number} starts with 20, setting emirate to Dubai")
                combined['visa_issuance_emirate'] = 'Dubai'
                combined['Visa Issuance Emirate'] = 'Dubai'
                combined['work_emirate'] = 'Dubai'
                combined['residence_emirate'] = 'Dubai'
                combined['work_region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['residence_region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['member_type'] = 'Expat whose residence issued in Dubai'
                
                # Also set Excel column names (important for final output)
                combined['Work Emirate'] = 'Dubai'
                combined['Residence Emirate'] = 'Dubai'
                combined['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['Member Type'] = 'Expat whose residence issued in Dubai'
                
            elif digits.startswith('10'):
                logger.info(f"Visa file number {visa_number} starts with 10, setting emirate to Abu Dhabi")
                combined['visa_issuance_emirate'] = 'Abu Dhabi'
                combined['Visa Issuance Emirate'] = 'Abu Dhabi'
                combined['work_emirate'] = 'Abu Dhabi'
                combined['residence_emirate'] = 'Abu Dhabi'
                combined['work_region'] = 'Al Ain City'
                combined['residence_region'] = 'Al Ain City'
                combined['member_type'] = 'Expat whose residence issued other than Dubai'
                
                # Also set Excel column names (important for final output)
                combined['Work Emirate'] = 'Abu Dhabi'
                combined['Residence Emirate'] = 'Abu Dhabi'
                combined['Work Region'] = 'Al Ain City'
                combined['Residence Region'] = 'Al Ain City'
                combined['Member Type'] = 'Expat whose residence issued other than Dubai'
                    
        # Make sure effective date is set
        if 'effective_date' not in combined or combined['effective_date'] == self.DEFAULT_VALUE:
            combined['effective_date'] = datetime.now().strftime('%d/%m/%Y')
            logger.info(f"Setting default effective_date to today: {combined['effective_date']}")
            
        # Also check Effective Date (Excel column name)
        if 'Effective Date' not in combined or combined['Effective Date'] == self.DEFAULT_VALUE:
            combined['Effective Date'] = datetime.now().strftime('%d/%m/%Y')
                        
        # Family No = Staff ID
        if 'staff_id' in combined and combined['staff_id'] != self.DEFAULT_VALUE:
            combined['family_no'] = combined['staff_id']
            logger.info(f"Set family_no to match staff_id: {combined['staff_id']}")
        
        if 'Staff ID' in combined and combined['Staff ID'] != self.DEFAULT_VALUE:
            combined['Family No.'] = combined['Staff ID']

        # Work and residence country
        combined['work_country'] = 'United Arab Emirates'
        combined['residence_country'] = 'United Arab Emirates'
        combined['Work Country'] = 'United Arab Emirates'
        combined['Residence Country'] = 'United Arab Emirates'

        # Commission
        combined['commission'] = 'NO'
        combined['Commission'] = 'NO'

        # Handle Mobile No format
        if 'mobile_no' in combined and combined['mobile_no'] != self.DEFAULT_VALUE:
            # Extract just the digits
            digits = ''.join(filter(str.isdigit, str(combined['mobile_no'])))
            # Take last 9 digits
            if len(digits) >= 9:
                combined['mobile_no'] = digits[-9:]
            logger.info(f"Formatted mobile_no: {combined['mobile_no']}")
        
        # Same for Mobile No (Excel column name)
        if 'Mobile No' in combined and combined['Mobile No'] != self.DEFAULT_VALUE:
            digits = ''.join(filter(str.isdigit, str(combined['Mobile No'])))
            if len(digits) >= 9:
                combined['Mobile No'] = digits[-9:]

        # Handle emirate-based fields
        if 'visa_issuance_emirate' in combined:
            issuance_emirate = combined['visa_issuance_emirate']
            
            if issuance_emirate == 'Dubai':
                combined['work_emirate'] = 'Dubai'
                combined['residence_emirate'] = 'Dubai'
                combined['work_region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['residence_region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['member_type'] = 'Expat whose residence issued in Dubai'
                
                # Also set Excel column names
                combined['Work Emirate'] = 'Dubai'
                combined['Residence Emirate'] = 'Dubai'
                combined['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['Member Type'] = 'Expat whose residence issued in Dubai'
                
            elif issuance_emirate:  # Any other emirate
                combined['work_emirate'] = issuance_emirate
                combined['residence_emirate'] = issuance_emirate
                combined['work_region'] = 'Al Ain City'
                combined['residence_region'] = 'Al Ain City'
                combined['member_type'] = 'Expat whose residence issued other than Dubai'
                
                # Also set Excel column names
                combined['Work Emirate'] = issuance_emirate
                combined['Residence Emirate'] = issuance_emirate
                combined['Work Region'] = 'Al Ain City'
                combined['Residence Region'] = 'Al Ain City'
                combined['Member Type'] = 'Expat whose residence issued other than Dubai'
                    
        if 'effective_date' in combined and 'Effective Date' in combined:
            # Keep only one effective date field - prefer Effective Date (Excel column)
            if combined['Effective Date'] != self.DEFAULT_VALUE:
                combined.pop('effective_date', None)
            elif combined['effective_date'] != self.DEFAULT_VALUE:
                combined['Effective Date'] = combined['effective_date']
                combined.pop('effective_date', None)

        # Company phone and email
        if 'mobile_no' in combined and combined['mobile_no'] != self.DEFAULT_VALUE:
            if 'company_phone' not in combined or combined['company_phone'] == self.DEFAULT_VALUE:
                combined['company_phone'] = combined['mobile_no']
        
        if 'Mobile No' in combined and combined['Mobile No'] != self.DEFAULT_VALUE:
            if 'Company Phone' not in combined or combined['Company Phone'] == self.DEFAULT_VALUE:
                combined['Company Phone'] = combined['Mobile No']

        if 'email' in combined and combined['email'] != self.DEFAULT_VALUE:
            if 'company_mail' not in combined or combined['company_mail'] == self.DEFAULT_VALUE:
                combined['company_mail'] = combined['email']
        
        if 'Email' in combined and combined['Email'] != self.DEFAULT_VALUE:
            if 'Company Mail' not in combined or combined['Company Mail'] == self.DEFAULT_VALUE:
                combined['Company Mail'] = combined['Email']
        
        # Make sure effective date is set only in the correct field
        if 'effective_date' in combined and 'Effective Date' in combined:
            # If Effective Date is empty but effective_date has value, use it
            if combined['Effective Date'] == self.DEFAULT_VALUE and combined['effective_date'] != self.DEFAULT_VALUE:
                combined['Effective Date'] = combined['effective_date']
            # Always remove lowercase version to avoid duplication
            combined.pop('effective_date', None)
            logger.info("Removed duplicate 'effective_date' field")
        
        # Final check: Prevent unified_no from being visa_file_number with slashes removed
        if ('unified_no' in combined and 'visa_file_number' in combined and
            combined['unified_no'] != self.DEFAULT_VALUE and combined['visa_file_number'] != self.DEFAULT_VALUE):
            
            unified = combined['unified_no']
            visa_file = combined['visa_file_number']
            visa_file_no_slashes = visa_file.replace('/', '')
            
            if unified == visa_file_no_slashes:
                logger.warning(f"FINAL CHECK: unified_no exactly matches visa_file_number with slashes removed!")
                logger.warning(f"  unified_no: {unified}")
                logger.warning(f"  visa_file_number: {visa_file}")
                
                # IMPORTANT: Clear the invalid unified_no!
                combined['unified_no'] = self.DEFAULT_VALUE
                combined['Unified No'] = self.DEFAULT_VALUE
                logger.info("Cleared invalid unified_no to prevent incorrect data")
        
        # Check if we're missing both unified_no and visa_file_number
        if (('unified_no' not in combined or combined['unified_no'] == self.DEFAULT_VALUE) and
            ('visa_file_number' not in combined or combined['visa_file_number'] == self.DEFAULT_VALUE)):
            
            logger.warning("Both unified_no and visa_file_number are missing or default!")
            
            # If we have the document paths, we might be able to extract from the filename
            if document_paths and 'visa' in document_paths:
                visa_paths = document_paths['visa']
                # Handle both list and string cases
                paths_to_check = visa_paths if isinstance(visa_paths, list) else [visa_paths]
                
                for path in paths_to_check:
                    filename = os.path.basename(path)
                    logger.info(f"Attempting to extract info from filename: {filename}")
                    
                    # Look for pattern XXX/YYYY/ZZZZZ in filename
                    slash_pattern = re.search(r'(\d{2,3}/\d{4}/\d+)', filename)
                    if slash_pattern:
                        potential_visa_file = slash_pattern.group(1)
                        logger.info(f"Found potential visa file number in filename: {potential_visa_file}")
                        combined['visa_file_number'] = potential_visa_file
                        combined['Visa File Number'] = potential_visa_file
                        
                    # Look for 8-11 digit sequence that might be unified number
                    digit_pattern = re.search(r'(\b\d{8,11}\b)', filename)
                    if digit_pattern:
                        potential_unified = digit_pattern.group(1)
                        # Only use if it doesn't look like visa file number without slashes
                        if '/' not in potential_unified and (not slash_pattern or potential_unified != slash_pattern.group(1).replace('/', '')):
                            logger.info(f"Found potential unified number in filename: {potential_unified}")
                            combined['unified_no'] = potential_unified
                            combined['Unified No'] = potential_unified
        
        # Log final critical field values
        logger.info("-" * 80)
        logger.info("FINAL COMBINED DATA (CRITICAL FIELDS)")
        logger.info("-" * 80)
        
        important_output_fields = [
            'Passport No', 'Emirates Id', 'Unified No', 'Visa File Number',
            'First Name', 'Last Name', 'Nationality', 'DOB', 'Effective Date'
        ]
        
        for field in important_output_fields:
            if field in combined:
                logger.info(f"{field}: {combined[field]}")
        
        # Validate and fix critical IDs
        combined = self._validate_id_fields(combined)
        
        # CRITICAL FIX: Restore original Excel name fields if they were present
        for field, value in original_names.items():
            if value != self.DEFAULT_VALUE and value != '':
                # Only restore if Excel had a value
                if field in combined:
                    if combined[field] != value:
                        logger.info(f"Restoring original Excel name field: {field} = {value} (was {combined[field]})")
                        combined[field] = value

        # Additional safety check for First/Last Name fields
        if 'First Name' in combined and original_names['First Name'] != self.DEFAULT_VALUE:
            combined['First Name'] = original_names['First Name']
        if 'Last Name' in combined and original_names['Last Name'] != self.DEFAULT_VALUE:
            combined['Last Name'] = original_names['Last Name']
        if 'Middle Name' in combined and original_names['Middle Name'] != self.DEFAULT_VALUE:
            combined['Middle Name'] = original_names['Middle Name']
        
        # CRITICAL FIX: Restore all original Excel values for key fields
        for field, value in original_excel_values.items():
            if field in combined and value != self.DEFAULT_VALUE and value != '':
                if combined[field] != value:
                    logger.info(f"Restoring original Excel value for {field}: {value} (was {combined[field]})")
                    combined[field] = value

        # Double-check critical name fields
        name_fields = ['First Name', 'Middle Name', 'Last Name']
        for field in name_fields:
            if field in combined and field in original_excel_values and original_excel_values[field] != self.DEFAULT_VALUE:
                if combined[field] != original_excel_values[field]:
                    logger.info(f"Final check - restoring {field}: {original_excel_values[field]}")
                    combined[field] = original_excel_values[field]
        
        # CRITICAL FIX: Ensure original Excel values are not lost in the final output
        for field, value in original_excel_values.items():
            if field in ['First Name', 'Middle Name', 'Last Name', 'Contract Name', 'DOB', 'Gender', 'Nationality', 'Passport No', 'Emirates Id', 'Unified No', 'Visa File Number']:
                # Always keep the original values for these critical fields if they exist
                if field in combined and value and value != self.DEFAULT_VALUE and value != '':
                    if combined[field] != value:
                        logger.info(f"RESTORING original Excel value for {field}: {value} (was {combined[field]})")
                        combined[field] = value
            elif field in combined and combined[field] == '' and value != self.DEFAULT_VALUE and value != '':
                # For other fields, only restore if the combined value is empty
                logger.info(f"Restoring original Excel value for {field}: {value}")
                combined[field] = value

        # EXTRA CHECK: Ensure Contract Name is properly preserved
        if 'Contract Name' in excel and excel['Contract Name'] and excel['Contract Name'] != self.DEFAULT_VALUE and excel['Contract Name'] != '':
            combined['Contract Name'] = excel['Contract Name']
            logger.info(f"FINAL CHECK: Ensuring Contract Name is preserved: {excel['Contract Name']}")
        
        return combined

    def _split_full_name(self, full_name: str, combined: Dict) -> None:
        """Split full name into components intelligently."""
        name_parts = full_name.split()
        if not name_parts:
            return
        
        # Special handling for names with format like "FIRST MIDDLE LAST"
        if len(name_parts) >= 3:
            first_name = name_parts[0]
            middle_name = name_parts[1]
            last_name = ' '.join(name_parts[2:])
            
            # Update name fields if they're empty in combined data
            if 'First Name' not in combined or combined['First Name'] == self.DEFAULT_VALUE:
                combined['First Name'] = first_name
                if 'first_name' not in combined or combined['first_name'] == self.DEFAULT_VALUE:
                    combined['first_name'] = first_name
                    
            if 'Middle Name' not in combined or combined['Middle Name'] == self.DEFAULT_VALUE:
                combined['Middle Name'] = middle_name
                if 'middle_name' not in combined or combined['middle_name'] == self.DEFAULT_VALUE:
                    combined['middle_name'] = middle_name
                    
            if 'Last Name' not in combined or combined['Last Name'] == self.DEFAULT_VALUE:
                combined['Last Name'] = last_name
                if 'last_name' not in combined or combined['last_name'] == self.DEFAULT_VALUE:
                    combined['last_name'] = last_name
                    
            logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
        elif len(name_parts) == 2:
            # Two parts: first and last name
            first_name = name_parts[0]
            last_name = name_parts[1]
            
            if 'First Name' not in combined or combined['First Name'] == self.DEFAULT_VALUE:
                combined['First Name'] = first_name
                if 'first_name' not in combined or combined['first_name'] == self.DEFAULT_VALUE:
                    combined['first_name'] = first_name
                    
            if 'Middle Name' not in combined or combined['Middle Name'] == self.DEFAULT_VALUE:
                combined['Middle Name'] = "."  # Default for middle name
                if 'middle_name' not in combined or combined['middle_name'] == self.DEFAULT_VALUE:
                    combined['middle_name'] = "."
                    
            if 'Last Name' not in combined or combined['Last Name'] == self.DEFAULT_VALUE:
                combined['Last Name'] = last_name
                if 'last_name' not in combined or combined['last_name'] == self.DEFAULT_VALUE:
                    combined['last_name'] = last_name
                    
            logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='.', Last='{last_name}'")
        else:
            # Single name: Use as first name
            if 'First Name' not in combined or combined['First Name'] == self.DEFAULT_VALUE:
                combined['First Name'] = name_parts[0]
                if 'first_name' not in combined or combined['first_name'] == self.DEFAULT_VALUE:
                    combined['first_name'] = name_parts[0]

    def _handle_passport_names(self, given_names: str, surname: str, combined: Dict) -> None:
        """Handle name fields from passport data."""
        if 'last_name' in combined and combined['last_name'] == self.DEFAULT_VALUE:
            combined['last_name'] = surname
            
        # Split given names into first and middle
        given_parts = given_names.split()
        if not given_parts:
            return
            
        if 'first_name' in combined and combined['first_name'] == self.DEFAULT_VALUE:
            combined['first_name'] = given_parts[0]
            
        if len(given_parts) > 1 and 'middle_name' in combined and combined['middle_name'] == self.DEFAULT_VALUE:
            combined['middle_name'] = ' '.join(given_parts[1:])

    def _map_to_template(self, data: Dict, template_columns: List[str], field_mappings: Dict) -> Dict:
        """Map combined data to template columns with improved field detection."""
        mapped = {}

        # First, initialize all template columns with empty values
        for col in template_columns:
            mapped[col] = ""
        
         # Detect template type based on columns
        is_almadallah = any(col in template_columns for col in ['POLICYCATEGORY', 'ENTITYID', 'POLICYSEQUENCE', 'FIRSTNAME', 'MIDDLENAME', 'LASTNAME'])
        is_takaful = any(col in template_columns for col in ['StaffNo', 'PrincipalName', 'FirstName', 'SecondName', 'LastName', 'EIDNumber', 'ResidentFileNumber'])
        
        # Direct copy of critical fields - these should override any other mapping
        critical_fields = {
            'unified_no': 'Unified No',
            'visa_file_number': 'Visa File Number',
            'emirates_id': 'Emirates Id',
            'passport_number': 'Passport No',
            'Unified No': 'Unified No',
            'Visa File Number': 'Visa File Number',
            'Emirates Id': 'Emirates Id',
            'Passport No': 'Passport No'
        }
        
        # Direct mapping of critical fields
        for source, target in critical_fields.items():
            if source in data and data[source] != self.DEFAULT_VALUE and data[source] != "":
                if target in template_columns:
                    mapped[target] = data[source]
                    logger.info(f"TEMPLATE MAP: {source} → {target}: {data[source]}")
        
        
        # Add Takaful-specific critical field mappings
        if is_takaful:
            takaful_critical_fields = {
                'emirates_id': 'EIDNumber',
                'Emirates Id': 'EIDNumber',
                'passport_number': 'PassportNum',
                'passport_no': 'PassportNum',
                'Passport No': 'PassportNum',
                'unified_no': 'UIDNo',
                'Unified No': 'UIDNo',
                'visa_file_number': 'ResidentFileNumber',
                'Visa File Number': 'ResidentFileNumber',
                'nationality': 'Country',
                'Nationality': 'Country',
                'date_of_birth': 'DOB',
                'dob': 'DOB',
                'DOB': 'DOB',
                'gender': 'Gender',
                'Gender': 'Gender'
            }
            
            # Apply Takaful critical mappings
            for source, target in takaful_critical_fields.items():
                if source in data and data[source] != self.DEFAULT_VALUE and data[source] != "":
                    if target in template_columns:
                        mapped[target] = data[source]
                        logger.info(f"TAKAFUL CRITICAL MAP: {source} → {target}: {data[source]}")
        
        # Copy all existing Excel data first to preserve it
        for key, value in data.items():
            if key in template_columns:
                mapped[key] = value
                if key == 'Contract Name':
                    logger.info(f"Preserving Contract Name: {value}")
        
        # Detect template type based on columns - check for Al Madallah specific fields
        is_almadallah = any(col in template_columns for col in ['POLICYCATEGORY', 'ENTITYID', 'POLICYSEQUENCE', 'FIRSTNAME', 'MIDDLENAME', 'LASTNAME'])
        is_takaful = any(col in template_columns for col in ['StaffNo', 'PrincipalName', 'FirstName', 'SecondName', 'LastName', 'EIDNumber', 'ResidentFileNumber'])
        
        if is_almadallah:
            logger.info("=" * 80)
            logger.info("AL MADALLAH TEMPLATE DETECTED")
            logger.info("=" * 80)
            logger.info(f"Template has {len(template_columns)} columns")
            logger.info(f"First 10 template columns: {template_columns[:10]}")
            
            # Al Madallah specific mappings
            almadallah_mappings = {
                'FIRSTNAME': ['first_name', 'First Name', 'given_names'],
                'MIDDLENAME': ['middle_name', 'Middle Name'],
                'LASTNAME': ['last_name', 'Last Name', 'surname'],
                'FULLNAME': ['full_name', 'name'],
                'DOB': ['date_of_birth', 'dob', 'birth_date', 'DOB'],
                'GENDER': ['gender', 'sex', 'Gender'],
                'MARITALSTATUS': ['marital_status', 'civil_status', 'Marital Status'],
                'RELATION': ['relation', 'relationship', 'Relation'],
                'EMPLOYEEID': ['staff_id', 'employee_id', 'employee_no', 'Staff ID'],
                'RANK': ['rank', 'position', 'job_title'],
                'Subgroup Name': ['contract_name', 'Contract Name', 'department'],
                'POLICYCATEGORY': ['policy_category', 'plan_type', 'policy_type'],
                'NATIONALITY': ['nationality', 'citizenship', 'nation', 'Nationality'],
                'EFFECTIVEDATE': ['effective_date', 'start_date', 'enrollment_date', 'Effective Date'],
                'EMIRATESID': ['emirates_id', 'eid', 'id_number', 'Emirates Id'],
                'UIDNO': ['unified_no', 'unified_number', 'uid_no', 'Unified No'],
                'VISAFILEREF': ['visa_file_number', 'entry_permit_no', 'visa_number', 'file', 'Visa File Number'],
                'RESIDENTIALEMIRATE': ['residence_emirate', 'home_emirate', 'Work Emirate'],
                'RESIDENTIALLOCATION': ['residence_region', 'home_region', 'Work Region'],
                'MEMBERTYPE': ['member_type', 'enrollee_type', 'Member Type'],
                'OCCUPATION': ['profession', 'job_title', 'occupation', 'Occupation'],
                'WORKEMIRATES': ['work_emirate', 'office_emirate', 'Work Emirate'],
                'WORKLOCATION': ['work_region', 'office_region', 'Work Region'],
                'VISAISSUEDEMIRATE': ['visa_issuance_emirate', 'visa_emirate', 'Visa Issuance Emirate'],
                'PASSPORTNO': ['passport_number', 'passport_no', 'passport', 'Passport No'],
                'SALARYBAND': ['salary_band', 'salary_range', 'income_band', 'Salary Band'],
                'COMMISSION': ['commission', 'comm', 'Commission'],
                'ESTABLISHMENTTYPE': ['establishment_type', 'company_type'],
                'COMPANYPHONENUMBER': ['mobile_no', 'phone', 'Mobile No'],
                'COMPANYEMAILID': ['email', 'email_address', 'Email'],
                'LANDLINENO': ['landline', 'home_phone', 'telephone', 'phone'],
                'MOBILE': ['mobile_no', 'cell_phone', 'Mobile No'],
                'EMAIL': ['email', 'personal_email', 'email_address', 'Email']
            }
            
            # Apply mappings for Al Madallah
            for col in template_columns:
                if col in almadallah_mappings:
                    for field_name in almadallah_mappings[col]:
                        if field_name in data and data[field_name] != self.DEFAULT_VALUE:
                            mapped[col] = data[field_name]
                            field_mappings[col] = field_name
                            logger.info(f"Al Madallah mapping: {field_name} → {col}: {data[field_name]}")
                            break
            
            # Generate FULLNAME from component parts if needed
            if 'FULLNAME' in template_columns and (mapped.get('FULLNAME', '') == '' or mapped.get('FULLNAME') == self.DEFAULT_VALUE):
                first = mapped.get('FIRSTNAME', '')
                middle = mapped.get('MIDDLENAME', '')
                last = mapped.get('LASTNAME', '')
                if first or last:
                    fullname = f"{first} {middle} {last}".replace('  ', ' ').strip()
                    mapped['FULLNAME'] = fullname
                    logger.info(f"Generated FULLNAME: {fullname}")
            
            # Set specific defaults for Al Madallah template
            # Always set COMMISSION to "NO"
            if 'COMMISSION' in template_columns:
                mapped['COMMISSION'] = 'NO'
                logger.info(f"Set default for Al Madallah COMMISSION: NO")
                
            # Always set ESTABLISHMENTTYPE to "Establishment"
            if 'ESTABLISHMENTTYPE' in template_columns:
                mapped['ESTABLISHMENTTYPE'] = 'Establishment'
                logger.info(f"Set default for Al Madallah ESTABLISHMENTTYPE: Establishment")
                
            # Set Subgroup Name from Contract Name if available
            if 'Subgroup Name' in template_columns and (mapped.get('Subgroup Name', '') == '' or mapped.get('Subgroup Name') == self.DEFAULT_VALUE):
                if 'Contract Name' in data and data['Contract Name'] != self.DEFAULT_VALUE:
                    mapped['Subgroup Name'] = data['Contract Name']
                    logger.info(f"Set Al Madallah Subgroup Name from Contract Name: {data['Contract Name']}")
                else:
                    mapped['Subgroup Name'] = 'GENERAL'
                    logger.info(f"Set default Al Madallah Subgroup Name: GENERAL")
            
            # Copy mobile number to multiple phone fields
            mobile_value = None
            if 'Mobile No' in data and data['Mobile No'] != self.DEFAULT_VALUE:
                mobile_value = data['Mobile No']
            elif 'mobile_no' in data and data['mobile_no'] != self.DEFAULT_VALUE:
                mobile_value = data['mobile_no']
                
            if mobile_value:
                for field in ['COMPANYPHONENUMBER', 'LANDLINENO', 'MOBILE']:
                    if field in template_columns:
                        mapped[field] = mobile_value
                        logger.info(f"Copied mobile number to Al Madallah {field}: {mobile_value}")
            
            # Copy email to multiple email fields
            email_value = None
            if 'Email' in data and data['Email'] != self.DEFAULT_VALUE:
                email_value = data['Email']
            elif 'email' in data and data['email'] != self.DEFAULT_VALUE:
                email_value = data['email']
                
            if email_value:
                for field in ['COMPANYEMAILID', 'EMAIL']:
                    if field in template_columns:
                        mapped[field] = email_value
                        logger.info(f"Copied email to Al Madallah {field}: {email_value}")
            
            # Set emirate values based on visa file number
            visa_file = None
            if 'VISAFILEREF' in mapped and mapped['VISAFILEREF'] != self.DEFAULT_VALUE:
                visa_file = mapped['VISAFILEREF']
            elif 'Visa File Number' in data and data['Visa File Number'] != self.DEFAULT_VALUE:
                visa_file = data['Visa File Number']
                
            if visa_file:
                digits = ''.join(filter(str.isdigit, str(visa_file)))
                is_abu_dhabi = digits.startswith('10')
                
                # Set emirate values
                if is_abu_dhabi:
                    # Abu Dhabi values
                    emirate_values = {
                        'RESIDENTIALEMIRATE': 'Abu Dhabi',
                        'WORKEMIRATES': 'Abu Dhabi',
                        'RESIDENTIALLOCATION': 'Abu Dhabi - Abu Dhabi',
                        'WORKLOCATION': 'Abu Dhabi - Abu Dhabi',
                        'VISAISSUEDEMIRATE': 'Abu Dhabi',
                        'MEMBERTYPE': 'Expat whose residence issued other than Dubai'
                    }
                else:
                    # Dubai values (default)
                    emirate_values = {
                        'RESIDENTIALEMIRATE': 'Dubai',
                        'WORKEMIRATES': 'Dubai',
                        'RESIDENTIALLOCATION': 'Dubai - Abu Hail',
                        'WORKLOCATION': 'Dubai - Abu Hail',
                        'VISAISSUEDEMIRATE': 'Dubai',
                        'MEMBERTYPE': 'Expat whose residence issued in Dubai'
                    }
                    
                # Apply emirate values to mapped fields
                for field, value in emirate_values.items():
                    if field in template_columns:
                        mapped[field] = value
                        logger.info(f"Set Al Madallah {field} based on visa file: {value}")
            else:
                # Default to Dubai if no visa file number
                dubai_defaults = {
                    'RESIDENTIALEMIRATE': 'Dubai',
                    'WORKEMIRATES': 'Dubai',
                    'RESIDENTIALLOCATION': 'Dubai - Abu Hail',
                    'WORKLOCATION': 'Dubai - Abu Hail',
                    'VISAISSUEDEMIRATE': 'Dubai',
                    'MEMBERTYPE': 'Expat whose residence issued in Dubai'
                }
                
                for field, value in dubai_defaults.items():
                    if field in template_columns:
                        mapped[field] = value
                        logger.info(f"Set default Al Madallah {field}: {value}")
                        
        elif is_takaful:
            logger.info("=" * 80)
            logger.info("TAKAFUL TEMPLATE DETECTED")
            logger.info("=" * 80)
            logger.info(f"Template has {len(template_columns)} columns")
            
            # Takaful specific mappings
            takaful_mappings = {
                'StaffNo': ['staff_id', 'Staff ID', 'employee_id'],
                'PrincipalName': ['principal_name', 'Principal Name'],
                'PrincipalCardNo': ['principal_card_no', 'Principal Card No.'],
                'CardNo': ['card_no', 'Card No'],
                'PhotoName': ['photo_name', 'Photo Name'],
                'FirstName': ['first_name', 'First Name', 'given_names'],
                'SecondName': ['middle_name', 'Middle Name'],
                'LastName': ['last_name', 'Last Name', 'surname'],
                'DOB': ['date_of_birth', 'dob', 'birth_date', 'DOB'],
                'Gender': ['gender', 'sex', 'Gender'],
                'Relation': ['relation', 'Relation'],
                'Country': ['nationality', 'Nationality', 'citizenship'],
                'MaritalStatus': ['marital_status', 'Marital Status'],
                'Category': ['category', 'Category'],
                'SubGroupDivision': ['subgroup_division', 'SubGroup Division', 'contract_name', 'Contract Name'],
                'EffectiveDate': ['effective_date', 'Effective Date', 'start_date'],
                'Emirate': ['visa_issuance_emirate', 'Visa Issuance Emirate'],
                'EIDNumber': ['emirates_id', 'Emirates Id', 'eid'],
                'Salary': ['salary', 'Salary', 'salary_band', 'Salary Band'],
                'MobileNumber': ['mobile_no', 'Mobile No', 'phone'],
                'EmailId': ['email', 'Email', 'email_address'],
                'City': ['residence_emirate', 'Residence Emirate'],
                'ResidentialLocation': ['residence_region', 'Residence Region'],
                'WorkLocation': ['work_region', 'Work Region'],
                'PassportNum': ['passport_number', 'passport_no', 'Passport No'],
                'UIDNo': ['unified_no', 'Unified No', 'unified_number'],
                'ResidentFileNumber': ['visa_file_number', 'Visa File Number', 'entry_permit_no'],
                'Policy_Id': ['policy_id', 'Policy_Id'],
                'EndorsementId': ['endorsement_id', 'EndorsementId'],
                'DHPOMemberId': ['dhpo_member_id', 'DHPOMemberId'],
                'BirthCertificateID': ['birth_certificate_id', 'BirthCertificateID'],
                'EstablishmentEmail': ['company_mail', 'Company Mail', 'email'],
                'RelationTo': ['relation_to', 'RelationTo'],
                'EntityContactNumber': ['company_phone', 'Company Phone', 'mobile_no'],
                'PolicySequence': ['policy_sequence', 'PolicySequence'],
                'DHPOAgent_Id': ['dhpo_agent_id', 'DHPOAgent_Id'],
                'DHPOErrorMessage': ['dhpo_error_message', 'DHPOErrorMessage'],
                'COCRef': ['coc_ref', 'COCRef'],
                'Penalty': ['penalty', 'Penalty'],
                'COCError': ['coc_error', 'COCError'],
                'COCCValiditydate': ['cocc_validity_date', 'COCCValiditydate'],
                'COCNonInsuranceDays': ['coc_non_insurance_days', 'COCNonInsuranceDays'],
                'COCStatus': ['coc_status', 'COCStatus'],
                'PenaltyCalcType': ['penalty_calc_type', 'PenaltyCalcType']
            }
            
            # Apply mappings for Takaful
            for col in template_columns:
                if col in takaful_mappings:
                    for field_name in takaful_mappings[col]:
                        if field_name in data and data[field_name] != self.DEFAULT_VALUE:
                            mapped[col] = data[field_name]
                            field_mappings[col] = field_name
                            logger.info(f"Takaful mapping: {field_name} → {col}: {data[field_name]}")
                            break
            
            # CRITICAL: Also check for direct template column names in data
            # This ensures data from Excel and extracted documents both work
            if col in data and data[col] != self.DEFAULT_VALUE:
                mapped[col] = data[col]
                field_mappings[col] = col
            
            # Relation - default to 'Principal' if empty
            if 'Relation' in template_columns and (not mapped.get('Relation') or mapped.get('Relation') == ''):
                mapped['Relation'] = 'Principal'
                logger.info("Set default Relation: Principal")
            
            # Salary processing
            if 'Salary' in template_columns and 'Salary' in mapped:
                salary_value = mapped['Salary']
                # If it's a number, convert to text description
                try:
                    salary_num = float(str(salary_value).replace(',', '').replace('AED', '').strip())
                    if salary_num < 4000:
                        mapped['Salary'] = 'less than 4000 AED/month'
                    elif salary_num <= 12000:
                        mapped['Salary'] = 'between 4001 AED and 12000 AED/month'
                    else:
                        mapped['Salary'] = 'more than 12000 AED/month'
                except:
                    # If not a number, keep as is
                    pass
            
            # SalaryBand - depends on Salary
            if 'SalaryBand' in template_columns and 'Salary' in mapped:
                salary_text = mapped['Salary'].lower()
                if 'less than 4000' in salary_text:
                    mapped['SalaryBand'] = 'LSB'
                else:
                    mapped['SalaryBand'] = 'NLSB'
                logger.info(f"Set SalaryBand based on Salary: {mapped['SalaryBand']}")
            
            # IsCommissionBasedSalary - always 'No'
            if 'IsCommissionBasedSalary' in template_columns:
                mapped['IsCommissionBasedSalary'] = 'No'
            
            # EntityType - always 'Establishment'
            if 'EntityType' in template_columns:
                mapped['EntityType'] = 'Establishment'
            
            # EntityId - always '230376/6'
            if 'EntityId' in template_columns:
                mapped['EntityId'] = '230376/6'
            
            # MemberType - based on ResidentFileNumber
            if 'MemberType' in template_columns and 'ResidentFileNumber' in mapped:
                resident_file = mapped['ResidentFileNumber']
                if resident_file and resident_file.startswith('20'):
                    mapped['MemberType'] = 'Expat who is residency is issued in Dubai'
                else:
                    mapped['MemberType'] = 'Expat who is residency is issued in Emirates other than Dubai'
                logger.info(f"Set MemberType based on ResidentFileNumber: {mapped['MemberType']}")
            
            # Set emirate values based on visa file number
            if 'ResidentFileNumber' in mapped and mapped['ResidentFileNumber'] != self.DEFAULT_VALUE:
                visa_file = mapped['ResidentFileNumber']
                digits = ''.join(filter(str.isdigit, str(visa_file)))
                
                if digits.startswith('20'):  # Dubai
                    if 'Emirate' in template_columns:
                        mapped['Emirate'] = 'Dubai'
                    if 'City' in template_columns:
                        mapped['City'] = 'Dubai'
                    if 'ResidentialLocation' in template_columns:
                        mapped['ResidentialLocation'] = 'DUBAI (DISTRICT UNKNOWN)'
                    if 'WorkLocation' in template_columns:
                        mapped['WorkLocation'] = 'DUBAI (DISTRICT UNKNOWN)'
                elif digits.startswith('10'):  # Abu Dhabi
                    if 'Emirate' in template_columns:
                        mapped['Emirate'] = 'Abu Dhabi'
                    if 'City' in template_columns:
                        mapped['City'] = 'Abu Dhabi'
                    if 'ResidentialLocation' in template_columns:
                        mapped['ResidentialLocation'] = 'Al Ain City'
                    if 'WorkLocation' in template_columns:
                        mapped['WorkLocation'] = 'Al Ain City'
        
        else:
            # If not Al Madallah, use general field mappings
            for col in template_columns:
                # Skip if already mapped and not default value
                if col in mapped and mapped[col] != self.DEFAULT_VALUE:
                    continue
                    
                # First normalize the column name for matching
                normalized_col = self._normalize_column_name(col)
                
                # Try direct match first
                if normalized_col in data and (col not in mapped or mapped[col] == self.DEFAULT_VALUE):
                    mapped[col] = data[normalized_col]
                    field_mappings[col] = normalized_col
                    continue
                
                # Try to match with original column name (without normalization)
                if col in data and (col not in mapped or mapped[col] == self.DEFAULT_VALUE):
                    mapped[col] = data[col]
                    field_mappings[col] = col
                    continue
                    
                # Check field variations using the mapping
                mapped_value = self.DEFAULT_VALUE
                found_mapping = False
                
                if not found_mapping:
                    for field, variations in self._field_mapping.items():
                        # Handle dictionary-style field mapping
                        if isinstance(variations, dict) and 'priority' in variations:
                            variations_list = variations['priority']
                        else:
                            variations_list = variations
                            
                        # Check if template column matches any variation
                        if col in variations_list or normalized_col in variations_list:
                            if field in data:
                                mapped_value = data[field]
                                field_mappings[col] = field
                                found_mapping = True
                                break
                        # Check if any field variation matches data keys
                        elif field == normalized_col and any(var in data for var in variations_list):
                            for var in variations_list:
                                if var in data:
                                    mapped_value = data[var]
                                    field_mappings[col] = var
                                    found_mapping = True
                                    break
                            if found_mapping:
                                break
                    
                    # Use default value if no mapping found
                    if not found_mapping and (col not in mapped or mapped[col] == self.DEFAULT_VALUE):
                        field_mappings[col] = None
                        mapped[col] = self._format_output_value(mapped_value, normalized_col)

        # Common field handling for all templates
        if 'Visa File Number' in mapped and mapped['Visa File Number'] != self.DEFAULT_VALUE:
            visa_number = mapped['Visa File Number']
            digits = ''.join(filter(str.isdigit, str(visa_number)))
            
            if digits.startswith('20'):  # Dubai
                # Set Dubai-specific values
                if 'Work Emirate' in template_columns and mapped.get('Work Emirate', '') == '':
                    mapped['Work Emirate'] = 'Dubai'
                if 'Residence Emirate' in template_columns and mapped.get('Residence Emirate', '') == '':
                    mapped['Residence Emirate'] = 'Dubai'
                if 'Work Region' in template_columns and mapped.get('Work Region', '') == '':
                    mapped['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                if 'Residence Region' in template_columns and mapped.get('Residence Region', '') == '':
                    mapped['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                if 'Visa Issuance Emirate' in template_columns and mapped.get('Visa Issuance Emirate', '') == '':
                    mapped['Visa Issuance Emirate'] = 'Dubai'
                if 'Member Type' in template_columns and mapped.get('Member Type', '') == '':
                    mapped['Member Type'] = 'Expat whose residence issued in Dubai'
            elif digits.startswith('10'):  # Abu Dhabi
                # Set Abu Dhabi-specific values
                if 'Work Emirate' in template_columns and mapped.get('Work Emirate', '') == '':
                    mapped['Work Emirate'] = 'Abu Dhabi'
                if 'Residence Emirate' in template_columns and mapped.get('Residence Emirate', '') == '':
                    mapped['Residence Emirate'] = 'Abu Dhabi'
                if 'Work Region' in template_columns and mapped.get('Work Region', '') == '':
                    mapped['Work Region'] = 'Al Ain City'
                if 'Residence Region' in template_columns and mapped.get('Residence Region', '') == '':
                    mapped['Residence Region'] = 'Al Ain City'
                if 'Visa Issuance Emirate' in template_columns and mapped.get('Visa Issuance Emirate', '') == '':
                    mapped['Visa Issuance Emirate'] = 'Abu Dhabi'
                if 'Member Type' in template_columns and mapped.get('Member Type', '') == '':
                    mapped['Member Type'] = 'Expat whose residence issued other than Dubai'

        # Check for and remove duplicate Effective Date at end
        for key in list(mapped.keys()):
            if key != 'Effective Date' and key.lower() == 'effective date':
                # Remove the duplicate
                logger.info(f"Removing duplicate Effective Date field: {key}")
                mapped.pop(key)
                if key in field_mappings:
                    field_mappings.pop(key)
                    
        # Final validation - ensure critical fields are preserved
        for field in ['Unified No', 'Visa File Number', 'Emirates Id', 'UIDNO', 'VISAFILEREF', 'EMIRATESID']:
            if field in mapped and mapped[field] != "" and mapped[field] != self.DEFAULT_VALUE:
                logger.info(f"Final template output for {field}: {mapped[field]}")

        # CRITICAL: Always return the mapped dictionary
        return mapped

    def _clean_final_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize final DataFrame with improved formatting."""
        # Replace NaN/None with default value
        df = df.fillna(self.DEFAULT_VALUE)
        
        # Process each column
        # Process each column
        for col in df.columns:
            # Ensure all values are strings
            df[col] = df[col].astype(str)
            normalized_col = self._normalize_column_name(col)
            
            # Only Middle Name should have '.' default
            if normalized_col == 'middle_name' or col in ['Middle Name', 'SecondName', 'MIDDLENAME']:
                df[col] = df[col].apply(
                    lambda x: self.DEFAULT_VALUE if pd.isna(x) or x == '' or x == self.DEFAULT_VALUE else x
                )
            else:
                # All other columns should be empty if they have the default value
                df[col] = df[col].apply(
                    lambda x: '' if pd.isna(x) or x == '' or x == self.DEFAULT_VALUE else x
                )
                
            # Handle date fields with special formatting
            if normalized_col in self._date_fields and df[col].any():
                df[col] = df[col].apply(
                    lambda x: self._format_date_value(x) if x and x != '' else x
                )
                
            # Handle numeric fields with special formatting
            elif normalized_col in self._numeric_fields and df[col].any():
                df[col] = df[col].apply(
                    lambda x: self._format_numeric_value(x) if x and x != '' else x
                )
                
            # Clean up strings (remove excess whitespace)
            else:
                df[col] = df[col].apply(
                    lambda x: re.sub(r'\s+', ' ', x).strip() if x and x != '' else x
                )
        
        # Make sure Emirates ID is properly formatted
        eid_cols = [col for col in df.columns if self._normalize_column_name(col) == 'emirates_id']
        for col in eid_cols:
            df[col] = df[col].apply(self._process_emirates_id)
        
        return df

    def _normalize_column_name(self, column: str) -> str:
        """Normalize column names for mapping with improved handling."""
        if not isinstance(column, str):
            return ''
            
        # Handle columns with asterisks (required fields)
        if column.endswith('*'):
            column = column[:-1]
            
        # Convert to lowercase, remove special characters, normalize spaces
        clean_name = column.lower().strip()
        clean_name = re.sub(r'[_\s]+', '_', clean_name)  # Convert spaces to underscores
        clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)  # Remove special chars
        
        # Remove duplicate underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        
        # Remove leading/trailing underscores
        return clean_name.strip('_')

    def _normalize_field_name(self, field: str) -> str:
        """Normalize field name for consistent mapping."""
        if not isinstance(field, str):
            return ''
            
        # Convert to lowercase, replace spaces/special chars with underscores
        normalized = field.lower().strip()
        normalized = re.sub(r'[^a-z0-9_]+', '_', normalized)
        normalized = re.sub(r'_+', '_', normalized)
        return normalized.strip('_')

    def _format_excel_date(self, excel_date: float) -> str:
        """Convert Excel date number to DD-MM-YYYY format."""
        try:
            # Excel dates are days since 1900-01-01 (or 1904-01-01 on Mac)
            # Adjust based on the epoch
            date_value = datetime(1899, 12, 30) + timedelta(days=excel_date)
            return date_value.strftime('%d-%m-%Y')
        except Exception:
            return str(excel_date)

    def _format_date_value(self, date_str: str) -> str:
            """Format date string to DD-MM-YYYY format."""
            if date_str == self.DEFAULT_VALUE:
                return date_str
                
            # Skip empty values
            if not date_str or date_str.strip() == '':
                return self.DEFAULT_VALUE
                
            # Try different date formats
            date_formats = [
                ('%Y-%m-%d', r'^\d{4}-\d{1,2}-\d{1,2}'),  # ISO format
                ('%d/%m/%Y', r'^\d{1,2}/\d{1,2}/\d{4}'),  # DD/MM/YYYY
                ('%m/%d/%Y', r'^\d{1,2}/\d{1,2}/\d{4}'),  # MM/DD/YYYY
                ('%d-%m-%Y', r'^\d{1,2}-\d{1,2}-\d{4}'),  # DD-MM-YYYY
                ('%Y/%m/%d', r'^\d{4}/\d{1,2}/\d{1,2}'),  # YYYY/MM/DD
                ('%d.%m.%Y', r'^\d{1,2}\.\d{1,2}\.\d{4}'),  # DD.MM.YYYY
                ('%d %b %Y', r'^\d{1,2} [A-Za-z]{3} \d{4}'),  # DD MMM YYYY
                ('%B %d, %Y', r'^[A-Za-z]+ \d{1,2}, \d{4}'),  # Month DD, YYYY
            ]
                
            for fmt, pattern in date_formats:
                if re.match(pattern, date_str):
                    try:
                        date_obj = datetime.strptime(str(date_str).strip(), fmt)
                        return date_obj.strftime('%d-%m-%Y')  # Changed to DD-MM-YYYY
                    except ValueError:
                        continue
            
            # If no format worked, try pandas to_datetime
            try:
                date_obj = pd.to_datetime(date_str, dayfirst=True)
                return date_obj.strftime('%d-%m-%Y')  # Changed to DD-MM-YYYY
            except:
                # If all parsing attempts fail, return original
                return date_str

    def _format_numeric_value(self, value: str) -> str:
        """Format numeric values consistently."""
        if value == self.DEFAULT_VALUE:
            return value
            
        # Remove non-numeric characters except decimal point
        numeric_str = re.sub(r'[^\d.]', '', str(value))
        
        # Handle phone numbers specially
        if re.match(r'^\+?\d{9,15}$', value.replace(' ', '')):
            # Format as phone number
            digits = re.sub(r'\D', '', value)
            if len(digits) == 10:
                return f"+971{digits[-9:]}"  # UAE format with missing country code
            elif len(digits) > 10:
                return f"+{digits}"
            return digits
            
        # Return cleaned numeric string
        return numeric_str if numeric_str else value    
    
    # Add this method to DataCombiner class
    def _split_combined_name(self, combined_name: str) -> Tuple[str, str, str]:
        """Split a combined name into first, middle, and last name components.
        
        Args:
            combined_name: Combined name string
            
        Returns:
            Tuple of (first_name, middle_name, last_name)
        """
        if not combined_name or combined_name == self.DEFAULT_VALUE:
            return self.DEFAULT_VALUE, self.DEFAULT_VALUE, self.DEFAULT_VALUE
            
        name_parts = combined_name.split()
        
        if len(name_parts) == 1:
            # Just one word, assume it's first name
            return name_parts[0], self.DEFAULT_VALUE, self.DEFAULT_VALUE
        elif len(name_parts) == 2:
            # Two words, assume first + last
            return name_parts[0], self.DEFAULT_VALUE, name_parts[1]
        elif len(name_parts) == 3:
            # Three words, assume first + middle + last
            return name_parts[0], name_parts[1], name_parts[2]
        else:
            # More than three parts - assume:
            # First word is first name
            # Last word is last name
            # Everything in between is middle name
            first = name_parts[0]
            middle = ' '.join(name_parts[1:-1])
            last = name_parts[-1]
            return first, middle, last
        
    def _format_fields_for_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle default values correctly - '.' only for middle name, empty for others."""
        for col in df.columns:
            col_lower = col.lower()
            # Only Middle Name gets '.' default
            if col_lower == 'middle_name' or col == 'Middle Name':
                df[col] = df[col].apply(
                    lambda x: '.' if pd.isna(x) or x == '' or x == self.DEFAULT_VALUE else x
                )
            else:
                # All other fields get empty string
                df[col] = df[col].apply(
                    lambda x: '' if pd.isna(x) or x == '' or x == self.DEFAULT_VALUE else x
                )    
            # Special handling for Emirates ID
            if 'emirates' in col_lower and 'id' in col_lower:
                df[col] = df[col].apply(self._process_emirates_id)
        return df
                

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure column names match expected format."""
        normalized = {}
        for col in df.columns:
            # Common variations
            if 'passport' in col.lower() and 'no' in col.lower():
                normalized[col] = 'Passport No'
            elif 'emirates' in col.lower() and 'id' in col.lower():
                normalized[col] = 'Emirates Id'
            elif 'unified' in col.lower() and 'no' in col.lower():
                normalized[col] = 'Unified No'
            elif 'visa' in col.lower() and 'file' in col.lower():
                normalized[col] = 'Visa File Number'
            elif 'nationality' in col.lower():
                normalized[col] = 'Nationality'
            elif 'dob' in col.lower() or ('date' in col.lower() and 'birth' in col.lower()):
                normalized[col] = 'DOB'
        
        # Rename columns that need normalization
        return df.rename(columns=normalized)            
    
    def _apply_standard_fields(self, row_data: Dict) -> None:
        """Apply standard field formatting and defaults to all rows."""
        # CRITICAL: Check if row_data is None and handle gracefully
        if row_data is None:
            logger.error("row_data is None in _apply_standard_fields - this should not happen!")
            return
        
        # Store DEFAULT_VALUE locally
        DEFAULT_VALUE = self.DEFAULT_VALUE
        
        # CRITICAL: ALWAYS set Effective Date to today's date
        today_date = datetime.now().strftime('%d/%m/%Y')
        
        # Set Effective Date for both possible field names
        if 'Effective Date' in row_data or True:  # Always try to set
            row_data['Effective Date'] = today_date
        if 'effective_date' in row_data or True:  # Always try to set
            row_data['effective_date'] = today_date
        if 'EFFECTIVEDATE' in row_data or True:  # For Al Madallah template
            row_data['EFFECTIVEDATE'] = today_date
            
        logger.info(f"Setting Effective Date to today: {today_date}")
        
        # Country values
        row_data['Work Country'] = 'United Arab Emirates'
        row_data['Residence Country'] = 'United Arab Emirates'
        row_data['Commission'] = 'NO'
        
        # Handle visa issuance emirate and related fields
        visa_file_number = None
        if 'Visa File Number' in row_data and row_data['Visa File Number'] != DEFAULT_VALUE:
            visa_file_number = row_data['Visa File Number']
        elif 'VISAFILEREF' in row_data and row_data['VISAFILEREF'] != DEFAULT_VALUE:
            visa_file_number = row_data['VISAFILEREF']
        
        if visa_file_number:
            # Extract just digits
            digits = ''.join(filter(str.isdigit, str(visa_file_number)))
            
            if digits.startswith('20'):
                # Dubai values
                row_data['Visa Issuance Emirate'] = 'Dubai'
                row_data['Work Emirate'] = 'Dubai'
                row_data['Residence Emirate'] = 'Dubai'
                row_data['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                row_data['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                row_data['Member Type'] = 'Expat whose residence issued in Dubai'
                
                # Also set Al Madallah column names
                row_data['VISAISSUEDEMIRATE'] = 'Dubai'
                row_data['WORKEMIRATES'] = 'Dubai'
                row_data['RESIDENTIALEMIRATE'] = 'Dubai'
                row_data['WORKLOCATION'] = 'Dubai - Abu Hail'
                row_data['RESIDENTIALLOCATION'] = 'Dubai - Abu Hail'
                row_data['MEMBERTYPE'] = 'Expat whose residence issued in Dubai'
                
            elif digits.startswith('10'):
                # Abu Dhabi values
                row_data['Visa Issuance Emirate'] = 'Abu Dhabi'
                row_data['Work Emirate'] = 'Abu Dhabi'
                row_data['Residence Emirate'] = 'Abu Dhabi'
                row_data['Work Region'] = 'Al Ain City'
                row_data['Residence Region'] = 'Al Ain City'
                row_data['Member Type'] = 'Expat whose residence issued other than Dubai'
                
                # Also set Al Madallah column names
                row_data['VISAISSUEDEMIRATE'] = 'Abu Dhabi'
                row_data['WORKEMIRATES'] = 'Abu Dhabi'
                row_data['RESIDENTIALEMIRATE'] = 'Abu Dhabi'
                row_data['WORKLOCATION'] = 'Abu Dhabi - Abu Dhabi'
                row_data['RESIDENTIALLOCATION'] = 'Abu Dhabi - Abu Dhabi'
                row_data['MEMBERTYPE'] = 'Expat whose residence issued other than Dubai'
            else:
                # Default values
                row_data['Member Type'] = 'Expat whose residence issued other than Dubai'
                row_data['MEMBERTYPE'] = 'Expat whose residence issued other than Dubai'
        
        # Check if this is Takaful template
        is_takaful = any(field in row_data for field in ['StaffNo', 'FirstName', 'SecondName', 'LastName', 'EIDNumber'])
        
        if is_takaful:
            # Takaful-specific defaults
            if 'Relation' in row_data and (not row_data['Relation'] or row_data['Relation'] == self.DEFAULT_VALUE):
                row_data['Relation'] = 'Principal'
            
            if 'IsCommissionBasedSalary' in row_data:
                row_data['IsCommissionBasedSalary'] = 'No'
                
            if 'EntityType' in row_data:
                row_data['EntityType'] = 'Establishment'
                
            if 'EntityId' in row_data:
                row_data['EntityId'] = '230376/6'
        
        # Format Mobile No
        mobile_fields = ['Mobile No', 'MOBILE', 'COMPANYPHONENUMBER', 'LANDLINENO']
        for field in mobile_fields:
            if field in row_data and row_data[field] != DEFAULT_VALUE:
                digits = ''.join(filter(str.isdigit, str(row_data[field])))
                if len(digits) >= 9:
                    formatted_mobile = digits[-9:]
                    row_data[field] = formatted_mobile
                    # Copy to other mobile fields if they're empty
                    for other_field in mobile_fields:
                        if other_field not in row_data or row_data[other_field] == DEFAULT_VALUE:
                            row_data[other_field] = formatted_mobile
                    break
        
        # Copy Staff ID to Family No if needed
        if 'Staff ID' in row_data and row_data['Staff ID'] != DEFAULT_VALUE:
            if 'Family No.' not in row_data or row_data['Family No.'] == DEFAULT_VALUE:
                row_data['Family No.'] = row_data['Staff ID']
        
        # Copy Staff ID to Employee ID for Al Madallah
        if 'Staff ID' in row_data and row_data['Staff ID'] != DEFAULT_VALUE:
            if 'EMPLOYEEID' not in row_data or row_data['EMPLOYEEID'] == DEFAULT_VALUE:
                row_data['EMPLOYEEID'] = row_data['Staff ID']
        
        # Ensure Middle Name has '.' while other empty fields are truly empty
        for col in row_data:
            col_lower = col.lower()
            # Only Middle Name gets default '.'
            if ('middle' in col_lower and 'name' in col_lower) or col in ['Middle Name', 'SecondName', 'MIDDLENAME']: 
                if pd.isna(row_data[col]) or row_data[col] == '' or row_data[col] == DEFAULT_VALUE:
                    row_data[col] = '.'
            # All other fields should be empty rather than '.'
            elif row_data[col] == DEFAULT_VALUE:
                row_data[col] = ''
                
    def _process_single_row(self, extracted_data: Dict, template_columns: List[str],
                  field_mappings: Dict, document_paths: Dict[str, Any] = None) -> pd.DataFrame:
        """Process single row of data."""
        # Handle new document_paths structure with lists of paths if needed
        if document_paths:
            has_lists = any(isinstance(paths, list) for paths in document_paths.values() if paths is not None)
            
            if has_lists:
                logger.info("Detected new document_paths structure with multiple documents per type")
                
                # Process each document and combine the extracted data - USE GPT ONLY
                for doc_type, paths in document_paths.items():
                    if isinstance(paths, list):
                        for path in paths:
                            try:
                                # Extract data from this document with GPT
                                if self.deepseek_processor:  # Using deepseek as GPT
                                    doc_data = self.deepseek_processor.process_document(path, doc_type)
                                    if doc_data:
                                        # Add to extracted_data with priority for non-default values
                                        for key, value in doc_data.items():
                                            if key not in extracted_data or (value != self.DEFAULT_VALUE and extracted_data[key] == self.DEFAULT_VALUE):
                                                extracted_data[key] = value
                            except Exception as e:
                                logger.error(f"Error processing document {path}: {str(e)}")
        
        cleaned_data = self._clean_extracted_data(extracted_data)
        combined_data = self._combine_row_data(cleaned_data, {}, document_paths)
        mapped_data = self._map_to_template(combined_data, template_columns, field_mappings)
        
        # Process Emirates ID directly
        for col in mapped_data:
            if 'emirates_id' in col.lower() or 'emirates id' in col.lower():
                mapped_data[col] = self._format_emirates_id(mapped_data[col])
                
        # Apply standard fields to ensure we have location data based on visa file number
        self._apply_standard_fields(mapped_data)
        
        return pd.DataFrame([mapped_data])
    
    def _ensure_effective_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final check to ensure Effective Date is set correctly on all rows.
        This should be called right before saving the DataFrame to Excel.
        """
        # Get today's date in DD/MM/YYYY format
        today_date = datetime.now().strftime('%d/%m/%Y')
        
        # Check if Effective Date column exists
        if 'Effective Date' in df.columns:
            # Set today's date for all rows
            df['Effective Date'] = today_date
            logger.info(f"FINAL CHECK: Set Effective Date to {today_date} for all {len(df)} rows")
        
        # Also check for variant with space at the end (Excel can be weird with column names)
        if 'Effective Date ' in df.columns:
            df['Effective Date '] = today_date
            logger.info(f"FINAL CHECK: Set 'Effective Date ' to {today_date} for all {len(df)} rows")
        
        return df
    
    def _validate_id_fields(self, data: Dict) -> Dict:
        """Ensure ID fields have the correct format."""
        result = data.copy()
        
        # Validate Unified No format - should be digits only
        if 'unified_no' in result and result['unified_no'] != self.DEFAULT_VALUE:
            unified = result['unified_no']
            
            # If it contains slashes, it's definitely NOT a valid unified number
            if '/' in unified:
                logger.warning(f"VALIDATION ERROR: unified_no contains slashes: {unified}")
                
                # Move to visa_file_number if appropriate
                if 'visa_file_number' not in result or result['visa_file_number'] == self.DEFAULT_VALUE:
                    result['visa_file_number'] = unified
                    logger.info(f"Moved slash-containing value to visa_file_number: {unified}")
                
                # Try to use the correct unified number if available in extracted data
                if hasattr(self, '_extracted_cache') and 'unified_no' in self._extracted_cache:
                    correct_unified = self._extracted_cache['unified_no']
                    if correct_unified != self.DEFAULT_VALUE and '/' not in correct_unified:
                        result['unified_no'] = correct_unified
                        logger.info(f"Using correct unified_no from extracted data: {correct_unified}")
                    else:
                        # Just use the digits as fallback
                        digits = ''.join(filter(str.isdigit, unified))
                        if len(digits) >= 8:
                            result['unified_no'] = digits
                            logger.info(f"Extracted digits for unified_no: {digits}")
                        else:
                            result['unified_no'] = self.DEFAULT_VALUE
                else:
                    # Extract digits from the incorrect value as fallback
                    digits = ''.join(filter(str.isdigit, unified))
                    if len(digits) >= 8:
                        result['unified_no'] = digits
                        logger.info(f"Extracted digits for unified_no: {digits}")
                    else:
                        result['unified_no'] = self.DEFAULT_VALUE
        
        # Also update the capitalized version fields for templates
        if 'unified_no' in result and result['unified_no'] != self.DEFAULT_VALUE:
            result['Unified No'] = result['unified_no']
        
        if 'visa_file_number' in result and result['visa_file_number'] != self.DEFAULT_VALUE:
            result['Visa File Number'] = result['visa_file_number']
            
        # CRITICAL FIX: Be more careful about clearing Unified No values
        if ('unified_no' in result and 'visa_file_number' in result and
            result['unified_no'] != self.DEFAULT_VALUE and result['visa_file_number'] != self.DEFAULT_VALUE):
            
            unified = result['unified_no']
            visa_file = result['visa_file_number']
            visa_file_no_slashes = visa_file.replace('/', '')
            
            # Only clear if EXACTLY the same (with high confidence it's wrong)
            if unified == visa_file_no_slashes:
                logger.warning(f"SUSPICIOUS: unified_no exactly matches visa_file_number with slashes removed!")
                logger.warning(f"  unified_no: {unified}")
                logger.warning(f"  visa_file_number: {visa_file}")
                
                # Instead of immediately clearing, look for alternatives
                # For now, keep the value since it might be correct in some cases
                # If the visa file number starts with 203, it's most likely a Dubai visa
                # and unified_no might be different
                if visa_file.startswith('203/') or visa_file.startswith('201/'):
                    logger.info("Dubai visa detected - clearing potentially incorrect unified_no")
                    result['unified_no'] = self.DEFAULT_VALUE
                    result['Unified No'] = self.DEFAULT_VALUE
                else:
                    logger.info("Keeping unified_no despite similarity to visa_file_number")
            
        return result
    
    def _verify_critical_fields(self, data: Dict, extracted: Dict) -> Dict:
        """Ensure critical fields are present in the final output."""
        result = data.copy()
        
        # Critical fields that must be in the final output if extracted
        critical_mapping = [
            ('unified_no', ['unified_no', 'Unified No']),
            ('visa_file_number', ['visa_file_number', 'Visa File Number']),
            ('emirates_id', ['emirates_id', 'Emirates Id']),
            ('passport_number', ['passport_no', 'Passport No'])
        ]
        
        # Check if any critical field is missing in output but was in extracted data
        for extract_field, output_fields in critical_mapping:
            # If we had this field in extracted data
            if extract_field in extracted and extracted[extract_field] != self.DEFAULT_VALUE:
                # Check if any output field is missing this data
                is_missing = True
                for output_field in output_fields:
                    if output_field in result and result[output_field] != "" and result[output_field] != self.DEFAULT_VALUE:
                        is_missing = False
                        break
                
                # If all output fields are missing this data, restore it
                if is_missing:
                    logger.warning(f"CRITICAL FIELD MISSING: {extract_field} was extracted but missing from output")
                    logger.warning(f"Extracted value: {extracted[extract_field]}")
                    
                    # Restore the value to all target fields
                    for output_field in output_fields:
                        result[output_field] = extracted[extract_field]
                        logger.info(f"RESTORED {output_field} = {extracted[extract_field]}")
        
        return result