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
        """
        Process Emirates ID with special handling.
        If it doesn't start with 784, replace with default value.
        """
        if value is None or pd.isna(value) or value == '' or value == self.DEFAULT_VALUE:
            return ''
            
        # First clean the value
        cleaned = re.sub(r'[^0-9-]', '', str(value))
        
        # Format with hyphens if needed
        if '-' not in cleaned and len(cleaned) == 15:
            cleaned = f"{cleaned[:3]}-{cleaned[3:7]}-{cleaned[7:14]}-{cleaned[14]}"
        
        # Check if it starts with 784 (after format standardization)
        if not cleaned.startswith('784'):
            logger.info(f"Emirates ID '{cleaned}' doesn't start with 784, using default ID")
            return '111-1111-1111111-1'
        
        return cleaned
    
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
                                    extracted_data: Dict, excel_data: Any = None, document_paths: Dict[str, str] = None) -> Dict:
        logger.info(f"Starting data combination with template: {template_path}")
        logger.info(f"Extracted data has {len(extracted_data)} fields: {list(extracted_data.keys())}")
        if excel_data is not None:
            if isinstance(excel_data, pd.DataFrame):
                logger.info(f"Excel data has {len(excel_data)} rows")
            elif isinstance(excel_data, list):
                logger.info(f"Excel data has {len(excel_data)} rows")
            elif isinstance(excel_data, dict):
                logger.info(f"Excel data has 1 row with {len(excel_data)} fields")
                
        """Combine data with better handling of multiple rows."""
        start_time = time.time()
        try:
            # Validate template
            if not os.path.exists(template_path):
                raise FileNotFoundError(f"Template file not found: {template_path}")
                
            # Get template structure
            template_info = self._get_template_structure(template_path)
            template_columns = template_info['columns']
            
            # Initialize field mappings
            field_mappings = {}
            
            # Convert excel_data to DataFrame if needed
            if excel_data is not None:
                if isinstance(excel_data, dict):
                    excel_data = pd.DataFrame([excel_data])
                elif isinstance(excel_data, list):
                    excel_data = pd.DataFrame(excel_data)
                elif not isinstance(excel_data, pd.DataFrame):
                    raise TypeError(f"Excel data must be DataFrame, dict, or list")
            
            # Process data with additional error checking
            try:
                if excel_data is not None:
                    if isinstance(excel_data, pd.DataFrame) and not excel_data.empty:
                        logger.info(f"Processing {len(excel_data)} rows with document data")
                        result_df = self._process_multiple_rows(extracted_data, excel_data, 
                                                            template_columns, field_mappings, document_paths)
                    elif isinstance(excel_data, list) and excel_data:
                        logger.info(f"Processing {len(excel_data)} rows (list) with document data")
                        excel_df = pd.DataFrame(excel_data)
                        result_df = self._process_multiple_rows(extracted_data, excel_df, 
                                                            template_columns, field_mappings, document_paths)
                    else:
                        logger.warning("Excel data is empty or None, using document data only")
                        result_df = self._process_single_row(extracted_data, template_columns, 
                                                        field_mappings, document_paths)
                else:
                    logger.info("No Excel data, using document data only")
                    result_df = self._process_single_row(extracted_data, template_columns, 
                                                    field_mappings, document_paths)
            except Exception as e:
                logger.error(f"Error processing data: {str(e)}", exc_info=True)
                # Fall back to creating a simple dataframe with just the extracted data
                logger.info("Falling back to basic data processing")
                simple_data = {field: value for field, value in extracted_data.items() 
                            if field in template_columns or any(field == self._normalize_field_name(col) for col in template_columns)}
                result_df = pd.DataFrame([simple_data])
                
            # Save results
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                
            # Final cleanup of date formats
            date_fields = ['effective_date', 'dob', 'passport_expiry_date', 'visa_expiry_date']
            for field in date_fields:
                if field in result_df.columns:
                    result_df[field] = result_df[field].apply(
                        lambda x: self._format_date_value(x) if pd.notna(x) else self.DEFAULT_VALUE
                    )
                    
            # Apply final cleaning to remove default values except for middle name
            result_df = self._clean_final_dataframe(result_df)
            
            # Save to Excel
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
            logger.error(f"Error combining data: {str(e)}")
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
                
            template_info = {
                'columns': template_df.columns.tolist(),
                'column_info': column_info,
                'column_count': len(template_df.columns),
                'last_modified': os.path.getmtime(template_path)
            }
            
            # Cache the result
            self._template_cache[template_path] = template_info
            return template_info

    def _process_multiple_rows(self, extracted_data: Dict, excel_data: pd.DataFrame, 
                        template_columns: List[str], field_mappings: Dict,
                        document_paths: Dict[str, str] = None) -> pd.DataFrame:
        """Process multiple rows of data with batch processing for performance."""
        result_rows = []
        
        # Clean document data once
        cleaned_extracted = self._clean_extracted_data(extracted_data)
        
        # Process each row
        for idx, excel_row in excel_data.iterrows():
            # Clean Excel row
            excel_dict = excel_row.to_dict()
            cleaned_excel = self._clean_excel_data(excel_dict)
            
            # Combine row data with document paths
            combined_row = self._combine_row_data(cleaned_extracted, cleaned_excel, document_paths)
            
            # Map to template
            mapped_row = self._map_to_template(combined_row, template_columns, field_mappings)
            result_rows.append(mapped_row)
        
        return pd.DataFrame(result_rows)

    def _process_single_row(self, extracted_data: Dict, template_columns: List[str],
                      field_mappings: Dict, document_paths: Dict[str, str] = None) -> pd.DataFrame:
        """Process single row of data."""
        cleaned_data = self._clean_extracted_data(extracted_data)
        combined_data = self._combine_row_data(cleaned_data, {}, document_paths)
        mapped_data = self._map_to_template(combined_data, template_columns, field_mappings)
        return pd.DataFrame([mapped_data])

    def _clean_extracted_data(self, data: Dict) -> Dict:
        """Clean extracted document data with standardization."""
        cleaned = {}
        for key, value in data.items():
            normalized_key = self._normalize_field_name(key)
            
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
        """Format Emirates ID to include hyphens in correct positions."""
        if not eid or eid == self.DEFAULT_VALUE:
            return self.DEFAULT_VALUE
            
        # Remove any existing hyphens or spaces
        digits = re.sub(r'[^0-9]', '', str(eid))
        
        # Check if we have the correct number of digits
        if len(digits) == 15:
            # Format as XXX-XXXX-XXXXXXX-X
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:14]}-{digits[14]}"
        
        return eid


    def _combine_row_data(self, extracted: Dict, excel: Dict, document_paths: Dict[str, str] = None) -> Dict:
        """Combine data with improved priority rules and field mapping."""
        # Start with a deep copy of Excel data to avoid modification
        combined = copy.deepcopy(excel)
        
        # First, format Emirates ID in Excel data if present
        if 'emirates_id' in combined and combined['emirates_id'] != self.DEFAULT_VALUE:
            combined['emirates_id'] = self._format_emirates_id(combined['emirates_id'])
        
        # Identify special fields that require custom handling
        name_fields = ['first_name', 'middle_name', 'last_name', 'full_name']
        id_fields = ['passport_number', 'emirates_id', 'entry_permit_no', 'unified_no']
        date_fields = ['dob', 'date_of_birth', 'effective_date', 'passport_expiry_date', 'visa_expiry_date']
        
        # Field mapping for extracted data
        field_map = {
            'entry_permit_no': ['visa_file_number', 'unified_no', 'permit_number'],
            'emirates_id': ['eid', 'id_number'],
            'passport_number': ['passport_no'],
            'given_names': ['first_name', 'middle_name'],
            'surname': ['last_name'],
            'full_name': ['name', 'customer_name'],
            'name_en': ['name', 'first_name'],
            'nationality': ['nationality', 'citizenship'],
            'date_of_birth': ['dob', 'birth_date'],
            'gender': ['sex'],
            'profession': ['occupation', 'job_title'],
            'expiry_date': ['passport_expiry_date', 'visa_expiry_date'],
            'issue_date': ['date_of_issue']
        }
        
        # Track overridden fields
        overridden = []
        
        if 'dob' in excel and excel['dob'] not in [self.DEFAULT_VALUE, '', None, 'nan']:
            combined['dob'] = self._format_date_value(excel['dob'])
        elif 'DOB' in excel and excel['DOB'] not in [self.DEFAULT_VALUE, '', None, 'nan']:
            combined['dob'] = self._format_date_value(excel['DOB'])
        elif 'date_of_birth' in extracted and extracted['date_of_birth'] != self.DEFAULT_VALUE:
            combined['dob'] = self._format_date_value(extracted['date_of_birth'])
        
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
        
        # Process all other extracted fields
        for ext_key, value in extracted.items():
            # Skip default values and already processed name fields
            if value == self.DEFAULT_VALUE or ext_key in ['full_name', 'given_names', 'surname']:
                continue
            
            # Format date values from extracted data
            if ext_key in date_fields:
                value = self._format_date_value(value)
                
            # Use field mapping to find matching fields in combined data
            target_keys = field_map.get(ext_key, [ext_key])
            for target_key in target_keys:
                # Don't override non-empty Excel data except for ID fields
                if target_key in combined:
                    if combined[target_key] == self.DEFAULT_VALUE or target_key in id_fields:
                        combined[target_key] = value
                        overridden.append(target_key)
                        break
        
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
                    
        if 'dob' in combined and combined['dob'] != self.DEFAULT_VALUE:
            combined['date_of_birth'] = combined['dob']
        elif 'date_of_birth' in combined and combined['date_of_birth'] != self.DEFAULT_VALUE:
            combined['dob'] = combined['date_of_birth']            
                    
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
                
                if 'visa_file_number' in combined and combined['visa_file_number'] != self.DEFAULT_VALUE:
                    visa_number = combined['visa_file_number']
                    
                    # Remove any non-digit characters to extract just the numbers
                    digits = ''.join(filter(str.isdigit, visa_number))
                    
                    # Check if it starts with specific digits
                    if digits.startswith('201'):
                        logger.info(f"Visa file number {visa_number} starts with 201, setting emirate to Dubai")
                        combined['visa_issuance_emirate'] = 'Dubai'
                    elif digits.startswith('101'):
                        logger.info(f"Visa file number {visa_number} starts with 101, setting emirate to Abu Dhabi")
                        combined['visa_issuance_emirate'] = 'Abu Dhabi'
                
                if 'effective_date' not in combined or combined['effective_date'] == self.DEFAULT_VALUE:
                    combined['effective_date'] = datetime.now().strftime('%d/%m/%Y')
                    logger.info(f"Setting default effective_date to today: {combined['effective_date']}")
                    
        # Family No = Staff ID
        if 'staff_id' in combined and combined['staff_id'] != self.DEFAULT_VALUE:
            combined['family_no'] = combined['staff_id']
            logger.info(f"Set family_no to match staff_id: {combined['staff_id']}")

        # Work and residence country
        combined['work_country'] = 'United Arab Emirates'
        combined['residence_country'] = 'United Arab Emirates'

        # Commission
        combined['commission'] = 'NO'

        # Handle Mobile No format
        if 'mobile_no' in combined and combined['mobile_no'] != self.DEFAULT_VALUE:
            # Extract just the digits
            digits = ''.join(filter(str.isdigit, combined['mobile_no']))
            # Take last 9 digits
            if len(digits) >= 9:
                combined['mobile_no'] = digits[-9:]
            logger.info(f"Formatted mobile_no: {combined['mobile_no']}")

        # Handle emirate-based fields
        if 'visa_issuance_emirate' in combined:
            issuance_emirate = combined['visa_issuance_emirate']
            
            if issuance_emirate == 'Dubai':
                combined['work_emirate'] = 'Dubai'
                combined['residence_emirate'] = 'Dubai'
                combined['work_region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['residence_region'] = 'DUBAI (DISTRICT UNKNOWN)'
                combined['member_type'] = 'Expat whose residence issued in Dubai'
            elif issuance_emirate:  # Any other emirate
                combined['work_emirate'] = issuance_emirate
                combined['residence_emirate'] = issuance_emirate
                combined['work_region'] = 'Al Ain City'
                combined['residence_region'] = 'Al Ain City'
                combined['member_type'] = 'Expat whose residence issued other than Dubai'

        # Company phone and email
        if 'mobile_no' in combined and combined['mobile_no'] != self.DEFAULT_VALUE:
            if 'company_phone' not in combined or combined['company_phone'] == self.DEFAULT_VALUE:
                combined['company_phone'] = combined['mobile_no']

        if 'email' in combined and combined['email'] != self.DEFAULT_VALUE:
            if 'company_mail' not in combined or combined['company_mail'] == self.DEFAULT_VALUE:
                combined['company_mail'] = combined['email']
                            
                return combined

    def _split_full_name(self, full_name: str, combined: Dict) -> None:
        """Split full name into components intelligently."""
        name_parts = full_name.split()
        if not name_parts:
            return
            
        # Update name fields if they're empty in combined data
        if len(name_parts) == 1:
            # Single word name
            if 'first_name' in combined and combined['first_name'] == self.DEFAULT_VALUE:
                combined['first_name'] = name_parts[0]
                
        elif len(name_parts) == 2:
            # First and last name
            if 'first_name' in combined and combined['first_name'] == self.DEFAULT_VALUE:
                combined['first_name'] = name_parts[0]
            if 'last_name' in combined and combined['last_name'] == self.DEFAULT_VALUE:
                combined['last_name'] = name_parts[1]
                
        else:
            # First, middle, and last name
            if 'first_name' in combined and combined['first_name'] == self.DEFAULT_VALUE:
                combined['first_name'] = name_parts[0]
            if 'middle_name' in combined and combined['middle_name'] == self.DEFAULT_VALUE:
                combined['middle_name'] = ' '.join(name_parts[1:-1])
            if 'last_name' in combined and combined['last_name'] == self.DEFAULT_VALUE:
                combined['last_name'] = name_parts[-1]

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
        
        for col in template_columns:
            # First normalize the column name for matching
            normalized_col = self._normalize_column_name(col)
            
            # Try direct match first
            if normalized_col in data:
                mapped[col] = data[normalized_col]
                field_mappings[col] = normalized_col
                continue
            
            # Try to match with original column name (without normalization)
            if col in data:
                mapped[col] = data[col]
                field_mappings[col] = col
                continue
                
            # Check field variations using the mapping
            mapped_value = self.DEFAULT_VALUE
            found_mapping = False
            
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
            if not found_mapping:
                field_mappings[col] = None
                
            mapped[col] = self._format_output_value(mapped_value, normalized_col)
                    
        return mapped

    def _clean_final_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize final DataFrame with improved formatting."""
        # Replace NaN/None with default value
        df = df.fillna(self.DEFAULT_VALUE)
        
        # Process each column
        for col in df.columns:
            normalized_col = self._normalize_column_name(col)
            
            # Convert to string first
            df[col] = df[col].astype(str)
            
            # Only Middle Name should have '.' default
            if normalized_col == 'middle_name':
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
        if 'emirates_id' in [self._normalize_column_name(col) for col in df.columns]:
            eid_col = next(col for col in df.columns if self._normalize_column_name(col) == 'emirates_id')
            df[eid_col] = df[eid_col].apply(self._process_emirates_id)
        
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
                