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
                                    extracted_data: Dict, excel_data: Any = None, document_paths: Dict[str, str] = None) -> Dict:
        """Combine data with better handling of multiple rows."""
        logger.info(f"Starting data combination with template: {template_path}")
        logger.info(f"Extracted data has {len(extracted_data)} fields: {list(extracted_data.keys())}")
        
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
            
            # Initialize field mappings
            field_mappings = {}
            
            # Make sure template columns are properly understood
            logger.info(f"Template has {len(template_columns)} columns: {template_columns[:10]}...")
            
            # Process excel_data with robust error handling
            try:
                # Validate and convert excel_data
                if excel_data is not None:
                    if isinstance(excel_data, dict):
                        # Convert dictionary to DataFrame with a single row
                        excel_data = pd.DataFrame([excel_data])
                        logger.info("Converted dict to DataFrame with 1 row")
                    elif isinstance(excel_data, list):
                        if not excel_data:
                            # Empty list
                            logger.warning("Excel data is an empty list, using empty DataFrame")
                            excel_data = pd.DataFrame()
                        else:
                            # Convert list to DataFrame
                            excel_data = pd.DataFrame(excel_data)
                            logger.info(f"Converted list with {len(excel_data)} items to DataFrame")
                    elif not isinstance(excel_data, pd.DataFrame):
                        # Invalid type
                        logger.warning(f"Excel data has invalid type {type(excel_data)}, using empty DataFrame")
                        excel_data = pd.DataFrame()
                else:
                    # None value
                    logger.info("Excel data is None, using empty DataFrame")
                    excel_data = pd.DataFrame()
            except Exception as e:
                logger.error(f"Error processing excel_data: {str(e)}", exc_info=True)
                excel_data = pd.DataFrame()  # Use empty DataFrame on error
            
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
            except Exception as e:
                logger.error(f"Error in data processing: {str(e)}", exc_info=True)
                # Fall back to creating a simple dataframe with just the extracted data
                logger.info("Falling back to basic data processing")
                
                # Create a DataFrame with extracted data directly mapped to template columns
                result_data = {}
                
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
                
                # Also try the Excel data
                if isinstance(excel_data, pd.DataFrame) and not excel_data.empty:
                    first_row = excel_data.iloc[0].to_dict()
                    for col in template_columns:
                        if col in first_row and first_row[col] not in [None, '', 'nan', self.DEFAULT_VALUE]:
                            result_data[col] = first_row[col]
                
                # Create a DataFrame
                result_df = pd.DataFrame([result_data])
                
            # Save results
            try:
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                    
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
                
                # Log key columns before saving
                for col in ['First Name', 'Last Name', 'Nationality', 'Passport No', 'Emirates Id', 'Unified No']:
                    if col in result_df.columns:
                        values = result_df[col].tolist()
                        logger.info(f"Column {col} values: {values}")
                
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
                      document_paths: Dict[str, str] = None) -> pd.DataFrame:
        """Process multiple rows with consistent data application."""
        # Store DEFAULT_VALUE locally
        DEFAULT_VALUE = self.DEFAULT_VALUE
        
        # Clean document data once
        cleaned_extracted = self._clean_extracted_data(extracted_data)
        
        # Extract name from documents to match with rows if needed
        extracted_name = None
        for field in ['name', 'full_name']:
            if field in cleaned_extracted and cleaned_extracted[field] != DEFAULT_VALUE:
                extracted_name = cleaned_extracted[field]
                logger.info(f"Found name in documents: {extracted_name}")
                break
        
        result_rows = []
        
        # First, check if we need to do name matching
        need_matching = False
        if extracted_name and len(excel_data) > 1:
            # Look for exact match in Excel data
            exact_match_found = False
            for idx, row in excel_data.iterrows():
                first_name = str(row.get('First Name', '')).strip()
                last_name = str(row.get('Last Name', '')).strip()
                employee_name = f"{first_name} {last_name}".strip()
                
                # Check for exact match
                if extracted_name.lower() == employee_name.lower():
                    exact_match_found = True
                    break
                    
            # Only do matching if we found an exact match
            need_matching = exact_match_found
            
        # Process each Excel row
        for idx, excel_row in excel_data.iterrows():
            # Clean Excel row
            excel_dict = excel_row.to_dict()
            cleaned_excel = self._clean_excel_data(excel_dict)
            
            # Determine if we should apply document data to this row
            apply_document_data = True
            
            # If we need matching, check if this row matches
            if need_matching and extracted_name:
                first_name = str(excel_dict.get('First Name', '')).strip()
                last_name = str(excel_dict.get('Last Name', '')).strip()
                employee_name = f"{first_name} {last_name}".strip()
                
                # Check if names match
                name_match = extracted_name.lower() == employee_name.lower()
                if not name_match:
                    # Try partial matching
                    common_words = set(extracted_name.lower().split()) & set(employee_name.lower().split())
                    name_match = len(common_words) > 0
                    
                apply_document_data = name_match
                logger.info(f"Row {idx}: {employee_name}, match with document name '{extracted_name}': {name_match}")
            
            # Combine row data
            combined_row = self._combine_row_data(
                cleaned_extracted if apply_document_data else {}, 
                cleaned_excel, 
                document_paths if apply_document_data else None
            )
            
            # Map to template
            mapped_row = self._map_to_template(combined_row, template_columns, field_mappings)
            
            # Ensure standard fields are set regardless of document matching
            # Fill in common mandatory fields for all rows
            self._apply_standard_fields(mapped_row)
            
            # Process emirates_id
            if 'Emirates Id' in mapped_row and mapped_row['Emirates Id'] != DEFAULT_VALUE:
                mapped_row['Emirates Id'] = self._format_emirates_id(mapped_row['Emirates Id'])
            
            # Add row to results
            result_rows.append(mapped_row)
        
        return pd.DataFrame(result_rows)
        
        # Final check for Emirates ID across all rows
        if 'Emirates Id' in result_df.columns:
            result_df['Emirates Id'] = result_df['Emirates Id'].apply(self._format_emirates_id)
        
        return result_df

    def _process_single_row(self, extracted_data: Dict, template_columns: List[str],
                  field_mappings: Dict, document_paths: Dict[str, str] = None) -> pd.DataFrame:
        """Process single row of data."""
        cleaned_data = self._clean_extracted_data(extracted_data)
        combined_data = self._combine_row_data(cleaned_data, {}, document_paths)
        mapped_data = self._map_to_template(combined_data, template_columns, field_mappings)
        
        # Process Emirates ID directly
        for col in mapped_data:
            if 'emirates_id' in col.lower() or 'emirates id' in col.lower():
                mapped_data[col] = self._format_emirates_id(mapped_data[col])
        
        return pd.DataFrame([mapped_data])

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


    def _combine_row_data(self, extracted: Dict, excel: Dict, document_paths: Dict[str, str] = None) -> Dict:
        """Combine data with improved priority rules and field mapping."""
        # Start with a deep copy of Excel data to avoid modification
        combined = copy.deepcopy(excel)
        
        # Debug logging
        logger.info(f"Combining data: {len(extracted)} extracted fields, {len(excel)} excel fields")
        
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
                    else:
                        # Direct field mapping
                        combined[field] = extracted[field]
                        # Also try Excel column names
                        excel_field = field.replace('_', ' ').title()
                        combined[excel_field] = extracted[field]
        
        elif doc_type == 'visa':
            # For visa documents, prioritize visa-specific fields but maintain passport info if already present
            for field in visa_priority_fields:
                if field in extracted and extracted[field] != self.DEFAULT_VALUE:
                    if field == 'entry_permit_no':
                        combined['entry_permit_no'] = extracted[field]
                        combined['Visa File Number'] = extracted[field]
                    elif field == 'visa_file_number':
                        combined['visa_file_number'] = extracted[field]
                        combined['Visa File Number'] = extracted[field]
                        combined['entry_permit_no'] = extracted[field]
                    elif field == 'unified_no':
                        combined['unified_no'] = extracted[field]
                        combined['Unified No'] = extracted[field]
                    elif field == 'full_name':
                        # Only use if we don't have first and last name already
                        if ('first_name' not in combined or combined['first_name'] == self.DEFAULT_VALUE) and \
                        ('last_name' not in combined or combined['last_name'] == self.DEFAULT_VALUE):
                            self._split_full_name(extracted[field], combined)
                    else:
                        # Direct field mapping
                        combined[field] = extracted[field]
                        # Also try Excel column names
                        excel_field = field.replace('_', ' ').title()
                        combined[excel_field] = extracted[field]
                        
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
            'nationality': 'Nationality',
            'passport_number': 'Passport No',
            'emirates_id': 'Emirates Id',
            'visa_file_number': 'Visa File Number',
            'visa_number': 'Visa File Number',
            'unified_no': 'Unified No',
            'u.i.d._no.': 'Unified No',
            'profession': 'Occupation',
            'date_of_birth': 'DOB',
            'name': 'First Name',  # Map full name to first name if needed
        }
        
        # Apply all critical mappings with direct field names
        for ext_field, excel_field in critical_mappings.items():
            if ext_field in extracted and extracted[ext_field] != self.DEFAULT_VALUE:
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
        
        # Field mapping for extracted data
        field_map = {
            'entry_permit_no': ['visa_file_number', 'unified_no', 'permit_number', 'Visa File Number'],
            'emirates_id': ['eid', 'id_number', 'Emirates Id'],
            'passport_number': ['passport_no', 'Passport No'],
            'given_names': ['first_name', 'middle_name', 'First Name', 'Middle Name'],
            'surname': ['last_name', 'Last Name'],
            'full_name': ['name', 'customer_name', 'First Name'],
            'name_en': ['name', 'first_name', 'First Name'],
            'nationality': ['nationality', 'citizenship', 'Nationality'],
            'date_of_birth': ['dob', 'birth_date', 'DOB'],
            'gender': ['sex', 'Gender'],
            'profession': ['occupation', 'job_title', 'Occupation'],
            'expiry_date': ['passport_expiry_date', 'visa_expiry_date', 'Passport Expiry Date', 'Visa Expiry Date'],
            'issue_date': ['date_of_issue']
        }
        
        # Track overridden fields
        overridden = []
        
        # Process all other extracted fields with direct mapping to Excel columns
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
                    if combined[target_key] == self.DEFAULT_VALUE or target_key in id_fields or pd.isna(combined[target_key]):
                        combined[target_key] = value
                        overridden.append(target_key)
                        logger.info(f"Mapped {ext_key} to {target_key}: {value}")
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
                
        # Process contract name from employer name in visa
        if 'sponsor_name' in extracted and extracted['sponsor_name'] != self.DEFAULT_VALUE:
            if 'Contract Name' in combined:
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
                            combined['Contract Name'] = contract
                            break
                elif 'DRESHAK' in employer_name.upper():
                    combined['Contract Name'] = "Dreshak Maintenance LLC"
                    
        # Check for visa file number and set visa issuance emirate
        if 'visa_file_number' in combined and combined['visa_file_number'] != self.DEFAULT_VALUE:
            visa_number = combined['visa_file_number']
            
            # Remove any non-digit characters to extract just the numbers
            digits = ''.join(filter(str.isdigit, str(visa_number)))
            
            # Check if it starts with specific digits
            if digits.startswith('20'):
                logger.info(f"Visa file number {visa_number} starts with 20, setting emirate to Dubai")
                combined['visa_issuance_emirate'] = 'Dubai'
            elif digits.startswith('10'):
                logger.info(f"Visa file number {visa_number} starts with 10, setting emirate to Abu Dhabi")
                combined['visa_issuance_emirate'] = 'Abu Dhabi'
        
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
            
        # Check for and remove duplicate Effective Date at end
        for key in list(mapped.keys()):
            if key != 'Effective Date' and key.lower() == 'effective date':
                # Remove the duplicate
                logger.info(f"Removing duplicate Effective Date field: {key}")
                mapped.pop(key)
                if key in field_mappings:
                    field_mappings.pop(key)
                    
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
        # Store DEFAULT_VALUE locally
        DEFAULT_VALUE = self.DEFAULT_VALUE
        
        # Country values
        row_data['Work Country'] = 'United Arab Emirates'
        row_data['Residence Country'] = 'United Arab Emirates'
        row_data['Commission'] = 'NO'
        
        # Handle visa issuance emirate and related fields
        visa_file_number = None
        if 'Visa File Number' in row_data and row_data['Visa File Number'] != DEFAULT_VALUE:
            visa_file_number = row_data['Visa File Number']
        
        if visa_file_number:
            # Extract just digits
            digits = ''.join(filter(str.isdigit, str(visa_file_number)))
            
            if digits.startswith('201'):
                # Dubai values
                row_data['Visa Issuance Emirate'] = 'Dubai'
                row_data['Work Emirate'] = 'Dubai'
                row_data['Residence Emirate'] = 'Dubai'
                row_data['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                row_data['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                row_data['Member Type'] = 'Expat whose residence issued in Dubai'
            elif digits.startswith('101'):
                # Abu Dhabi values
                row_data['Visa Issuance Emirate'] = 'Abu Dhabi'
                row_data['Work Emirate'] = 'Abu Dhabi'
                row_data['Residence Emirate'] = 'Abu Dhabi'
                row_data['Work Region'] = 'Al Ain City'
                row_data['Residence Region'] = 'Al Ain City'
                row_data['Member Type'] = 'Expat whose residence issued other than Dubai'
            else:
                # Default values
                row_data['Member Type'] = 'Expat whose residence issued other than Dubai'
        
        # Format Mobile No
        if 'Mobile No' in row_data and row_data['Mobile No'] != DEFAULT_VALUE:
            digits = ''.join(filter(str.isdigit, str(row_data['Mobile No'])))
            if len(digits) >= 9:
                row_data['Mobile No'] = digits[-9:]
        
        # Set company contact from personal contact
        if 'Mobile No' in row_data and row_data['Mobile No'] != DEFAULT_VALUE:
            if 'Company Phone' not in row_data or row_data['Company Phone'] == DEFAULT_VALUE:
                row_data['Company Phone'] = row_data['Mobile No']
                
        if 'Email' in row_data and row_data['Email'] != DEFAULT_VALUE:
            if 'Company Mail' not in row_data or row_data['Company Mail'] == DEFAULT_VALUE:
                row_data['Company Mail'] = row_data['Email']
        
        # Copy Staff ID to Family No if needed
        if 'Staff ID' in row_data and row_data['Staff ID'] != DEFAULT_VALUE:
            if 'Family No.' not in row_data or row_data['Family No.'] == DEFAULT_VALUE:
                row_data['Family No.'] = row_data['Staff ID']
        
        # Set Effective Date if missing
        if 'Effective Date' not in row_data or row_data['Effective Date'] == DEFAULT_VALUE:
            row_data['Effective Date'] = datetime.now().strftime('%d/%m/%Y')
        
        # Ensure Middle Name has '.' while other empty fields are truly empty
        for col in row_data:
            col_lower = col.lower()
            # Only Middle Name gets default '.'
            if 'middle' in col_lower and 'name' in col_lower:
                if pd.isna(row_data[col]) or row_data[col] == '' or row_data[col] == DEFAULT_VALUE:
                    row_data[col] = '.'
            # All other fields should be empty rather than '.'
            elif row_data[col] == DEFAULT_VALUE:
                row_data[col] = ''