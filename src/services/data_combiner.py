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
    
    def __init__(self, textract_processor, excel_processor):
        """Initialize the data combiner.
        
        Args:
            textract_processor: Processor for document text extraction
            excel_processor: Processor for Excel file handling
        """
        self.textract_processor = textract_processor
        self.excel_processor = excel_processor
        self.DEFAULT_VALUE = '.'
        
        # Pre-initialize field mappings for better performance
        self._field_mapping = self._initialize_field_mapping()
        self._date_fields = self._initialize_date_fields()
        self._numeric_fields = self._initialize_numeric_fields()
        
        # Caching mechanism for template structure
        self._template_cache = {}
        self._template_cache_lock = threading.RLock()
        
    def _initialize_field_mapping(self) -> Dict[str, List[str]]:
        """Initialize comprehensive field mapping dictionary."""
        return {
            # Personal identification fields
            'passport_no': ['passport_number', 'passport', 'passport_no', 'passportnumber'],
            'passport_number': ['passport_no', 'passport', 'passportnumber'],
            'emirates_id': ['eid', 'emiratesid', 'emirates_id_number', 'id_number', 'uae_id'],
            'eid': ['emirates_id', 'emirates_id_number', 'id_number', 'uae_id'],
            'unified_no': ['unified_number', 'unified', 'entry_permit_no', 'visa_number'],
            'entry_permit_no': ['visa_number', 'visa_file_number', 'permit_number', 'unified_no'],
            
            # Personal information fields
            'first_name': ['firstname', 'fname', 'given_name', 'given_names', 'name_first', 'first'],
            'middle_name': ['middlename', 'mname', 'name_middle', 'middle'],
            'last_name': ['lastname', 'lname', 'surname', 'name_last', 'family_name', 'last'],
            'full_name': ['name', 'complete_name', 'person_name', 'customer_name'],
            'gender': ['sex', 'gender_type'],
            'dob': ['date_of_birth', 'birth_date', 'birthdate', 'birth_day'],
            'date_of_birth': ['dob', 'birth_date', 'birthdate', 'birth_day'],
            'nationality': ['nation', 'citizenship', 'country', 'country_of_birth'],
            'mobile_no': ['phone', 'phone_number', 'mobile', 'contact_number', 'cell'],
            'email': ['email_address', 'mail', 'email_id'],
            
            # Document information fields
            'expiry_date': ['valid_until', 'expires_on', 'date_of_expiry', 'passport_expiry_date', 'visa_expiry_date'],
            'issue_date': ['date_of_issue', 'issued_on', 'start_date'],
            'visa_type': ['visa_category', 'permit_type', 'residence_type'],
            'profession': ['occupation', 'job_title', 'position', 'employment'],
            
            # Insurance specific fields
            'policy_number': ['policy_no', 'policy', 'insurance_policy', 'plan_number'],
            'insurance_company': ['insurer', 'provider', 'company', 'carrier'],
            'plan_type': ['policy_type', 'coverage_type', 'plan', 'insurance_type'],
            'member_type': ['relationship', 'relation', 'dependent_type', 'role'],
            'staff_id': ['employee_id', 'employee_no', 'staff_number', 'worker_id'],
            'effective_date': ['start_date', 'coverage_start', 'policy_start', 'begin_date'],
            'marital_status': ['marriage_status', 'civil_status'],
            'premium': ['insurance_premium', 'cost', 'annual_premium'],
            'coverage_amount': ['sum_insured', 'benefit_amount', 'coverage', 'insured_amount'],
            'salary_band': ['salary_range', 'income_band', 'salary_bracket']
        }
    
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

    @handle_errors(ErrorCategory.PROCESSING, ErrorSeverity.MEDIUM)
    def combine_and_populate_template(self, template_path: str, output_path: str, 
                                    extracted_data: Dict, excel_data: Optional[pd.DataFrame] = None) -> Dict:
        """
        Combine data from all sources and populate template with performance optimizations.
        
        Args:
            template_path: Path to Excel template
            output_path: Path to save output
            extracted_data: Data extracted from documents
            excel_data: DataFrame from original Excel
            
        Returns:
            Dict: Result dictionary with status and details
            
        Raises:
            ServiceError: If data combination fails
        """
        start_time = time.time()
        try:
            # Validate inputs
            if not os.path.exists(template_path):
                raise FileNotFoundError(f"Template file not found: {template_path}")
                
            # Use cached template structure if available
            template_info = self._get_template_structure(template_path)
            template_columns = template_info['columns']
            
            # Track field mapping for debugging
            field_mappings = {}
            
            # Process data based on excel_data type
            if excel_data is not None:
                # Handle both DataFrame and dict inputs
                if isinstance(excel_data, dict):
                    excel_data = pd.DataFrame([excel_data])
                elif not isinstance(excel_data, pd.DataFrame):
                    raise TypeError(f"Excel data must be DataFrame or dict, got {type(excel_data)}")
                    
                if excel_data.empty:
                    logger.warning("Excel data is empty, using document data only")
                    result_df = self._process_single_row(extracted_data, template_columns, field_mappings)
                else:
                    logger.info(f"Processing multiple rows ({len(excel_data)}) with document data")
                    result_df = self._process_multiple_rows(extracted_data, excel_data, template_columns, field_mappings)
            else:
                logger.info("No Excel data provided, using document data only")
                result_df = self._process_single_row(extracted_data, template_columns, field_mappings)
            
            # Clean and standardize final data
            result_df = self._clean_final_dataframe(result_df)
            
            # Save to output file
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                
            result_df.to_excel(output_path, index=False)
            
            processing_time = time.time() - start_time
            logger.info(f"Data combined successfully in {processing_time:.2f}s: "
                       f"{len(result_df)} rows, {len(template_columns)} columns")
            
            # Log field mapping summary for debugging
            if logger.isEnabledFor(logging.DEBUG):
                for dest_field, source_field in field_mappings.items():
                    logger.debug(f"Mapped '{source_field}' to template field '{dest_field}'")
            
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
                            template_columns: List[str], field_mappings: Dict) -> pd.DataFrame:
        """Process multiple rows of data with batch processing for performance."""
        result_rows = []
        
        # Clean document data once
        cleaned_extracted = self._clean_extracted_data(extracted_data)
        
        # Process each row
        for idx, excel_row in excel_data.iterrows():
            # Clean Excel row
            excel_dict = excel_row.to_dict()
            cleaned_excel = self._clean_excel_data(excel_dict)
            
            # Combine row data
            combined_row = self._combine_row_data(cleaned_extracted, cleaned_excel)
            
            # Map to template
            mapped_row = self._map_to_template(combined_row, template_columns, field_mappings)
            result_rows.append(mapped_row)
        
        return pd.DataFrame(result_rows)

    def _process_single_row(self, extracted_data: Dict, template_columns: List[str],
                          field_mappings: Dict) -> pd.DataFrame:
        """Process single row of data."""
        cleaned_data = self._clean_extracted_data(extracted_data)
        mapped_data = self._map_to_template(cleaned_data, template_columns, field_mappings)
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

    def _clean_excel_data(self, data: Dict) -> Dict:
        """Clean Excel data with better handling of special values."""
        cleaned = {}
        for key, value in data.items():
            # Normalize key
            normalized_key = self._normalize_field_name(key)
            
            # Handle different value types
            if pd.isna(value) or value == '' or str(value).lower() == 'nan':
                cleaned[normalized_key] = self.DEFAULT_VALUE
            elif isinstance(value, (int, float)):
                if normalized_key in self._date_fields and isinstance(value, float):
                    # Convert Excel date number to date string
                    cleaned[normalized_key] = self._format_excel_date(value)
                elif np.isclose(value, int(value)):
                    # Convert whole numbers without decimal point
                    cleaned[normalized_key] = str(int(value))
                else:
                    cleaned[normalized_key] = str(value)
            elif isinstance(value, datetime):
                # Format datetime objects
                cleaned[normalized_key] = value.strftime('%Y-%m-%d')
            else:
                # Clean up string values
                str_value = str(value).strip()
                
                # Apply field-specific formatting
                if normalized_key in self._date_fields:
                    cleaned[normalized_key] = self._format_date_value(str_value)
                elif normalized_key in self._numeric_fields:
                    cleaned[normalized_key] = self._format_numeric_value(str_value)
                else:
                    cleaned[normalized_key] = str_value
                    
        return cleaned

    def _combine_row_data(self, extracted: Dict, excel: Dict) -> Dict:
        """Combine data with improved priority rules and field mapping."""
        # Start with a deep copy of Excel data to avoid modification
        combined = copy.deepcopy(excel)
        
        # Identify special fields that require custom handling
        name_fields = ['first_name', 'middle_name', 'last_name', 'full_name']
        id_fields = ['passport_number', 'emirates_id', 'entry_permit_no', 'unified_no']
        
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
                
            # Use field mapping to find matching fields in combined data
            target_keys = field_map.get(ext_key, [ext_key])
            for target_key in target_keys:
                if target_key in combined and combined[target_key] == self.DEFAULT_VALUE:
                    combined[target_key] = value
                    overridden.append(target_key)
                    break
        
        # Priority check for ID fields - prefer OCR data for these fields
        for id_field in id_fields:
            if id_field in extracted and extracted[id_field] != self.DEFAULT_VALUE:
                # Use OCR data for ID fields regardless of Excel data
                combined[id_field] = extracted[id_field]
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Overridden fields from OCR data: {overridden}")
            
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

    def _map_to_template(self, data: Dict, template_columns: List[str],
                       field_mappings: Dict) -> Dict:
        """Map combined data to template columns with improved field detection."""
        mapped = {}
        
        for col in template_columns:
            normalized_col = self._normalize_column_name(col)
            
            # Try direct match first
            if normalized_col in data:
                mapped[col] = data[normalized_col]
                field_mappings[col] = normalized_col
                continue
                
            # Check field variations using the mapping
            mapped_value = self.DEFAULT_VALUE
            for field, variations in self._field_mapping.items():
                if normalized_col in variations and field in data:
                    mapped_value = data[field]
                    field_mappings[col] = field
                    break
                elif field == normalized_col and any(var in data for var in variations):
                    # Find first matching variation
                    for var in variations:
                        if var in data:
                            mapped_value = data[var]
                            field_mappings[col] = var
                            break
                    break
                    
            # Use default value if no mapping found
            if col not in field_mappings:
                field_mappings[col] = None
                
            mapped[col] = mapped_value
                
        return mapped

    def _clean_final_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize final DataFrame with improved formatting."""
        # Replace NaN/None with default value
        df = df.fillna(self.DEFAULT_VALUE)
        
        # Ensure all columns are strings
        for col in df.columns:
            df[col] = df[col].astype(str)
            
            # Apply normalization based on field type
            normalized_col = self._normalize_column_name(col)
            
            # Handle date fields
            if normalized_col in self._date_fields:
                df[col] = df[col].apply(self._format_date_value)
                
            # Handle numeric fields
            elif normalized_col in self._numeric_fields:
                df[col] = df[col].apply(
                    lambda x: self._format_numeric_value(x) if x != self.DEFAULT_VALUE else x
                )
                
            # Clean up strings (remove excess whitespace)
            else:
                df[col] = df[col].apply(
                    lambda x: re.sub(r'\s+', ' ', x).strip() if x != self.DEFAULT_VALUE else x
                )
                
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
        """Convert Excel date number to YYYY-MM-DD format."""
        try:
            # Excel dates are days since 1900-01-01 (or 1904-01-01 on Mac)
            # Adjust based on the epoch
            date_value = datetime(1899, 12, 30) + timedelta(days=excel_date)
            return date_value.strftime('%Y-%m-%d')
        except Exception:
            return str(excel_date)

    def _format_date_value(self, date_str: str) -> str:
        """Format date string to YYYY-MM-DD standard format."""
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
                    date_obj = datetime.strptime(date_str, fmt)
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        # If no format worked, try pandas to_datetime
        try:
            date_obj = pd.to_datetime(date_str)
            return date_obj.strftime('%Y-%m-%d')
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