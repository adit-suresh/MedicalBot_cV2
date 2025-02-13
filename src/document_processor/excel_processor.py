import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
import os
from datetime import datetime
from openpyxl import load_workbook
import re

from src.utils.error_handling import ServiceError, handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class ExcelProcessor:
    """Processor for handling Excel files."""

    def __init__(self):
        """Initialize Excel processor."""
        # Define required fields and their types/formats
        self.required_fields = {
            'contract_name': str,
            'first_name': str,
            'middle_name': str,
            'last_name': str,
            'effective_date': 'date',
            'dob': 'date',
            'gender': ['Male', 'Female'],
            'marital_status': ['Married', 'Divorced', 'Widowed', 'Single'],
            'category': str,
            'relation': str,
            'principal_card_no': str,
            'family_no': str,
            'staff_id': str,
            'nationality': str,
            'emirates_id': str,
            'unified_no': str,
            'passport_no': str,
            'work_country': str,
            'work_emirate': str,
            'work_region': str,
            'residence_country': str,
            'residence_emirate': str,
            'residence_region': str,
            'email': str,
            'mobile_no': str,
            'salary_band': str,
            'commission': str,
            'visa_issuance_emirate': str,
            'visa_file_number': str,
            'member_type': str
        }

        # Default value for missing fields
        self.DEFAULT_VALUE = "."

    @handle_errors(ErrorCategory.PROCESS, ErrorSeverity.MEDIUM)
    def validate_template(self, file_path: str) -> Tuple[bool, List[str]]:
        """
        Validate Excel template against required fields.
        
        Args:
            file_path: Path to Excel template file
            
        Returns:
            Tuple of (validity, missing fields)
        """
        try:
            # Load Excel file
            wb = load_workbook(file_path)
            ws = wb.active

            # Get header row
            header = [cell.value for cell in ws[1]]
            missing_fields = [field for field in self.required_fields.keys() if field not in header]

            return len(missing_fields) == 0, missing_fields

        except Exception as e:
            logger.error(f"Error validating Excel template: {str(e)}")
            raise ServiceError(f"Excel template validation failed: {str(e)}")
        
    def process_excel(self, file_path: str, dayfirst: bool = True) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Process Excel file and validate data.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            Tuple of (processed dataframe, list of validation errors)
        """
        try:
            
            df = pd.read_excel(file_path)
            
            # Clean column names
            df.columns = [self._clean_column_name(col) for col in df.columns]

            # Fill missing values with default
            for field in self.required_fields.keys():
                if field not in df.columns:
                    df[field] = self.DEFAULT_VALUE

            # Validate data
            errors = self._validate_data(df)

            # Clean and standardize data
            df = self._clean_data(df, dayfirst=dayfirst)

            return df, errors

        except Exception as e:
            logger.error(f"Error processing Excel file: {str(e)}")
            raise ServiceError(f"Excel processing failed: {str(e)}")

    def _clean_column_name(self, column: str) -> str:
        """Clean and standardize column names."""
        if not isinstance(column, str):
            return ''
        # Remove special characters and spaces
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', column.lower().strip())
        # Remove multiple underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        # Remove leading/trailing underscores
        return clean_name.strip('_')

    def _validate_data(self, df: pd.DataFrame) -> List[Dict]:
        """Validate DataFrame against required fields and formats."""
        errors = []

        for field, validation in self.required_fields.items():
            if field not in df.columns:
                errors.append({
                    'field': field,
                    'error': 'Missing required column'
                })
                continue

            # Validate based on type
            if validation == 'date':
                try:
                    pd.to_datetime(df[field], errors='raise')
                except Exception as e:
                    errors.append({
                        'field': field,
                        'error': f'Invalid date format: {str(e)}'
                    })
            elif isinstance(validation, list):
                invalid_mask = ~df[field].isin(validation)
                invalid_values = df[field][invalid_mask].unique()
                if len(invalid_values) > 0:
                    errors.append({
                        'field': field,
                        'error': f'Invalid values: {invalid_values}',
                        'valid_values': validation
                    })

        return errors

    def _clean_data(self, df: pd.DataFrame, dayfirst: bool = True) -> pd.DataFrame:
        """Clean and standardize data."""
        # Handle dates
        date_fields = [field for field, val_type in self.required_fields.items() 
                      if val_type == 'date' and field in df.columns]
        for field in date_fields:
            try:
                df[field] = pd.to_datetime(df[field], dayfirst=dayfirst).dt.strftime('%Y-%m-%d')
            except:
                df[field] = self.DEFAULT_VALUE

        # Clean string fields
        string_fields = [field for field, val_type in self.required_fields.items() 
                        if val_type == str and field in df.columns]
        for field in string_fields:
            df[field] = df[field].fillna(self.DEFAULT_VALUE)
            df[field] = df[field].astype(str).str.strip()

        # Handle phone numbers
        if 'mobile_no' in df.columns:
            df['mobile_no'] = df['mobile_no'].apply(self._format_phone_number)

        # Handle email addresses
        if 'email' in df.columns:
            df['email'] = df['email'].str.lower()

        return df

    def _format_phone_number(self, number: str) -> str:
        """Format phone numbers consistently."""
        if pd.isna(number) or number == self.DEFAULT_VALUE:
            return self.DEFAULT_VALUE
        
        # Remove non-numeric characters
        number = re.sub(r'\D', '', str(number))
        
        # Add country code if missing
        if len(number) == 9:  # Local number without country code
            number = '971' + number
        elif len(number) == 10 and number.startswith('0'):  # Local number with leading 0
            number = '971' + number[1:]
            
        return f"+{number}"