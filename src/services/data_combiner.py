import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime
import re
import os
from openpyxl import load_workbook

from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.utils.error_handling import ServiceError, handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class DataCombiner:
    """Service for combining OCR and Excel data into template."""

    def __init__(self, textract_processor: TextractProcessor, excel_processor: ExcelProcessor):
        """Initialize with required processors."""
        self.textract_processor = textract_processor
        self.excel_processor = excel_processor
        self.DEFAULT_VALUE = "."

    def combine_and_populate_template(self,
                                   template_path: str,
                                   output_path: str,
                                   document_paths: Dict[str, str],
                                   excel_path: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Combine data from all sources and populate template.
        
        Args:
            template_path: Path to Excel template
            output_path: Path to save populated template
            document_paths: Dict mapping document types to file paths
            excel_path: Optional path to Excel data file
            
        Returns:
            Dict containing success/failure info and any missing fields
        """
        try:
            # Extract data from documents
            ocr_data = {}
            for doc_type, file_path in document_paths.items():
                extracted = self.textract_processor.process_document(file_path, doc_type)
                ocr_data.update(extracted)

            # Process Excel data if provided
            excel_data = {}
            if excel_path:
                df, errors = self.excel_processor.process_excel(excel_path)
                if not df.empty:
                    excel_data = df.iloc[0].to_dict()  # Take first row

            # Combine data with priority
            combined_data = self._combine_data(ocr_data, excel_data)

            # Map combined data to template format
            template_data = self._map_to_template_format(combined_data)

            # Populate template
            self._populate_template(template_path, output_path, template_data)

            # Validate required fields
            missing_fields = self._validate_required_fields(template_data)

            return {
                'status': 'success',
                'missing_fields': missing_fields,
                'output_path': output_path
            }

        except Exception as e:
            logger.error(f"Error combining data: {str(e)}")
            raise ServiceError(f"Data combination failed: {str(e)}")

    def _combine_data(self, ocr_data: Dict, excel_data: Dict) -> Dict:
        """
        Combine OCR and Excel data with priority rules.
        Priority: Excel data for most fields, OCR data for identity documents.
        """
        # Start with all fields defaulted
        combined = {
            'contract_name': self.DEFAULT_VALUE,
            'first_name': self.DEFAULT_VALUE,
            'middle_name': self.DEFAULT_VALUE,
            'last_name': self.DEFAULT_VALUE,
            'effective_date': datetime.now().strftime('%Y-%m-%d'),
            'dob': self.DEFAULT_VALUE,
            'gender': self.DEFAULT_VALUE,
            'marital_status': self.DEFAULT_VALUE,
            'category': self.DEFAULT_VALUE,
            'relation': self.DEFAULT_VALUE,
            'principal_card_no': self.DEFAULT_VALUE,
            'family_no': self.DEFAULT_VALUE,
            'staff_id': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'emirates_id': self.DEFAULT_VALUE,
            'unified_no': self.DEFAULT_VALUE,
            'passport_no': self.DEFAULT_VALUE,
            'work_country': 'UAE',
            'work_emirate': self.DEFAULT_VALUE,
            'work_region': self.DEFAULT_VALUE,
            'residence_country': 'UAE',
            'residence_emirate': self.DEFAULT_VALUE,
            'residence_region': self.DEFAULT_VALUE,
            'email': self.DEFAULT_VALUE,
            'mobile_no': self.DEFAULT_VALUE,
            'salary_band': self.DEFAULT_VALUE,
            'commission': self.DEFAULT_VALUE,
            'visa_issuance_emirate': self.DEFAULT_VALUE,
            'visa_file_number': self.DEFAULT_VALUE,
            'member_type': self.DEFAULT_VALUE
        }

        # First, apply Excel data if available
        if excel_data:
            for field in combined.keys():
                if field in excel_data and excel_data[field] not in [self.DEFAULT_VALUE, 'nan', '', None]:
                    combined[field] = str(excel_data[field])

        # Then, override with OCR data for specific fields
        if ocr_data:
            # Map OCR fields to template fields
            ocr_mapping = {
                'full_name': ['first_name', 'last_name'],  # Split full name
                'nationality': 'nationality',
                'passport_number': 'passport_no',
                'emirates_id': 'emirates_id',
                'date_of_birth': 'dob',
                'sex': 'gender',  # Convert M/F to Male/Female
                'visa_file_number': 'visa_file_number',
                'visa_issuance_emirate': 'visa_issuance_emirate',
                'entry_permit_no': 'unified_no'  # Use entry permit as unified no if not available
            }

            for ocr_field, template_field in ocr_mapping.items():
                if ocr_field in ocr_data and ocr_data[ocr_field] != self.DEFAULT_VALUE:
                    if isinstance(template_field, list):
                        # Handle full name splitting
                        if ocr_field == 'full_name':
                            names = ocr_data[ocr_field].strip().split()
                            if len(names) >= 2:
                                if combined['first_name'] == self.DEFAULT_VALUE:
                                    combined['first_name'] = names[0]
                                if combined['last_name'] == self.DEFAULT_VALUE:
                                    combined['last_name'] = ' '.join(names[1:])
                    else:
                        # Handle specific field conversions
                        if ocr_field == 'sex':
                            combined[template_field] = 'Male' if ocr_data[ocr_field] == 'M' else 'Female'
                        else:
                            if combined[template_field] == self.DEFAULT_VALUE:
                                combined[template_field] = ocr_data[ocr_field]

        # Post-processing
        # Handle date formats
        if combined['dob'] != self.DEFAULT_VALUE:
            try:
                dob = pd.to_datetime(combined['dob'], format='%d/%m/%Y', dayfirst=True)
                combined['dob'] = dob.strftime('%Y-%m-%d')
            except:
                pass

        # Format phone numbers
        if combined['mobile_no'] != self.DEFAULT_VALUE:
            number = re.sub(r'\D', '', combined['mobile_no'])
            if len(number) == 9:
                combined['mobile_no'] = f"+971{number}"
            elif len(number) == 10 and number.startswith('0'):
                combined['mobile_no'] = f"+971{number[1:]}"
            elif len(number) >= 12:
                combined['mobile_no'] = f"+{number}"

        return combined

    def _map_to_template_format(self, data: Dict) -> Dict:
        """Map combined data to template format."""
        # Add any specific formatting required by template
        template_data = data.copy()
        
        # Format dates
        if 'dob' in template_data and template_data['dob'] != self.DEFAULT_VALUE:
            try:
                dob = pd.to_datetime(template_data['dob'])
                template_data['dob'] = dob.strftime('%Y-%m-%d')
            except:
                template_data['dob'] = self.DEFAULT_VALUE

        # Format phone numbers
        if template_data.get('mobile_no') != self.DEFAULT_VALUE:
            phone = template_data['mobile_no']
            if not phone.startswith('+971'):
                phone = '+971' + phone.lstrip('0')
            template_data['mobile_no'] = phone

        return template_data

    def _populate_template(self, template_path: str, output_path: str, data: Dict) -> None:
        """Populate Excel template with data."""
        # Copy template
        import shutil
        shutil.copy2(template_path, output_path)

        # Load workbook
        workbook = load_workbook(output_path)
        sheet = workbook['Sample Template']

        # Find column indices from header row
        headers = {}
        for col in range(1, sheet.max_column + 1):
            cell_value = sheet.cell(row=1, column=col).value
            if cell_value:
                field_name = self._clean_field_name(cell_value)
                headers[field_name] = col

        # Populate data in first empty row
        row = 2  # Start after header
        while sheet.cell(row=row, column=1).value is not None:
            row += 1

        # Write data
        for field, value in data.items():
            if field in headers:
                sheet.cell(row=row, column=headers[field], value=value)

        # Save workbook
        workbook.save(output_path)

    def _clean_field_name(self, field_name: str) -> str:
        """Clean field name to match dictionary keys."""
        return (field_name.lower()
                .replace(' ', '_')
                .replace('/', '_')
                .replace('-', '_')
                .strip('_'))

    def _validate_required_fields(self, data: Dict) -> List[str]:
        """Validate all required fields are populated."""
        missing = []
        for field, value in data.items():
            if value == self.DEFAULT_VALUE:
                missing.append(field)
        return missing