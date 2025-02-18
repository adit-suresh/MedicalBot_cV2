from typing import Dict, Optional, List
import pandas as pd
import os
import logging
import re
from src.utils.error_handling import ServiceError

logger = logging.getLogger(__name__)

class DataCombiner:
    """Combines data from documents and Excel files."""
    
    def __init__(self, textract_processor, excel_processor):
        self.textract_processor = textract_processor
        self.excel_processor = excel_processor
        self.DEFAULT_VALUE = '.'

    def combine_and_populate_template(self, template_path: str, output_path: str, 
                                    extracted_data: Dict, excel_data: Optional[pd.DataFrame] = None) -> Dict:
        """
        Combine data from all sources and populate template.
        
        Args:
            template_path: Path to Excel template
            output_path: Path to save output
            extracted_data: Data extracted from documents
            excel_data: DataFrame from original Excel
        """
        try:
            logger.info("Starting data combination process...")
            
            # Read template
            template_df = pd.read_excel(template_path)
            template_columns = template_df.columns.tolist()
            
            # Process data based on whether Excel data exists
            if excel_data is not None and not excel_data.empty:
                result_df = self._process_multiple_rows(extracted_data, excel_data, template_columns)
            else:
                result_df = self._process_single_row(extracted_data, template_columns)
            
            # Validate and clean result
            result_df = self._clean_final_dataframe(result_df)
            
            # Save to output file
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            result_df.to_excel(output_path, index=False)
            
            logger.info(f"Successfully combined data into {output_path}")
            return {
                'status': 'success',
                'output_path': output_path,
                'rows_processed': len(result_df)
            }

        except Exception as e:
            logger.error(f"Error combining data: {str(e)}")
            raise ServiceError(f"Data combination failed: {str(e)}")

    def _process_multiple_rows(self, extracted_data: Dict, excel_data: pd.DataFrame, 
                             template_columns: List[str]) -> pd.DataFrame:
        """Process multiple rows of data."""
        result_rows = []
        
        for idx, excel_row in excel_data.iterrows():
            # Clean and combine data for this row
            cleaned_extracted = self._clean_extracted_data(extracted_data)
            excel_dict = excel_row.to_dict()
            cleaned_excel = self._clean_excel_data(excel_dict)
            
            # Combine data giving priority to Excel
            combined_row = self._combine_row_data(cleaned_extracted, cleaned_excel)
            
            # Map to template format
            mapped_row = self._map_to_template(combined_row, template_columns)
            result_rows.append(mapped_row)
        
        return pd.DataFrame(result_rows)

    def _process_single_row(self, extracted_data: Dict, template_columns: List[str]) -> pd.DataFrame:
        """Process single row of data."""
        cleaned_data = self._clean_extracted_data(extracted_data)
        mapped_data = self._map_to_template(cleaned_data, template_columns)
        return pd.DataFrame([mapped_data])

    def _clean_extracted_data(self, data: Dict) -> Dict:
        """Clean extracted document data."""
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, str):
                # Remove extra whitespace and slashes
                cleaned_value = value.strip().replace(' / ', '/').replace('  ', ' ')
                cleaned[key] = cleaned_value if cleaned_value != '' else self.DEFAULT_VALUE
            else:
                cleaned[key] = self.DEFAULT_VALUE if value is None else str(value)
        return cleaned

    def _clean_excel_data(self, data: Dict) -> Dict:
        """Clean Excel data."""
        cleaned = {}
        for key, value in data.items():
            if pd.isna(value) or value == '' or str(value).lower() == 'nan':
                cleaned[key] = self.DEFAULT_VALUE
            elif isinstance(value, (int, float)):
                cleaned[key] = str(int(value)) if value.is_integer() else str(value)
            else:
                cleaned[key] = str(value).strip()
        return cleaned

    def _combine_row_data(self, extracted: Dict, excel: Dict) -> Dict:
        """Combine data with priority rules."""
        combined = {}
        
        # Start with Excel data
        combined.update(excel)
        
        # Field mapping for extracted data
        field_map = {
            'entry_permit_no': ['visa_file_number', 'unified_no'],
            'full_name': ['first_name', 'last_name'],
            'nationality': 'nationality',
            'passport_number': ['passport_no', 'passport_number'],
            'date_of_birth': ['dob', 'date_of_birth'],
            'profession': ['occupation', 'profession'],
            'visa_issue_date': 'visa_issue_date',
            'visa_issuance_emirate': 'visa_issuance_emirate',
            'emirates_id': 'emirates_id'
        }
        
        # Override with extracted data using mapping
        for ext_key, temp_keys in field_map.items():
            if ext_key not in extracted or extracted[ext_key] == self.DEFAULT_VALUE:
                continue
                
            if isinstance(temp_keys, list):
                # Try multiple possible column names
                for temp_key in temp_keys:
                    if temp_key in combined and combined[temp_key] == self.DEFAULT_VALUE:
                        if ext_key == 'full_name':
                            self._handle_full_name(extracted[ext_key], combined)
                        else:
                            combined[temp_key] = extracted[ext_key]
                        break
            else:
                # Single column name
                if temp_keys in combined and combined[temp_keys] == self.DEFAULT_VALUE:
                    combined[temp_keys] = extracted[ext_key]
        
        return combined

    def _handle_full_name(self, full_name: str, combined: Dict) -> None:
        """Handle splitting full name into parts."""
        names = full_name.split()
        if len(names) > 1:
            if 'first_name' in combined and combined['first_name'] == self.DEFAULT_VALUE:
                combined['first_name'] = ' '.join(names[:-1])
            if 'last_name' in combined and combined['last_name'] == self.DEFAULT_VALUE:
                combined['last_name'] = names[-1]

    def _map_to_template(self, data: Dict, template_columns: List[str]) -> Dict:
        """Map combined data to template columns."""
        mapped = {}
        
        # Create reverse mapping for variations
        reverse_map = {
            'passport_no': ['passport_number', 'passport'],
            'emirates_id': ['eid', 'emiratesid', 'emirates_id_number'],
            'first_name': ['firstname', 'fname'],
            'last_name': ['lastname', 'lname']
        }
        
        for col in template_columns:
            col_key = self._normalize_column_name(col)
            
            # Check direct match
            if col_key in data:
                mapped[col] = data[col_key]
                continue
                
            # Check variations
            found = False
            for main_key, variations in reverse_map.items():
                if col_key in variations and main_key in data:
                    mapped[col] = data[main_key]
                    found = True
                    break
                    
            if not found:
                mapped[col] = self.DEFAULT_VALUE
                
        return mapped

    def _normalize_column_name(self, column: str) -> str:
        """Normalize column names for mapping."""
        if not isinstance(column, str):
            return ''
            
        # Clean name
        clean_name = column.lower().strip()
        clean_name = re.sub(r'[^a-z0-9\s_]', '', clean_name)
        clean_name = clean_name.replace(' ', '_')
        
        # Common variations mapping
        name_map = {
            'passport_no': 'passport_number',
            'eid': 'emirates_id',
            'dob': 'date_of_birth',
            'fname': 'first_name',
            'lname': 'last_name'
        }
        
        return name_map.get(clean_name, clean_name)

    def _clean_final_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate final DataFrame."""
        # Replace NaN/None with default value
        df = df.fillna(self.DEFAULT_VALUE)
        
        # Convert all values to strings
        for col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].apply(lambda x: x.strip() if x != self.DEFAULT_VALUE else x)
            
        # Handle date columns
        date_columns = ['dob', 'date_of_birth', 'effective_date', 
                       'passport_expiry_date', 'visa_expiry_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._format_date)
                
        return df

    def _format_date(self, date_str: str) -> str:
        """Format date string to YYYY-MM-DD."""
        if date_str == self.DEFAULT_VALUE:
            return date_str
            
        try:
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
                try:
                    return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
                except:
                    continue
            
            # If no format works, try pandas default parser
            return pd.to_datetime(date_str).strftime('%Y-%m-%d')
            
        except:
            return date_str