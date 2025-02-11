import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.utils.error_handling import ServiceError, handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class DataIntegrator:
    """Service for integrating data from multiple sources."""

    def __init__(self, textract_processor: TextractProcessor, excel_processor: ExcelProcessor):
        """Initialize with required processors."""
        self.textract_processor = textract_processor
        self.excel_processor = excel_processor

    @handle_errors(ErrorCategory.PROCESSING, ErrorSeverity.HIGH)
    def process_documents(self, 
                        document_paths: Dict[str, str],
                        excel_path: Optional[str] = None) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Process documents and Excel file, combining the data.
        
        Args:
            document_paths: Dict mapping document types to file paths
            excel_path: Optional path to Excel file with additional data
            
        Returns:
            Tuple of (combined DataFrame, list of validation errors)
        """
        errors = []
        extracted_data = {}

        # Process documents using Textract
        for doc_type, file_path in document_paths.items():
            try:
                result = self.textract_processor.process_document(file_path, doc_type)
                extracted_data.update(result)
            except Exception as e:
                errors.append({
                    'source': f'{doc_type}_document',
                    'file': file_path,
                    'error': str(e)
                })

        # Create DataFrame from extracted document data
        docs_df = pd.DataFrame([extracted_data])

        # Process Excel file if provided
        excel_df = None
        if excel_path:
            try:
                excel_df, excel_errors = self.excel_processor.process_excel(excel_path)
                errors.extend(excel_errors)
            except Exception as e:
                errors.append({
                    'source': 'excel',
                    'file': excel_path,
                    'error': str(e)
                })

        # Combine data sources
        combined_df = self._combine_data(docs_df, excel_df)
        
        # Validate combined data
        validation_errors = self._validate_combined_data(combined_df)
        errors.extend(validation_errors)

        return combined_df, errors

    def _combine_data(self, docs_df: pd.DataFrame, excel_df: Optional[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine document and Excel data.
        
        Args:
            docs_df: DataFrame from document extraction
            excel_df: Optional DataFrame from Excel file
            
        Returns:
            Combined DataFrame
        """
        if excel_df is None:
            return docs_df

        # Identify matching records between sources
        merge_keys = []
        if 'emirates_id' in docs_df.columns and 'emirates_id' in excel_df.columns:
            merge_keys.append('emirates_id')
        if 'passport_number' in docs_df.columns and 'passport_number' in excel_df.columns:
            merge_keys.append('passport_number')

        if not merge_keys:
            logger.warning("No common keys found for merging data")
            return docs_df

        # Merge data, preferring document-extracted values over Excel values
        combined_df = pd.merge(
            docs_df,
            excel_df,
            on=merge_keys,
            how='left',
            suffixes=('_doc', '')
        )

        # Resolve conflicts (prefer document-extracted values)
        for col in combined_df.columns:
            if col.endswith('_doc'):
                base_col = col[:-4]
                if base_col in combined_df.columns:
                    combined_df[base_col] = combined_df[col].combine_first(combined_df[base_col])
                combined_df.drop(col, axis=1, inplace=True)

        return combined_df

    def _validate_combined_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Validate combined dataset.
        
        Args:
            df: Combined DataFrame
            
        Returns:
            List of validation errors
        """
        errors = []

        # Required fields validation
        required_fields = {
            'emirates_id': r'784-\d{4}-\d{7}-\d{1}',
            'passport_number': r'[A-Z0-9]{6,9}',
            'first_name': r'.+',
            'last_name': r'.+',
            'nationality': r'.+'
        }

        for field, pattern in required_fields.items():
            if field not in df.columns:
                errors.append({
                    'field': field,
                    'error': 'Required field missing from dataset'
                })
                continue

            # Check for missing values
            missing_mask = df[field].isna()
            if missing_mask.any():
                errors.append({
                    'field': field,
                    'error': 'Missing required value',
                    'rows': missing_mask[missing_mask].index.tolist()
                })

            # Validate format
            import re
            invalid_mask = ~df[field].astype(str).str.match(pattern)
            invalid_mask = invalid_mask & ~df[field].isna()
            if invalid_mask.any():
                errors.append({
                    'field': field,
                    'error': f'Invalid format',
                    'rows': invalid_mask[invalid_mask].index.tolist()
                })

        return errors

    def create_output_excel(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Create standardized output Excel file.
        
        Args:
            df: Combined DataFrame
            output_path: Path to save output file
        """
        self.excel_processor.create_standardized_excel(df, output_path)

    def get_missing_documents(self, document_paths: Dict[str, str]) -> List[str]:
        """
        Get list of missing required documents.
        
        Args:
            document_paths: Dict of document paths
            
        Returns:
            List of missing document types
        """
        required_docs = {'passport', 'emirates_id'}
        provided_docs = set(document_paths.keys())
        return list(required_docs - provided_docs)