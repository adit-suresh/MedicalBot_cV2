import os
import sys
import logging
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(f'debug_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def comprehensive_template_test():
    """Test all three templates with the same documents to ensure consistency."""
    
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE TEMPLATE CONSISTENCY TEST")
    logger.info("=" * 80)
    
    # Import components
    try:
        from test_complete_workflow import WorkflowTester
        from src.document_processor.gpt_processor import GPTProcessor 
        from src.services.data_combiner import DataCombiner 
        from src.document_processor.excel_processor import EnhancedExcelProcessor 
        
    except ImportError as e:
        logger.error(f"Import error: {e}")
        return False

    # Initialize components
    try:
        tester = WorkflowTester()
        logger.info("✅ WorkflowTester initialized")
    except Exception as e:
        logger.error(f"Failed to initialize WorkflowTester: {e}")
        return False

    # Test data setup
    test_scenarios = {
        'nas': {
            'template': 'templates/nas.xlsx',
            'excel_file': 'test_data/nas_input.xlsx',
            'expected_columns': ['First Name', 'Middle Name', 'Last Name', 'Emirates Id', 'Unified No', 'Visa File Number', 'Passport No']
        },
        'almadallah': {
            'template': 'templates/al_madallah.xlsx', 
            'excel_file': 'test_data/almadallah_input.xlsx',
            'expected_columns': ['FIRSTNAME', 'MIDDLENAME', 'LASTNAME', 'EMIRATESID', 'UIDNO', 'VISAFILEREF', 'PASSPORTNO']
        },
        'takaful': {
            'template': 'templates/takaful.xlsx',
            'excel_file': 'test_data/takaful_input.xlsx', 
            'expected_columns': ['FirstName', 'SecondName', 'LastName', 'EIDNumber', 'UIDNo', 'ResidentFileNumber', 'PassportNum']
        }
    }

    # Mock extracted data (same for all tests)
    mock_extracted_data = {
        'passport_number': 'AB1234567',
        'emirates_id': '784-1234-1234567-1',
        'unified_no': '123456789',
        'visa_file_number': '201/2024/123456',
        'nationality': 'Indian',
        'date_of_birth': '01/01/1990',
        'gender': 'Male',
        'mobile_no': '501234567',
        'email': 'test@example.com',
        'first_name': 'John',
        'middle_name': 'Middle',
        'last_name': 'Doe'
    }

    # Mock Excel data (same structure, different field names)
    mock_excel_data = {
        'nas': pd.DataFrame([
            {'First Name': 'John Smith', 'Middle Name': '.', 'Last Name': '', 'Contract Name': 'Test Contract'},
            {'First Name': 'Jane Doe', 'Middle Name': '.', 'Last Name': '', 'Contract Name': 'Test Contract'}
        ]),
        'almadallah': pd.DataFrame([
            {'FIRSTNAME': 'John Smith', 'MIDDLENAME': '.', 'LASTNAME': '', 'Subgroup Name': 'Test Contract'},
            {'FIRSTNAME': 'Jane Doe', 'MIDDLENAME': '.', 'LASTNAME': '', 'Subgroup Name': 'Test Contract'}
        ]),
        'takaful': pd.DataFrame([
            {'FirstName': 'John Smith', 'SecondName': '.', 'LastName': '', 'SubGroupDivision': 'Test Contract'},
            {'FirstName': 'Jane Doe', 'SecondName': '.', 'LastName': '', 'SubGroupDivision': 'Test Contract'}
        ])
    }

    results = {}
    
    for template_name, config in test_scenarios.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"TESTING TEMPLATE: {template_name.upper()}")
        logger.info(f"{'='*60}")
        
        try:
            # Check if template exists
            if not os.path.exists(config['template']):
                logger.error(f"Template not found: {config['template']}")
                results[template_name] = {'status': 'failed', 'error': 'Template not found'}
                continue

            # Test data combination
            output_path = f"debug_output_{template_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            try:
                result = tester.data_combiner.combine_and_populate_template(
                    template_path=config['template'],
                    output_path=output_path,
                    extracted_data=mock_extracted_data,
                    excel_data=mock_excel_data[template_name],
                    document_paths={}
                )
                
                if result['status'] == 'success':
                    # Analyze the output
                    analysis = analyze_output_file(output_path, config['expected_columns'], mock_extracted_data)
                    results[template_name] = {
                        'status': 'success',
                        'rows_processed': result['rows_processed'],
                        'analysis': analysis,
                        'output_file': output_path
                    }
                    logger.info(f"✅ {template_name} template processed successfully")
                else:
                    results[template_name] = {'status': 'failed', 'error': result.get('error', 'Unknown error')}
                    logger.error(f"❌ {template_name} template failed: {result.get('error')}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing {template_name}: {str(e)}")
                results[template_name] = {'status': 'failed', 'error': str(e)}
                
        except Exception as e:
            logger.error(f"❌ Setup error for {template_name}: {str(e)}")
            results[template_name] = {'status': 'failed', 'error': f'Setup error: {str(e)}'}

    # Generate comprehensive report
    generate_consistency_report(results, mock_extracted_data)
    
    return results

def analyze_output_file(file_path: str, expected_columns: List[str], extracted_data: Dict) -> Dict:
    """Analyze output file to check data consistency."""
    analysis = {
        'field_mapping_success': {},
        'missing_fields': [],
        'empty_fields': [],
        'data_consistency': True
    }
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Analyzing output file: {len(df)} rows, {len(df.columns)} columns")
        
        # Check each expected column
        for col in expected_columns:
            if col in df.columns:
                # Check if data was populated
                non_empty_count = sum(1 for val in df[col] if pd.notna(val) and str(val).strip() and str(val) != '.')
                analysis['field_mapping_success'][col] = non_empty_count
                
                if non_empty_count == 0:
                    analysis['empty_fields'].append(col)
                    logger.warning(f"⚠️ Field {col} is empty in all rows")
                else:
                    logger.info(f"✅ Field {col} populated in {non_empty_count} rows")
            else:
                analysis['missing_fields'].append(col)
                logger.error(f"❌ Expected column {col} not found in output")
        
        # Check if extracted data made it to the output
        field_mappings = {
            'passport_number': ['Passport No', 'PASSPORTNO', 'PassportNum'],
            'emirates_id': ['Emirates Id', 'EMIRATESID', 'EIDNumber'], 
            'unified_no': ['Unified No', 'UIDNO', 'UIDNo'],
            'visa_file_number': ['Visa File Number', 'VISAFILEREF', 'ResidentFileNumber'],
            'nationality': ['Nationality', 'NATIONALITY', 'Country']
        }
        
        for extracted_field, possible_columns in field_mappings.items():
            if extracted_field in extracted_data and extracted_data[extracted_field]:
                found = False
                for col in possible_columns:
                    if col in df.columns:
                        if any(str(val) == str(extracted_data[extracted_field]) for val in df[col]):
                            logger.info(f"✅ {extracted_field} found in column {col}")
                            found = True
                            break
                
                if not found:
                    logger.error(f"❌ {extracted_field} with value '{extracted_data[extracted_field]}' NOT FOUND in output")
                    analysis['data_consistency'] = False
        
    except Exception as e:
        logger.error(f"Error analyzing output file: {str(e)}")
        analysis['error'] = str(e)
    
    return analysis

def generate_consistency_report(results: Dict, extracted_data: Dict):
    """Generate a comprehensive consistency report."""
    
    logger.info("\n" + "="*80)
    logger.info("CONSISTENCY REPORT")
    logger.info("="*80)
    
    successful_templates = [name for name, result in results.items() if result.get('status') == 'success']
    failed_templates = [name for name, result in results.items() if result.get('status') == 'failed']
    
    logger.info(f"✅ Successful Templates: {len(successful_templates)} - {successful_templates}")
    logger.info(f"❌ Failed Templates: {len(failed_templates)} - {failed_templates}")
    
    if successful_templates:
        logger.info("\nSUCCESS ANALYSIS:")
        for template in successful_templates:
            result = results[template]
            analysis = result.get('analysis', {})
            logger.info(f"\n{template.upper()}:")
            logger.info(f"  Rows processed: {result.get('rows_processed', 0)}")
            logger.info(f"  Data consistency: {analysis.get('data_consistency', False)}")
            logger.info(f"  Empty fields: {analysis.get('empty_fields', [])}")
            logger.info(f"  Missing fields: {analysis.get('missing_fields', [])}")
    
    if failed_templates:
        logger.info("\nFAILURE ANALYSIS:")
        for template in failed_templates:
            result = results[template]
            logger.error(f"{template.upper()}: {result.get('error', 'Unknown error')}")
    
    # Save detailed report
    report_file = f"consistency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(report_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'extracted_data_used': extracted_data,
                'results': results,
                'summary': {
                    'successful_templates': successful_templates,
                    'failed_templates': failed_templates,
                    'consistency_achieved': len(failed_templates) == 0
                }
            }, f, indent=2, default=str)
        logger.info(f"\n📄 Detailed report saved: {report_file}")
    except Exception as e:
        logger.error(f"Failed to save report: {e}")

def debug_field_mapping():
    """Debug field mapping logic specifically."""
    logger.info("\n" + "="*80)
    logger.info("FIELD MAPPING DEBUG")
    logger.info("="*80)
    
    # Test data
    test_data = {
        'passport_number': 'AB1234567',
        'emirates_id': '784-1234-1234567-1', 
        'unified_no': '123456789',
        'visa_file_number': '201/2024/123456',
        'nationality': 'Indian'
    }
    
    # Template columns
    template_sets = {
        'nas': ['First Name', 'Middle Name', 'Last Name', 'Emirates Id', 'Unified No', 'Visa File Number', 'Passport No', 'Nationality'],
        'almadallah': ['FIRSTNAME', 'MIDDLENAME', 'LASTNAME', 'EMIRATESID', 'UIDNO', 'VISAFILEREF', 'PASSPORTNO', 'NATIONALITY'],
        'takaful': ['FirstName', 'SecondName', 'LastName', 'EIDNumber', 'UIDNo', 'ResidentFileNumber', 'PassportNum', 'Country']
    }
    
    try:
        from src.services.data_combiner import DataCombiner
        from src.document_processor.excel_processor import EnhancedExcelProcessor
        from src.document_processor.textract_processor import TextractProcessor
        from src.document_processor.gpt_processor import GPTProcessor
        
        # Initialize combiner
        textract = TextractProcessor()
        excel_proc = EnhancedExcelProcessor()
        gpt_proc = GPTProcessor()
        combiner = DataCombiner(textract, excel_proc, gpt_proc)
        
        for template_name, columns in template_sets.items():
            logger.info(f"\nTesting {template_name.upper()} field mapping:")
            
            # Test the mapping function directly
            mapped = combiner._map_to_template(test_data, columns, {})
            
            # Check if critical fields were mapped
            critical_checks = {
                'nas': [('passport_number', 'Passport No'), ('emirates_id', 'Emirates Id'), ('unified_no', 'Unified No')],
                'almadallah': [('passport_number', 'PASSPORTNO'), ('emirates_id', 'EMIRATESID'), ('unified_no', 'UIDNO')],
                'takaful': [('passport_number', 'PassportNum'), ('emirates_id', 'EIDNumber'), ('unified_no', 'UIDNo')]
            }
            
            for source_field, target_field in critical_checks[template_name]:
                if target_field in mapped and mapped[target_field] == test_data[source_field]:
                    logger.info(f"  ✅ {source_field} → {target_field}: {mapped[target_field]}")
                else:
                    logger.error(f"  ❌ {source_field} → {target_field}: Expected '{test_data[source_field]}', got '{mapped.get(target_field, 'MISSING')}'")
            
    except Exception as e:
        logger.error(f"Field mapping debug failed: {e}")

if __name__ == "__main__":
    logger.info("Starting comprehensive debug and test...")
    
    # Run field mapping debug
    debug_field_mapping()
    
    # Run comprehensive template test
    results = comprehensive_template_test()
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("DEBUG SESSION COMPLETE")
    logger.info("="*80)
    
    success_count = sum(1 for r in results.values() if r.get('status') == 'success')
    total_count = len(results)
    
    if success_count == total_count:
        logger.info(f"🎉 ALL {total_count} TEMPLATES PASSED!")
    else:
        logger.warning(f"⚠️ {success_count}/{total_count} templates passed. Issues need fixing.")
    
    logger.info("Check the generated log file and report for detailed analysis.")