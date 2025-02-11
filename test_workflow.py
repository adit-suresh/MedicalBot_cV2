import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from src.services.workflow_orchestrator import WorkflowOrchestrator
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_complete_workflow(
    test_docs_dir: str,
    template_path: str,
    output_dir: str
):
    """
    Test complete workflow with real documents.
    
    Args:
        test_docs_dir: Directory containing test documents
        template_path: Path to Excel template
        output_dir: Directory for output files
    """
    try:
        logger.info("Starting workflow test...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        process_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Initialize orchestrator
        orchestrator = WorkflowOrchestrator()
        
        # Collect test documents
        documents = {}
        for filename in os.listdir(test_docs_dir):
            file_path = os.path.join(test_docs_dir, filename)
            if filename.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png')):
                if 'passport' in filename.lower():
                    documents['passport'] = file_path
                elif 'emirates' in filename.lower() or 'eid' in filename.lower():
                    documents['emirates_id'] = file_path
                elif 'visa' in filename.lower():
                    documents['visa'] = file_path

        logger.info(f"Found documents: {list(documents.keys())}")

        # Verify documents
        missing_docs = orchestrator.data_integrator.get_missing_documents(documents)
        if missing_docs:
            logger.warning(f"Missing documents: {missing_docs}")
            return

        # Process documents
        logger.info("Processing documents...")
        result = orchestrator.retry_failed_process(
            process_id,
            documents,
            template_path,
            output_dir
        )

        # Check results
        if result['status'] == 'success':
            logger.info("Workflow completed successfully!")
            logger.info(f"Output file: {result['output_file']}")
            
            # Verify output
            df = pd.read_excel(result['output_file'])
            logger.info("\nExtracted Data:")
            for col in df.columns:
                logger.info(f"{col}: {df[col].iloc[0]}")
                
        elif result['status'] == 'completed_with_errors':
            logger.warning("Workflow completed with errors:")
            for error in result.get('errors', []):
                logger.warning(f"- {error}")
                
        else:
            logger.error(f"Workflow failed: {result.get('error')}")

    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Check AWS credentials
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        exit(1)
    
    # Test paths
    TEST_DOCS_DIR = "test_files"  # Directory with test documents
    TEMPLATE_PATH = "template.xlsx"  # Path to Excel template
    OUTPUT_DIR = "test_output"  # Directory for output files
    
    if not os.path.exists(TEST_DOCS_DIR):
        logger.error(f"Test documents directory not found: {TEST_DOCS_DIR}")
        exit(1)
    
    if not os.path.exists(TEMPLATE_PATH):
        logger.error(f"Template file not found: {TEMPLATE_PATH}")
        exit(1)
    
    # Run test
    test_complete_workflow(TEST_DOCS_DIR, TEMPLATE_PATH, OUTPUT_DIR)