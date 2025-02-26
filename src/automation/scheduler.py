import time
import schedule
import logging
import os
import sys
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from test_complete_workflow import WorkflowTester

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(project_root, 'logs', f'scheduler_{datetime.now().strftime("%Y%m%d")}.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_workflow():
    """Run the workflow and handle any exceptions."""
    try:
        logger.info("Starting scheduled workflow run")
        tester = WorkflowTester()
        result = tester.run_complete_workflow()
        
        if result['status'] == 'success':
            logger.info(f"Workflow completed successfully. Processed {result.get('emails_processed', 0)} emails.")
        else:
            logger.error(f"Workflow failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"Error running workflow: {str(e)}", exc_info=True)

def main():
    """Run the scheduler."""
    # Ensure logs directory exists
    os.makedirs(os.path.join(project_root, 'logs'), exist_ok=True)
    
    logger.info("Starting Medical Bot Scheduler")
    
    # Schedule the job
    schedule.every(15).minutes.do(run_workflow)
    
    # Run once immediately at startup
    run_workflow()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()