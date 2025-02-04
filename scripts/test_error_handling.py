import os
import sys
import time
import random
from datetime import datetime

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.utils.error_handler import ErrorHandler, ProcessError, ErrorCategory, ErrorSeverity
from src.utils.retry_handler import RetryHandler, RetryConfig
from src.utils.slack_notifier import SlackNotifier

class TestError(Exception):
    pass

class NetworkError(Exception):
    pass

def test_error_handling():
    """Test error handling and retry mechanisms."""
    # Initialize components
    slack = SlackNotifier()
    error_handler = ErrorHandler(slack_notifier=slack)
    retry_handler = RetryHandler(error_handler)
    
    print("\nTesting Error Handling System...")
    test_process_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Test 1: Basic error handling
    print("\n1. Testing basic error handling...")
    try:
        raise TestError("Test error message")
    except TestError as e:
        error = ProcessError(
            error=e,
            process_id=test_process_id,
            stage="testing",
            category=ErrorCategory.PROCESSING,
            severity=ErrorSeverity.MEDIUM,
            context={"test": "basic_error"}
        )
        handled = error_handler.handle_error(error)
        print(f"Error handled: {handled}")
    
    # Test 2: Retry mechanism with network error
    print("\n2. Testing retry mechanism...")
    
    @retry_handler.with_retry(
        retry_config=RetryConfig(
            max_attempts=3,
            base_delay=1.0
        ),
        retryable_errors=(NetworkError,),
        process_id=test_process_id,
        stage="network_test"
    )
    def test_network_operation():
        if random.random() < 0.8:  # 80% chance of failure
            raise NetworkError("Network timeout")
        return "Success"
    
    try:
        result = test_network_operation()
        print(f"Network operation result: {result}")
    except NetworkError as e:
        print(f"Network operation failed after all retries: {str(e)}")
    
    # Test 3: Condition-based retry
    print("\n3. Testing condition-based retry...")
    
    def check_result(result):
        return result.get('status') != 'success'
    
    @retry_handler.retry_on_condition(
        condition=check_result,
        retry_config=RetryConfig(max_attempts=3),
        process_id=test_process_id,
        stage="condition_test"
    )
    def test_condition_operation():
        if random.random() < 0.8:  # 80% chance of 'failure' status
            return {'status': 'pending', 'data': None}
        return {'status': 'success', 'data': 'test'}
    
    try:
        result = test_condition_operation()
        print(f"Condition operation result: {result}")
    except ValueError as e:
        print(f"Condition operation failed: {str(e)}")
    
    # Test 4: Critical error handling
    print("\n4. Testing critical error handling...")
    try:
        raise TestError("Critical system error")
    except TestError as e:
        error = ProcessError(
            error=e,
            process_id=test_process_id,
            stage="testing",
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            context={"test": "critical_error"}
        )
        handled = error_handler.handle_error(error)
        print(f"Critical error handled: {handled}")
    
    # Test 5: Error statistics
    print("\n5. Testing error statistics...")
    stats = error_handler.get_error_stats()
    print("\nError Statistics:")
    print(f"Total errors: {stats['total_errors']}")
    print("\nCounts by category:")
    for category, count in stats['counts_by_category'].items():
        print(f"  {category}: {count}")
    print("\nRecent errors:")
    for error in stats['recent_errors']:
        print(f"  {error['timestamp']}: {error['error_message']}")

if __name__ == "__main__":
    test_error_handling()