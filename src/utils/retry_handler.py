import time
import random
import logging
import functools
from typing import Callable, Any, Dict, Optional, Tuple, Union, Type, List
import threading
from datetime import datetime, timedelta
import requests

from src.utils.error_handler import ProcessError, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class RetryConfig:
    """Enhanced configuration for retry behavior."""
    
    def __init__(self,
                 max_attempts: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 exponential_base: float = 2.0,
                 jitter: bool = True,
                 timeout: Optional[float] = None,
                 retry_on_exceptions: Tuple[Type[Exception], ...] = (Exception,),
                 retry_if_result_condition: Optional[Callable[[Any], bool]] = None):
        """
        Initialize retry configuration.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            exponential_base: Base for exponential backoff calculation
            jitter: Whether to add randomness to delay times
            timeout: Overall timeout for all attempts in seconds
            retry_on_exceptions: Exception types that trigger retry
            retry_if_result_condition: Function to evaluate result and decide if retry needed
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.timeout = timeout
        self.retry_on_exceptions = retry_on_exceptions
        self.retry_if_result_condition = retry_if_result_condition
        
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay with optional jitter."""
        delay = min(
            self.base_delay * (self.exponential_base ** (attempt - 1)),
            self.max_delay
        )
        
        if self.jitter:
            # Add jitter between 0.5x and 1.5x
            jitter_factor = 0.5 + random.random()
            delay = delay * jitter_factor
            
        return delay
        
    def should_retry_result(self, result: Any) -> bool:
        """Determine if result requires retry."""
        if self.retry_if_result_condition is None:
            return False
        return self.retry_if_result_condition(result)
        
    @staticmethod
    def for_network_operations() -> 'RetryConfig':
        """Factory method for network operation config."""
        return RetryConfig(
            max_attempts=5,
            base_delay=1.0,
            max_delay=30.0,
            exponential_base=2.0,
            jitter=True,
            retry_on_exceptions=(
                requests.exceptions.RequestException,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                ConnectionError,
                TimeoutError,
            )
        )
        
    @staticmethod
    def for_database_operations() -> 'RetryConfig':
        """Factory method for database operation config."""
        import sqlite3
        return RetryConfig(
            max_attempts=3,
            base_delay=0.5,
            max_delay=5.0,
            exponential_base=1.5,
            jitter=False,
            retry_on_exceptions=(
                sqlite3.OperationalError,
                sqlite3.DatabaseError
            )
        )


class RetryState:
    """Track retry state between attempts with thread safety."""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.attempts = 0
        self.start_time = datetime.now()
        self.last_exception = None
        self.last_result = None
        self.success = False
        self.lock = threading.RLock()
        
    def increment_attempt(self) -> int:
        """Increment attempt counter and return new value."""
        with self.lock:
            self.attempts += 1
            return self.attempts
            
    def record_exception(self, exception: Exception) -> None:
        """Record exception from most recent attempt."""
        with self.lock:
            self.last_exception = exception
            
    def record_result(self, result: Any) -> None:
        """Record result from most recent attempt."""
        with self.lock:
            self.last_result = result
            
    def set_success(self) -> None:
        """Mark retry as successful."""
        with self.lock:
            self.success = True
            
    def is_timed_out(self) -> bool:
        """Check if overall retry timeout has been reached."""
        if self.config.timeout is None:
            return False
            
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return elapsed >= self.config.timeout
        
    def should_continue(self) -> bool:
        """Determine if retries should continue."""
        with self.lock:
            return (
                self.attempts < self.config.max_attempts and
                not self.success and
                not self.is_timed_out()
            )


class RetryHandler:
    """Enhanced retry handler with comprehensive retry strategies."""
    
    def __init__(self, error_handler=None):
        """
        Initialize retry handler.
        
        Args:
            error_handler: Optional error handler for tracking retry failures
        """
        self.error_handler = error_handler
        
    def with_retry(self, 
                  retry_config: Optional[RetryConfig] = None,
                  process_id: Optional[str] = None,
                  stage: Optional[str] = None):
        """
        Decorator for adding comprehensive retry logic to functions.
        
        Args:
            retry_config: Configuration for retry behavior
            process_id: Process identifier for error tracking
            stage: Process stage for error context
            
        Returns:
            Decorated function with retry logic
        """
        config = retry_config or RetryConfig()

        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                # Try to get process_id from args/kwargs if not provided
                current_process_id = process_id
                if current_process_id is None:
                    for arg in args:
                        if isinstance(arg, str) and arg.startswith("PROC_"):
                            current_process_id = arg
                            break
                    
                    if current_process_id is None and "process_id" in kwargs:
                        current_process_id = kwargs["process_id"]
                
                current_stage = stage or func.__qualname__
                retry_state = RetryState(config)
                
                while retry_state.should_continue():
                    attempt = retry_state.increment_attempt()
                    try:
                        # Execute function
                        start_time = time.time()
                        result = func(*args, **kwargs)
                        execution_time = time.time() - start_time
                        
                        # Log performance if execution was slow
                        if execution_time > 1.0:
                            logger.info(
                                f"Function {func.__name__} took {execution_time:.2f}s on attempt {attempt}"
                            )
                        
                        # Check result condition
                        retry_state.record_result(result)
                        if config.should_retry_result(result):
                            logger.warning(
                                f"Retry condition met for {func.__name__} result on attempt {attempt}"
                            )
                            self._wait_before_retry(config, attempt)
                            continue
                            
                        # Success - return result
                        retry_state.set_success()
                        return result
                    
                    except config.retry_on_exceptions as e:
                        retry_state.record_exception(e)
                        
                        # Calculate delay
                        delay = config.calculate_delay(attempt)
                        
                        # Log retry attempt
                        logger.warning(
                            f"Attempt {attempt}/{config.max_attempts} failed for "
                            f"{func.__name__}: {str(e)}. "
                            f"Retrying in {delay:.2f}s"
                        )
                        
                        # Handle error with error handler
                        if self.error_handler and current_process_id:
                            error = ProcessError(
                                error=e,
                                process_id=current_process_id,
                                stage=current_stage,
                                category=ErrorCategory.PROCESSING,
                                severity=ErrorSeverity.MEDIUM,
                                context={
                                    'attempt': attempt,
                                    'max_attempts': config.max_attempts,
                                    'function': func.__name__,
                                    'retry_delay': delay
                                }
                            )
                            self.error_handler.handle_error(error)
                        
                        if retry_state.should_continue():
                            self._wait_before_retry(config, attempt)
                        else:
                            break
                
                # If we get here, all retries failed or timed out
                if retry_state.last_exception:
                    logger.error(
                        f"All {config.max_attempts} attempts failed for {func.__name__}"
                    )
                    
                    # Final failure notification
                    if self.error_handler and current_process_id:
                        error = ProcessError(
                            error=retry_state.last_exception,
                            process_id=current_process_id,
                            stage=current_stage,
                            category=ErrorCategory.PROCESSING,
                            severity=ErrorSeverity.HIGH,
                            context={
                                'attempts': retry_state.attempts,
                                'max_attempts': config.max_attempts,
                                'function': func.__name__,
                                'timed_out': retry_state.is_timed_out(),
                                'total_time': (datetime.now() - retry_state.start_time).total_seconds()
                            }
                        )
                        self.error_handler.handle_error(error)
                        
                    raise retry_state.last_exception
                
                # Final result didn't meet success condition
                logger.error(
                    f"Function {func.__name__} result still doesn't meet success condition after "
                    f"{retry_state.attempts} attempts"
                )
                raise ValueError(
                    f"Function {func.__name__} failed to meet condition after "
                    f"{config.max_attempts} attempts"
                )
                
            return wrapper
        return decorator

    def retry_on_condition(self, 
                         condition: Callable[[Any], bool],
                         retry_config: Optional[RetryConfig] = None,
                         process_id: Optional[str] = None,
                         stage: Optional[str] = None):
        """
        Decorator for retrying when a specific condition is met.
        
        Args:
            condition: Function that evaluates result and returns True if retry needed
            retry_config: Configuration for retry behavior
            process_id: Process identifier for error tracking
            stage: Process stage for error context
            
        Returns:
            Decorated function with condition-based retry
        """
        config = retry_config or RetryConfig()
        # Apply condition to config
        config.retry_if_result_condition = condition
        
        # Use existing retry mechanism
        return self.with_retry(
            retry_config=config,
            process_id=process_id,
            stage=stage
        )
        
    def _wait_before_retry(self, config: RetryConfig, attempt: int) -> None:
        """Wait with appropriate backoff before next retry."""
        delay = config.calculate_delay(attempt)
        time.sleep(delay)

    def retry_with_timeout(self, timeout: float,
                         retry_interval: float = 1.0,
                         process_id: Optional[str] = None):
        """
        Decorator for retrying until a timeout is reached.
        
        Args:
            timeout: Maximum time in seconds to keep retrying
            retry_interval: Time between retries in seconds
            process_id: Process identifier for error tracking
            
        Returns:
            Decorated function with timeout-based retry
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                start_time = time.time()
                last_exception = None
                
                while (time.time() - start_time) < timeout:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        logger.debug(f"Retrying {func.__name__} after error: {str(e)}")
                        time.sleep(retry_interval)
                
                # If we get here, we timed out
                message = f"Operation timed out after {timeout} seconds"
                if last_exception:
                    logger.error(f"{message}: {str(last_exception)}")
                    raise type(last_exception)(f"{message}: {str(last_exception)}")
                else:
                    logger.error(message)
                    raise TimeoutError(message)
                    
            return wrapper
        return decorator