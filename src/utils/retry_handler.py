import time
from typing import Callable, Any, Dict, Optional
import logging
from functools import wraps
import random

logger = logging.getLogger(__name__)

class RetryConfig:
    def __init__(self,
                 max_attempts: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 exponential_base: float = 2.0,
                 jitter: bool = True):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

class RetryHandler:
    def __init__(self, error_handler):
        self.error_handler = error_handler

    def with_retry(self, 
                  retry_config: Optional[RetryConfig] = None,
                  retryable_errors: tuple = (Exception,),
                  process_id: Optional[str] = None,
                  stage: Optional[str] = None):
        """
        Decorator for adding retry logic to functions.
        
        Usage:
            @retry_handler.with_retry(
                retry_config=RetryConfig(max_attempts=3),
                retryable_errors=(NetworkError, TimeoutError)
            )
            def some_function():
                # function code
        """
        config = retry_config or RetryConfig()

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                attempt = 1
                last_exception = None
                
                while attempt <= config.max_attempts:
                    try:
                        return func(*args, **kwargs)
                    
                    except retryable_errors as e:
                        last_exception = e
                        
                        # Calculate delay with exponential backoff
                        delay = min(
                            config.base_delay * (config.exponential_base ** (attempt - 1)),
                            config.max_delay
                        )
                        
                        # Add jitter if enabled
                        if config.jitter:
                            delay = delay * (0.5 + random.random())
                        
                        # Log retry attempt
                        logger.warning(
                            f"Attempt {attempt}/{config.max_attempts} failed for "
                            f"{func.__name__}. Retrying in {delay:.2f} seconds. "
                            f"Error: {str(e)}"
                        )
                        
                        # Handle error with error handler
                        if process_id and stage:
                            from src.utils.error_handler import ProcessError, ErrorCategory, ErrorSeverity
                            error = ProcessError(
                                error=e,
                                process_id=process_id,
                                stage=stage,
                                category=ErrorCategory.PROCESSING,
                                severity=ErrorSeverity.MEDIUM,
                                context={
                                    'attempt': attempt,
                                    'max_attempts': config.max_attempts,
                                    'function': func.__name__
                                }
                            )
                            self.error_handler.handle_error(error)
                        
                        if attempt < config.max_attempts:
                            time.sleep(delay)
                        
                        attempt += 1
                
                # If we get here, all retries failed
                logger.error(
                    f"All {config.max_attempts} attempts failed for {func.__name__}"
                )
                raise last_exception
            
            return wrapper
        return decorator

    def retry_on_condition(self, 
                         condition: Callable[..., bool],
                         retry_config: Optional[RetryConfig] = None,
                         process_id: Optional[str] = None,
                         stage: Optional[str] = None):
        """
        Decorator for retrying when a condition is met.
        
        Usage:
            def check_result(result):
                return result.get('status') != 'success'
                
            @retry_handler.retry_on_condition(
                condition=check_result,
                retry_config=RetryConfig(max_attempts=3)
            )
            def some_function():
                # function code
        """
        config = retry_config or RetryConfig()

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                attempt = 1
                while attempt <= config.max_attempts:
                    result = func(*args, **kwargs)
                    
                    if not condition(result):
                        return result
                    
                    # Calculate delay
                    delay = min(
                        config.base_delay * (config.exponential_base ** (attempt - 1)),
                        config.max_delay
                    )
                    if config.jitter:
                        delay = delay * (0.5 + random.random())
                    
                    # Log retry attempt
                    logger.warning(
                        f"Condition not met on attempt {attempt}/{config.max_attempts} "
                        f"for {func.__name__}. Retrying in {delay:.2f} seconds."
                    )
                    
                    if attempt < config.max_attempts:
                        time.sleep(delay)
                    
                    attempt += 1
                
                # If we get here, all retries failed to meet condition
                raise ValueError(
                    f"Function {func.__name__} failed to meet condition after "
                    f"{config.max_attempts} attempts"
                )
            
            return wrapper
        return decorator