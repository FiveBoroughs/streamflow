#!/usr/bin/env python3
"""
Centralized logging configuration for StreamFlow.

This module provides a unified logging setup that respects the DEBUG_MODE
environment variable and can be imported by all other modules.
"""

import logging
import os
import sys
from typing import Optional


class HTTPLogFilter(logging.Filter):
    """Filter out HTTP-related log messages."""
    
    def filter(self, record):
        # Exclude messages containing HTTP request/response indicators
        message = record.getMessage().lower()
        http_indicators = [
            'http request',
            'http response',
            'status code',
            'get /',
            'post /',
            'put /',
            'delete /',
            'patch /',
            '" with',
            '- - [',  # Common HTTP access log format
            'werkzeug',
        ]
        return not any(indicator in message for indicator in http_indicators)


def setup_logging(module_name: Optional[str] = None) -> logging.Logger:
    """
    Configure logging with DEBUG_MODE support.
    
    Args:
        module_name: Name of the module for the logger. If None, returns root logger.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Get DEBUG_MODE from environment (default: false)
    debug_mode = os.getenv('DEBUG_MODE', 'false').lower() in ('true', '1', 'yes', 'on')
    
    # Set logging level based on DEBUG_MODE
    log_level = logging.DEBUG if debug_mode else logging.INFO
    
    # Configure root logger if not already configured
    if not logging.root.handlers:
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            stream=sys.stdout
        )
        
        # Apply HTTP filter to all handlers
        for handler in logging.root.handlers:
            handler.addFilter(HTTPLogFilter())
    else:
        # Update level if logger already exists
        logging.root.setLevel(log_level)
        for handler in logging.root.handlers:
            handler.setLevel(log_level)
    
    # Get logger for the specific module
    logger = logging.getLogger(module_name) if module_name else logging.root
    logger.setLevel(log_level)
    
    if debug_mode:
        logger.debug(f"Debug mode enabled for {module_name or 'root'}")
    
    return logger


def log_function_call(logger: logging.Logger, func_name: str, **kwargs):
    """
    Log a function call with its parameters (only in debug mode).
    
    Args:
        logger: Logger instance to use
        func_name: Name of the function being called
        **kwargs: Function parameters to log
    """
    if logger.isEnabledFor(logging.DEBUG):
        params = ', '.join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
        logger.debug(f"→ {func_name}({params})")


def log_function_return(logger: logging.Logger, func_name: str, result=None, elapsed_time: Optional[float] = None):
    """
    Log a function return with its result (only in debug mode).
    
    Args:
        logger: Logger instance to use
        func_name: Name of the function returning
        result: Return value to log (optional)
        elapsed_time: Execution time in seconds (optional)
    """
    if logger.isEnabledFor(logging.DEBUG):
        msg = f"← {func_name}"
        if result is not None:
            # Truncate long results
            result_str = str(result)
            if len(result_str) > 100:
                result_str = result_str[:100] + "..."
            msg += f" → {result_str}"
        if elapsed_time is not None:
            msg += f" ({elapsed_time:.3f}s)"
        logger.debug(msg)


def log_exception(logger: logging.Logger, exc: Exception, context: str = ""):
    """
    Log an exception with context and stack trace in debug mode.
    
    Args:
        logger: Logger instance to use
        exc: Exception to log
        context: Additional context about where/why the exception occurred
    """
    msg = f"Exception in {context}: {type(exc).__name__}: {exc}" if context else f"{type(exc).__name__}: {exc}"
    
    if logger.isEnabledFor(logging.DEBUG):
        # In debug mode, log with full stack trace
        logger.debug(msg, exc_info=True)
    else:
        # In normal mode, just log the error message
        logger.error(msg)


def log_api_request(logger: logging.Logger, method: str, url: str, **kwargs):
    """
    Log an API request (only in debug mode).
    
    Args:
        logger: Logger instance to use
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        **kwargs: Additional request details (headers, params, data, etc.)
    """
    if logger.isEnabledFor(logging.DEBUG):
        # Sanitize sensitive data
        sanitized_kwargs = {}
        for key, value in kwargs.items():
            if key in ('headers', 'auth'):
                sanitized_kwargs[key] = '<redacted>'
            elif key == 'data' or key == 'json':
                # Show structure but not full content
                if isinstance(value, dict):
                    sanitized_kwargs[key] = f"<dict with {len(value)} keys>"
                elif isinstance(value, (list, tuple)):
                    sanitized_kwargs[key] = f"<{type(value).__name__} with {len(value)} items>"
                else:
                    sanitized_kwargs[key] = f"<{type(value).__name__}>"
            else:
                sanitized_kwargs[key] = value
        
        extras = ', '.join(f"{k}={v}" for k, v in sanitized_kwargs.items())
        logger.debug(f"→ API {method} {url} {extras}")


def log_api_response(logger: logging.Logger, method: str, url: str, status_code: int, elapsed_time: Optional[float] = None):
    """
    Log an API response (only in debug mode).
    
    Args:
        logger: Logger instance to use
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        status_code: HTTP status code
        elapsed_time: Request duration in seconds (optional)
    """
    if logger.isEnabledFor(logging.DEBUG):
        msg = f"← API {method} {url} → {status_code}"
        if elapsed_time is not None:
            msg += f" ({elapsed_time:.3f}s)"
        logger.debug(msg)


def log_state_change(logger: logging.Logger, entity: str, old_state, new_state, reason: str = ""):
    """
    Log a state change (only in debug mode).
    
    Args:
        logger: Logger instance to use
        entity: What is changing state (e.g., "channel_123", "stream_checker")
        old_state: Previous state
        new_state: New state
        reason: Why the state changed (optional)
    """
    if logger.isEnabledFor(logging.DEBUG):
        msg = f"State change: {entity} {old_state} → {new_state}"
        if reason:
            msg += f" ({reason})"
        logger.debug(msg)
