#!/usr/bin/env python3
"""
Health check utilities for StreamFlow services.

Ensures services start in the correct order and are ready before dependent services start.
"""

import sys
import time
import redis
from celery import Celery
from logging_config import setup_logging

logger = setup_logging(__name__)


def check_redis_health(host='localhost', port=6379, db=0, timeout=5, max_retries=30):
    """
    Check if Redis is healthy and ready to accept connections.
    
    Args:
        host: Redis host
        port: Redis port
        db: Redis database number
        timeout: Connection timeout in seconds
        max_retries: Maximum number of retry attempts
        
    Returns:
        bool: True if Redis is healthy, False otherwise
    """
    # Create Redis client once, reuse for all retry attempts
    client = redis.Redis(
        host=host,
        port=port,
        db=db,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        decode_responses=True
    )
    
    for attempt in range(1, max_retries + 1):
        try:
            # Test connection
            client.ping()
            logger.info(f"✓ Redis is healthy and ready (attempt {attempt}/{max_retries})")
            return True
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if attempt < max_retries:
                logger.warning(
                    f"⚠ Redis not ready yet (attempt {attempt}/{max_retries}): {e}"
                )
                time.sleep(1)
            else:
                logger.error(f"✗ Redis failed to become ready after {max_retries} attempts")
                return False
        except Exception as e:
            logger.error(f"✗ Unexpected error checking Redis health: {e}")
            return False
    
    return False


def check_celery_health(broker_url, max_retries=30):
    """
    Check if Celery worker is healthy and ready to accept tasks.
    
    Args:
        broker_url: Celery broker URL (Redis URL)
        max_retries: Maximum number of retry attempts
        
    Returns:
        bool: True if Celery is healthy, False otherwise
    """
    # Create Celery app once and reuse for all retry attempts
    app = Celery('health_check', broker=broker_url)
    
    for attempt in range(1, max_retries + 1):
        try:
            # Get active workers
            inspect = app.control.inspect(timeout=2.0)
            active = inspect.active()
            
            # Check if active is not None and has workers (handles both None and empty dict)
            if active:
                logger.info(
                    f"✓ Celery worker is healthy with {len(active)} worker(s) "
                    f"(attempt {attempt}/{max_retries})"
                )
                return True
            else:
                if attempt < max_retries:
                    logger.warning(
                        f"⚠ Celery worker not ready yet (attempt {attempt}/{max_retries})"
                    )
                    time.sleep(1)
                else:
                    logger.error(
                        f"✗ Celery worker failed to become ready after {max_retries} attempts"
                    )
                    return False
                    
        except Exception as e:
            if attempt < max_retries:
                logger.warning(
                    f"⚠ Celery worker not ready yet (attempt {attempt}/{max_retries}): {e}"
                )
                time.sleep(1)
            else:
                logger.error(
                    f"✗ Celery worker failed to become ready after {max_retries} attempts: {e}"
                )
                return False
    
    return False


if __name__ == '__main__':
    """
    Command-line interface for health checks.
    
    Usage:
        python health_check.py redis [host] [port] [db]
        python health_check.py celery [broker_url]
    """
    import os
    
    if len(sys.argv) < 2:
        print("Usage: health_check.py <redis|celery> [options]")
        sys.exit(1)
    
    service = sys.argv[1].lower()
    
    if service == 'redis':
        host = sys.argv[2] if len(sys.argv) > 2 else os.environ.get('REDIS_HOST', 'localhost')
        port = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.environ.get('REDIS_PORT', 6379))
        db = int(sys.argv[4]) if len(sys.argv) > 4 else int(os.environ.get('REDIS_DB', 0))
        
        if check_redis_health(host, port, db):
            sys.exit(0)
        else:
            sys.exit(1)
            
    elif service == 'celery':
        if len(sys.argv) > 2:
            broker_url = sys.argv[2]
        else:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
            redis_port = int(os.environ.get('REDIS_PORT', 6379))
            redis_db = int(os.environ.get('REDIS_DB', 0))
            broker_url = f'redis://{redis_host}:{redis_port}/{redis_db}'
        
        if check_celery_health(broker_url):
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        print(f"Unknown service: {service}")
        print("Usage: health_check.py <redis|celery> [options]")
        sys.exit(1)
