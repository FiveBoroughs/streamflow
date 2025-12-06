#!/usr/bin/env python3
"""
Celery application configuration for StreamFlow.

Configures Celery for distributed stream checking with concurrency limits
and proper result backend for task tracking.
"""

import os
from celery import Celery
from logging_config import setup_logging

logger = setup_logging(__name__)

# Get Redis connection details from environment
# Default to localhost for All-In-One deployment
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

# Build Redis URL
REDIS_URL = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'

# Create Celery application
celery_app = Celery(
    'streamflow',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['celery_tasks']
)

# Configure Celery
celery_app.conf.update(
    # Task execution settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task result settings
    result_expires=3600,  # Results expire after 1 hour
    
    # Worker settings
    worker_prefetch_multiplier=1,  # Disable prefetching for better concurrency control
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks
    
    # Task routing
    task_default_queue='stream_checks',
    task_routes={
        'celery_tasks.check_stream_task': {'queue': 'stream_checks'},
    },
    
    # Task time limits
    task_soft_time_limit=300,  # 5 minutes soft limit
    task_time_limit=360,  # 6 minutes hard limit
    
    # Acknowledgement settings
    task_acks_late=True,  # Acknowledge after task completion
    task_reject_on_worker_lost=True,  # Reject task if worker dies
)

logger.info(f"Celery configured with broker: {REDIS_URL}")
