#!/usr/bin/env python3
"""
Celery tasks for concurrent stream checking in StreamFlow.

Provides distributed task execution for stream quality analysis with
concurrency control based on M3U account limits and global settings.
"""

from celery import Task
from celery_app import celery_app
from stream_check_utils import analyze_stream
from logging_config import setup_logging
from typing import Dict, Any

logger = setup_logging(__name__)


class StreamCheckTask(Task):
    """
    Custom task class for stream checking with automatic retry logic.
    """
    autoretry_for = (Exception,)
    retry_kwargs = {'max_retries': 2, 'countdown': 5}
    retry_backoff = True


@celery_app.task(base=StreamCheckTask, bind=True, name='celery_tasks.check_stream_task')
def check_stream_task(
    self,
    stream_id: int,
    stream_url: str,
    stream_name: str,
    channel_id: int,
    channel_name: str,
    m3u_account_id: int = None,
    ffmpeg_duration: int = 30,
    timeout: int = 30,
    retries: int = 1,
    retry_delay: int = 10,
    user_agent: str = 'VLC/3.0.14'
) -> Dict[str, Any]:
    """
    Celery task to analyze a single stream's quality.
    
    This task runs concurrently and is subject to:
    - M3U account-specific concurrent stream limits
    - Global concurrent stream limit
    
    Args:
        self: Task instance (injected by bind=True)
        stream_id: The stream ID
        stream_url: URL of the stream to check
        stream_name: Name of the stream
        channel_id: Channel ID this stream belongs to
        channel_name: Channel name for logging
        m3u_account_id: M3U account ID (for concurrency limiting)
        ffmpeg_duration: Duration to analyze stream in seconds
        timeout: Operation timeout in seconds
        retries: Number of retry attempts
        retry_delay: Delay between retries in seconds
        user_agent: User agent for HTTP requests
        
    Returns:
        Dictionary containing analysis results with score
    """
    try:
        logger.info(
            f"Task {self.request.id}: Checking stream {stream_id} ({stream_name}) "
            f"from channel {channel_id} ({channel_name})"
        )
        
        # Analyze the stream
        analyzed = analyze_stream(
            stream_url=stream_url,
            stream_id=stream_id,
            stream_name=stream_name,
            ffmpeg_duration=ffmpeg_duration,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            user_agent=user_agent
        )
        
        # Add channel context
        analyzed['channel_id'] = channel_id
        analyzed['channel_name'] = channel_name
        analyzed['m3u_account_id'] = m3u_account_id
        analyzed['task_id'] = self.request.id
        
        logger.info(
            f"Task {self.request.id}: Stream {stream_id} analysis complete - "
            f"resolution: {analyzed.get('resolution')}, "
            f"bitrate: {analyzed.get('bitrate_kbps')} kbps"
        )
        
        return analyzed
        
    except Exception as e:
        logger.error(
            f"Task {self.request.id}: Failed to check stream {stream_id} "
            f"({stream_name}): {e}",
            exc_info=True
        )
        # Re-raise to trigger retry logic
        raise


@celery_app.task(name='celery_tasks.health_check_task')
def health_check_task() -> Dict[str, str]:
    """
    Simple health check task to verify Celery is working.
    
    Returns:
        Dictionary with status message
    """
    return {'status': 'ok', 'message': 'Celery worker is healthy'}
