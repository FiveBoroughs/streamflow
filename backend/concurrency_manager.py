#!/usr/bin/env python3
"""
Concurrency manager for stream checking tasks.

Manages concurrent stream checking limits at both the M3U account level
and global level using Redis for distributed state tracking.
"""

import json
import time
from typing import Optional, Dict, List
import redis
from redis import Redis
from logging_config import setup_logging
import os

logger = setup_logging(__name__)


class ConcurrencyManager:
    """
    Manages concurrency limits for stream checking tasks.
    
    Uses Redis to track:
    - Per-M3U account concurrent task counts
    - Global concurrent task count
    - Task to account mappings
    """
    
    def __init__(self, redis_client: Optional[Redis] = None):
        """
        Initialize the concurrency manager.
        
        Args:
            redis_client: Optional Redis client instance. If not provided,
                         creates a new connection.
        """
        if redis_client is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
            redis_port = int(os.environ.get('REDIS_PORT', 6379))
            redis_db = int(os.environ.get('REDIS_DB', 0))
            self.redis = Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
        else:
            self.redis = redis_client
        
        # Redis key prefixes
        self.ACCOUNT_PREFIX = 'streamflow:concurrency:account:'
        self.GLOBAL_KEY = 'streamflow:concurrency:global'
        self.TASK_MAP_PREFIX = 'streamflow:concurrency:task:'
        
        logger.info("Concurrency manager initialized")
    
    def can_start_task_and_register(
        self,
        task_id: str,
        m3u_account_id: Optional[int],
        stream_id: int,
        account_limit: int,
        global_limit: int,
        ttl: int = 3600
    ) -> bool:
        """
        Atomically check if a task can start AND register it if allowed.
        
        This combines the check and registration in a single atomic operation
        to prevent race conditions.
        
        Args:
            task_id: Celery task ID
            m3u_account_id: M3U account ID (None for streams without account)
            stream_id: Stream ID being checked
            account_limit: Maximum concurrent streams for this account
                          (0 = unlimited, N > 0 = limit to N streams)
            global_limit: Maximum global concurrent streams
                         (0 = unlimited, N > 0 = limit to N streams)
            ttl: Time-to-live for task mapping in seconds
            
        Returns:
            True if task was registered, False if limits prevent starting
        """
        try:
            # Use Redis pipeline with WATCH for optimistic locking
            with self.redis.pipeline() as pipe:
                while True:
                    try:
                        # Watch keys for changes
                        pipe.watch(self.GLOBAL_KEY)
                        if m3u_account_id is not None:
                            account_key = f"{self.ACCOUNT_PREFIX}{m3u_account_id}"
                            pipe.watch(account_key)
                        
                        # Check global limit (0 = unlimited, skip check)
                        if global_limit > 0:
                            global_count = int(pipe.get(self.GLOBAL_KEY) or 0)
                            if global_count >= global_limit:
                                pipe.unwatch()
                                return False
                        
                        # Check account limit (0 = unlimited, skip check)
                        if m3u_account_id is not None and account_limit > 0:
                            account_key = f"{self.ACCOUNT_PREFIX}{m3u_account_id}"
                            account_count = int(pipe.get(account_key) or 0)
                            if account_count >= account_limit:
                                pipe.unwatch()
                                return False
                        
                        # Begin transaction (MULTI)
                        pipe.multi()
                        
                        # Increment counters
                        pipe.incr(self.GLOBAL_KEY)
                        if m3u_account_id is not None:
                            pipe.incr(account_key)
                        
                        # Store task mapping
                        task_map_key = f"{self.TASK_MAP_PREFIX}{task_id}"
                        task_data = {
                            'm3u_account_id': m3u_account_id,
                            'stream_id': stream_id,
                            'start_time': time.time()
                        }
                        pipe.setex(task_map_key, ttl, json.dumps(task_data))
                        
                        # Execute transaction
                        pipe.execute()
                        
                        logger.debug(
                            f"Registered task {task_id} for stream {stream_id} "
                            f"(account: {m3u_account_id})"
                        )
                        return True
                        
                    except redis.WatchError:
                        # Key was modified by another client, retry
                        logger.debug(f"WatchError for task {task_id}, retrying...")
                        continue
                        
        except Exception as e:
            logger.error(f"Error in atomic can_start_and_register: {e}")
            return False
    
    def can_start_task(
        self,
        m3u_account_id: Optional[int],
        account_limit: int,
        global_limit: int
    ) -> bool:
        """
        Check if a task can start given current concurrency limits.
        
        Args:
            m3u_account_id: M3U account ID (None for streams without account)
            account_limit: Maximum concurrent streams for this account
                          (0 = unlimited, N > 0 = limit to N streams)
            global_limit: Maximum global concurrent streams
                         (0 = unlimited, N > 0 = limit to N streams)
            
        Returns:
            True if task can start, False otherwise
        """
        try:
            # Check global limit (0 = unlimited, skip check)
            if global_limit > 0:
                global_count = int(self.redis.get(self.GLOBAL_KEY) or 0)
                if global_count >= global_limit:
                    logger.debug(
                        f"Global limit reached: {global_count}/{global_limit}"
                    )
                    return False
            
            # Check account limit (0 = unlimited, skip check)
            if m3u_account_id is not None and account_limit > 0:
                account_key = f"{self.ACCOUNT_PREFIX}{m3u_account_id}"
                account_count = int(self.redis.get(account_key) or 0)
                if account_count >= account_limit:
                    logger.debug(
                        f"Account {m3u_account_id} limit reached: "
                        f"{account_count}/{account_limit}"
                    )
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking concurrency limits: {e}")
            # On error, allow task to proceed
            return True
    
    def register_task_start(
        self,
        task_id: str,
        m3u_account_id: Optional[int],
        stream_id: int,
        ttl: int = 3600
    ) -> bool:
        """
        Register that a task has started.
        
        Increments counters and stores task mapping.
        
        Args:
            task_id: Celery task ID
            m3u_account_id: M3U account ID (None for streams without account)
            stream_id: Stream ID being checked
            ttl: Time-to-live for task mapping in seconds (default 1 hour)
            
        Returns:
            True if registration succeeded, False otherwise
        """
        try:
            pipe = self.redis.pipeline()
            
            # Increment global counter
            pipe.incr(self.GLOBAL_KEY)
            
            # Increment account counter if account specified
            if m3u_account_id is not None:
                account_key = f"{self.ACCOUNT_PREFIX}{m3u_account_id}"
                pipe.incr(account_key)
            
            # Store task mapping for cleanup
            task_map_key = f"{self.TASK_MAP_PREFIX}{task_id}"
            task_data = {
                'm3u_account_id': m3u_account_id,
                'stream_id': stream_id,
                'start_time': time.time()
            }
            pipe.setex(task_map_key, ttl, json.dumps(task_data))
            
            pipe.execute()
            
            logger.debug(
                f"Registered task {task_id} for stream {stream_id} "
                f"(account: {m3u_account_id})"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error registering task start: {e}")
            return False
    
    def register_task_end(self, task_id: str) -> bool:
        """
        Register that a task has ended.
        
        Decrements counters and removes task mapping.
        
        Args:
            task_id: Celery task ID
            
        Returns:
            True if registration succeeded, False otherwise
        """
        try:
            # Get task mapping to know which counters to decrement
            task_map_key = f"{self.TASK_MAP_PREFIX}{task_id}"
            task_data_str = self.redis.get(task_map_key)
            
            if not task_data_str:
                logger.warning(
                    f"No task mapping found for task {task_id}, "
                    "counters may be inaccurate"
                )
                return False
            
            task_data = json.loads(task_data_str)
            m3u_account_id = task_data.get('m3u_account_id')
            
            pipe = self.redis.pipeline()
            
            # Decrement global counter (ensure it doesn't go negative)
            current_global = int(self.redis.get(self.GLOBAL_KEY) or 0)
            if current_global > 0:
                pipe.decr(self.GLOBAL_KEY)
            else:
                pipe.set(self.GLOBAL_KEY, 0)
            
            # Decrement account counter if account was specified
            if m3u_account_id is not None:
                account_key = f"{self.ACCOUNT_PREFIX}{m3u_account_id}"
                current_account = int(self.redis.get(account_key) or 0)
                if current_account > 0:
                    pipe.decr(account_key)
                else:
                    pipe.set(account_key, 0)
            
            # Remove task mapping
            pipe.delete(task_map_key)
            
            pipe.execute()
            
            logger.debug(
                f"Unregistered task {task_id} "
                f"(account: {m3u_account_id})"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error registering task end: {e}")
            return False
    
    def get_current_counts(self) -> Dict[str, int]:
        """
        Get current concurrency counts.
        
        Returns:
            Dictionary with 'global' count and per-account counts
        """
        try:
            # Get global count
            global_count = int(self.redis.get(self.GLOBAL_KEY) or 0)
            
            # Get all account keys
            account_keys = self.redis.keys(f"{self.ACCOUNT_PREFIX}*")
            account_counts = {}
            
            for key in account_keys:
                account_id = key.replace(self.ACCOUNT_PREFIX, '')
                count = int(self.redis.get(key) or 0)
                account_counts[account_id] = count
            
            return {
                'global': global_count,
                'accounts': account_counts
            }
            
        except Exception as e:
            logger.error(f"Error getting current counts: {e}")
            return {'global': 0, 'accounts': {}}
    
    def reset_counts(self) -> bool:
        """
        Reset all concurrency counters.
        
        Useful for recovering from inconsistent state.
        
        Returns:
            True if reset succeeded, False otherwise
        """
        try:
            # Delete global counter
            self.redis.delete(self.GLOBAL_KEY)
            
            # Delete all account counters
            account_keys = self.redis.keys(f"{self.ACCOUNT_PREFIX}*")
            if account_keys:
                self.redis.delete(*account_keys)
            
            # Delete all task mappings
            task_keys = self.redis.keys(f"{self.TASK_MAP_PREFIX}*")
            if task_keys:
                self.redis.delete(*task_keys)
            
            logger.info("Concurrency counters reset")
            return True
            
        except Exception as e:
            logger.error(f"Error resetting counts: {e}")
            return False
    
    def cleanup_stale_tasks(self, max_age_seconds: int = 3600) -> int:
        """
        Clean up stale task mappings and adjust counters.
        
        Args:
            max_age_seconds: Maximum age for task mappings
            
        Returns:
            Number of stale tasks cleaned up
        """
        try:
            task_keys = self.redis.keys(f"{self.TASK_MAP_PREFIX}*")
            current_time = time.time()
            cleaned = 0
            
            for task_key in task_keys:
                task_data_str = self.redis.get(task_key)
                if not task_data_str:
                    continue
                
                try:
                    task_data = json.loads(task_data_str)
                    start_time = task_data.get('start_time', 0)
                    
                    if current_time - start_time > max_age_seconds:
                        # Extract task ID from key
                        task_id = task_key.replace(self.TASK_MAP_PREFIX, '')
                        # Clean up this stale task
                        if self.register_task_end(task_id):
                            cleaned += 1
                            logger.warning(
                                f"Cleaned up stale task {task_id} "
                                f"(age: {int(current_time - start_time)}s)"
                            )
                except json.JSONDecodeError:
                    logger.warning(f"Invalid task data for {task_key}")
                    continue
            
            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} stale tasks")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error cleaning up stale tasks: {e}")
            return 0


# Global instance
_concurrency_manager = None


def get_concurrency_manager() -> ConcurrencyManager:
    """
    Get or create the global concurrency manager instance.
    
    Returns:
        ConcurrencyManager instance
    """
    global _concurrency_manager
    if _concurrency_manager is None:
        _concurrency_manager = ConcurrencyManager()
    return _concurrency_manager
