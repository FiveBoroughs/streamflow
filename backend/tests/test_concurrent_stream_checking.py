#!/usr/bin/env python3
"""
Test concurrent stream checking with Celery.

Tests the basic functionality of concurrent stream checking including:
- Celery task execution
- Concurrency management
- M3U account limits
- Global concurrent limits
"""

import unittest
import time
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestConcurrentStreamChecking(unittest.TestCase):
    """Test concurrent stream checking functionality."""
    
    def test_celery_app_configuration(self):
        """Test that Celery app is properly configured."""
        try:
            from celery_app import celery_app
            
            self.assertIsNotNone(celery_app)
            self.assertEqual(celery_app.conf.task_serializer, 'json')
            self.assertEqual(celery_app.conf.accept_content, ['json'])
            self.assertEqual(celery_app.conf.result_serializer, 'json')
        except ImportError:
            self.skipTest("Celery or Redis not available")
    
    def test_concurrency_manager_initialization(self):
        """Test concurrency manager can be initialized."""
        try:
            from concurrency_manager import ConcurrencyManager
            
            # Create with mock Redis client
            mock_redis = Mock()
            mock_redis.get.return_value = '0'
            mock_redis.keys.return_value = []
            mock_redis.ping.return_value = True
            
            manager = ConcurrencyManager(redis_client=mock_redis)
            self.assertIsNotNone(manager)
            
        except ImportError:
            self.skipTest("Redis not available")
    
    def test_can_start_task_respects_limits(self):
        """Test that concurrency limits are respected."""
        try:
            from concurrency_manager import ConcurrencyManager
            
            mock_redis = Mock()
            mock_redis.get.return_value = '5'  # Current count is 5
            
            manager = ConcurrencyManager(redis_client=mock_redis)
            
            # With global limit of 10, should allow task
            self.assertTrue(manager.can_start_task(None, 0, 10))
            
            # With global limit of 5, should not allow (at limit)
            self.assertFalse(manager.can_start_task(None, 0, 5))
            
            # With global limit of 3, should not allow (over limit)
            self.assertFalse(manager.can_start_task(None, 0, 3))
            
        except ImportError:
            self.skipTest("Redis not available")
    
    def test_can_start_task_respects_account_limits(self):
        """Test that M3U account limits are respected."""
        try:
            from concurrency_manager import ConcurrencyManager
            
            mock_redis = Mock()
            
            def mock_get(key):
                if 'account:123' in key:
                    return '2'  # Account 123 has 2 active streams
                return '5'  # Global count
            
            mock_redis.get = mock_get
            
            manager = ConcurrencyManager(redis_client=mock_redis)
            
            # Account limit is 3, currently at 2, should allow
            self.assertTrue(manager.can_start_task(123, 3, 10))
            
            # Account limit is 2, currently at 2, should not allow (at limit)
            self.assertFalse(manager.can_start_task(123, 2, 10))
            
            # Account limit is 1, currently at 2, should not allow (over limit)
            self.assertFalse(manager.can_start_task(123, 1, 10))
            
        except ImportError:
            self.skipTest("Redis not available")
    
    def test_register_task_start(self):
        """Test registering task start updates counters."""
        try:
            from concurrency_manager import ConcurrencyManager
            
            mock_redis = Mock()
            mock_pipe = Mock()
            mock_redis.pipeline.return_value.__enter__ = Mock(return_value=mock_pipe)
            mock_redis.pipeline.return_value.__exit__ = Mock(return_value=False)
            mock_pipe.incr = Mock()
            mock_pipe.setex = Mock()
            mock_pipe.execute = Mock()
            
            manager = ConcurrencyManager(redis_client=mock_redis)
            
            # Register a task
            success = manager.register_task_start('task123', 456, 789)
            
            self.assertTrue(success)
            
        except ImportError:
            self.skipTest("Redis not available")
    
    def test_stream_checker_config_has_concurrent_settings(self):
        """Test that stream checker config includes concurrent settings."""
        from stream_checker_service import StreamCheckConfig
        
        config = StreamCheckConfig()
        
        # Check default configuration
        self.assertIn('concurrent_streams', config.config)
        self.assertIn('global_limit', config.config['concurrent_streams'])
        self.assertIn('enabled', config.config['concurrent_streams'])
        
        # Check default values
        self.assertEqual(config.get('concurrent_streams.global_limit'), 10)
        self.assertEqual(config.get('concurrent_streams.enabled'), True)
    
    def test_udi_redis_storage_initialization(self):
        """Test that UDI Redis storage can be initialized."""
        try:
            from udi.redis_storage import UDIRedisStorage
            
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            
            storage = UDIRedisStorage(redis_client=mock_redis)
            self.assertIsNotNone(storage)
            
        except ImportError:
            self.skipTest("Redis not available")
    
    def test_udi_manager_uses_redis_storage(self):
        """Test that UDI manager can use Redis storage."""
        from udi.manager import UDIManager
        
        # UDI Manager should try Redis by default
        manager = UDIManager(use_redis=True)
        
        # It will fallback to file storage if Redis is unavailable
        # Just verify manager is created successfully
        self.assertIsNotNone(manager)
        self.assertIsNotNone(manager.storage)
    
    @patch('stream_check_utils.analyze_stream')
    def test_check_stream_task_structure(self, mock_analyze):
        """Test that check_stream_task has proper structure."""
        try:
            from celery_tasks import check_stream_task
            
            # Mock the analysis result
            mock_analyze.return_value = {
                'stream_id': 123,
                'stream_name': 'Test Stream',
                'resolution': '1920x1080',
                'fps': 30,
                'bitrate_kbps': 5000,
                'video_codec': 'h264',
                'audio_codec': 'aac',
                'status': 'OK'
            }
            
            # Verify task is registered and has required attributes
            self.assertIsNotNone(check_stream_task)
            self.assertTrue(hasattr(check_stream_task, 'apply_async'))
            self.assertEqual(check_stream_task.name, 'celery_tasks.check_stream_task')
        except ImportError:
            self.skipTest("Celery or Redis not available")


if __name__ == '__main__':
    unittest.main()

