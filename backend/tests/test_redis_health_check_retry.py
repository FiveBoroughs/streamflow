#!/usr/bin/env python3
"""
Test the Redis health check retry logic.

This test verifies that the UDIRedisStorage health_check method properly
retries connection attempts with exponential backoff before giving up.
"""

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from redis import ConnectionError

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestRedisHealthCheckRetry(unittest.TestCase):
    """Test Redis health check retry logic."""
    
    @patch('udi.redis_storage.Redis')
    def test_health_check_succeeds_first_try(self, mock_redis_class):
        """Test that health check returns True when Redis responds immediately."""
        from udi.redis_storage import UDIRedisStorage
        
        # Mock Redis client that responds to ping
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis_class.return_value = mock_redis
        
        # Create storage with mocked Redis
        storage = UDIRedisStorage(redis_client=mock_redis)
        
        # Health check should succeed immediately
        result = storage.health_check()
        
        self.assertTrue(result)
        self.assertEqual(mock_redis.ping.call_count, 1)
    
    @patch('udi.redis_storage.Redis')
    @patch('time.sleep')  # Mock sleep to avoid delays in tests
    def test_health_check_retries_and_succeeds(self, mock_sleep, mock_redis_class):
        """Test that health check retries and eventually succeeds."""
        from udi.redis_storage import UDIRedisStorage
        
        # Mock Redis client that fails first 3 times, then succeeds
        mock_redis = MagicMock()
        mock_redis.ping.side_effect = [
            ConnectionError("Connection refused"),
            ConnectionError("Connection refused"),
            ConnectionError("Connection refused"),
            True  # Success on 4th attempt
        ]
        mock_redis_class.return_value = mock_redis
        
        # Create storage with mocked Redis
        storage = UDIRedisStorage(redis_client=mock_redis)
        
        # Health check should eventually succeed
        result = storage.health_check(max_retries=5, initial_delay=0.1)
        
        self.assertTrue(result)
        self.assertEqual(mock_redis.ping.call_count, 4)
        # Should have slept 3 times (between attempts)
        self.assertEqual(mock_sleep.call_count, 3)
    
    @patch('udi.redis_storage.Redis')
    @patch('time.sleep')  # Mock sleep to avoid delays in tests
    def test_health_check_exhausts_retries(self, mock_sleep, mock_redis_class):
        """Test that health check fails after exhausting all retries."""
        from udi.redis_storage import UDIRedisStorage
        
        # Mock Redis client that always fails
        mock_redis = MagicMock()
        mock_redis.ping.side_effect = ConnectionError("Connection refused")
        mock_redis_class.return_value = mock_redis
        
        # Create storage with mocked Redis
        storage = UDIRedisStorage(redis_client=mock_redis)
        
        # Health check should fail after max_retries
        result = storage.health_check(max_retries=3, initial_delay=0.1)
        
        self.assertFalse(result)
        self.assertEqual(mock_redis.ping.call_count, 3)
        # Should have slept 2 times (between attempts, not after last failure)
        self.assertEqual(mock_sleep.call_count, 2)
    
    @patch('udi.redis_storage.Redis')
    @patch('time.sleep')  # Mock sleep to avoid delays in tests
    def test_health_check_exponential_backoff(self, mock_sleep, mock_redis_class):
        """Test that health check uses exponential backoff for retry delays."""
        from udi.redis_storage import UDIRedisStorage
        
        # Mock Redis client that always fails
        mock_redis = MagicMock()
        mock_redis.ping.side_effect = ConnectionError("Connection refused")
        mock_redis_class.return_value = mock_redis
        
        # Create storage with mocked Redis
        storage = UDIRedisStorage(redis_client=mock_redis)
        
        # Health check with specific initial delay
        initial_delay = 0.1
        result = storage.health_check(max_retries=4, initial_delay=initial_delay)
        
        self.assertFalse(result)
        
        # Verify exponential backoff: 0.1, 0.2, 0.4
        expected_delays = [initial_delay, initial_delay * 2, initial_delay * 4]
        actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
        
        self.assertEqual(actual_delays, expected_delays)


def main():
    """Run the tests."""
    print("=" * 60)
    print("Redis Health Check Retry Logic Tests")
    print("=" * 60)
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRedisHealthCheckRetry)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print(f"Results: {result.testsRun - len(result.failures) - len(result.errors)}/{result.testsRun} tests passed")
    print("=" * 60)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())
