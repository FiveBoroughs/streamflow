#!/usr/bin/env python3
"""
Test the UDIRedisStorage update_channel and update_stream methods.

This test verifies that the UDIRedisStorage class properly implements
the update_channel and update_stream methods that match the interface
of UDIStorage.
"""

import unittest
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestRedisStorageUpdateInterface(unittest.TestCase):
    """Test that Redis storage has the required update methods."""
    
    def test_redis_storage_has_update_channel_method(self):
        """Test that UDIRedisStorage has update_channel method."""
        from udi.redis_storage import UDIRedisStorage
        
        # Check the method exists
        self.assertTrue(hasattr(UDIRedisStorage, 'update_channel'))
        
        # Check it's callable
        self.assertTrue(callable(getattr(UDIRedisStorage, 'update_channel')))
    
    def test_redis_storage_has_update_stream_method(self):
        """Test that UDIRedisStorage has update_stream method."""
        from udi.redis_storage import UDIRedisStorage
        
        # Check the method exists
        self.assertTrue(hasattr(UDIRedisStorage, 'update_stream'))
        
        # Check it's callable
        self.assertTrue(callable(getattr(UDIRedisStorage, 'update_stream')))
    
    def test_update_channel_signature_matches_storage(self):
        """Test that update_channel has the same signature as UDIStorage."""
        from udi.redis_storage import UDIRedisStorage
        from udi.storage import UDIStorage
        import inspect
        
        redis_sig = inspect.signature(UDIRedisStorage.update_channel)
        storage_sig = inspect.signature(UDIStorage.update_channel)
        
        # Both should have channel_id and channel_data parameters
        redis_params = list(redis_sig.parameters.keys())
        storage_params = list(storage_sig.parameters.keys())
        
        # Remove 'self' from comparison
        redis_params = [p for p in redis_params if p != 'self']
        storage_params = [p for p in storage_params if p != 'self']
        
        self.assertEqual(redis_params, storage_params, 
                        "update_channel signatures should match")
    
    def test_update_stream_signature_matches_storage(self):
        """Test that update_stream has the same signature as UDIStorage."""
        from udi.redis_storage import UDIRedisStorage
        from udi.storage import UDIStorage
        import inspect
        
        redis_sig = inspect.signature(UDIRedisStorage.update_stream)
        storage_sig = inspect.signature(UDIStorage.update_stream)
        
        # Both should have stream_id and stream_data parameters
        redis_params = list(redis_sig.parameters.keys())
        storage_params = list(storage_sig.parameters.keys())
        
        # Remove 'self' from comparison
        redis_params = [p for p in redis_params if p != 'self']
        storage_params = [p for p in storage_params if p != 'self']
        
        self.assertEqual(redis_params, storage_params,
                        "update_stream signatures should match")


def main():
    """Run the tests."""
    print("=" * 60)
    print("Redis Storage Update Interface Tests")
    print("=" * 60)
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRedisStorageUpdateInterface)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print(f"Results: {result.testsRun - len(result.failures) - len(result.errors)}/{result.testsRun} tests passed")
    print("=" * 60)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())
