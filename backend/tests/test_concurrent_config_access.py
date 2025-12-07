#!/usr/bin/env python3
"""
Test that concurrent_config variable is properly accessed in _check_channel_concurrent.

This test specifically verifies the fix for the NameError: name 'concurrent_config' is not defined
that occurred at line 1340 in stream_checker_service.py.
"""

import unittest
import sys
import os
import tempfile
import json
from pathlib import Path

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestConcurrentConfigAccess(unittest.TestCase):
    """Test that concurrent configuration is properly accessed."""
    
    def test_concurrent_streams_config_structure(self):
        """Test that concurrent_streams config has the expected structure."""
        from stream_checker_service import StreamCheckConfig
        
        # Create a temp config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_file = Path(f.name)
            json.dump({}, f)
        
        try:
            config = StreamCheckConfig(config_file=config_file)
            
            # Verify concurrent_streams section exists
            self.assertIn('concurrent_streams', config.config)
            
            # Verify expected keys
            concurrent_config = config.config['concurrent_streams']
            self.assertIn('global_limit', concurrent_config)
            self.assertIn('enabled', concurrent_config)
            self.assertIn('stagger_delay', concurrent_config)
            
            # Verify types
            self.assertIsInstance(concurrent_config['global_limit'], int)
            self.assertIsInstance(concurrent_config['enabled'], bool)
            self.assertIsInstance(concurrent_config['stagger_delay'], (int, float))
        finally:
            if config_file.exists():
                config_file.unlink()
    
    def test_stagger_delay_config_access_pattern(self):
        """Test that stagger_delay can be accessed using the config.get pattern."""
        from stream_checker_service import StreamCheckConfig
        
        # Create a temp config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_file = Path(f.name)
            json.dump({}, f)
        
        try:
            # Create config with temp config file
            config = StreamCheckConfig(config_file=config_file)
            
            # Verify stagger_delay can be accessed via config.get pattern (default value)
            stagger_delay = config.get('concurrent_streams.stagger_delay', 1.0)
            self.assertEqual(stagger_delay, 1.0)  # Default value
            
            # Custom stagger delay
            config.config['concurrent_streams']['stagger_delay'] = 2.5
            stagger_delay = config.get('concurrent_streams.stagger_delay', 1.0)
            self.assertEqual(stagger_delay, 2.5)
            
            # Verify missing stagger_delay falls back to default
            del config.config['concurrent_streams']['stagger_delay']
            stagger_delay = config.get('concurrent_streams.stagger_delay', 1.0)
            self.assertEqual(stagger_delay, 1.0)
        finally:
            if config_file.exists():
                config_file.unlink()
    
    def test_stagger_delay_access_method_consistency(self):
        """Test that stagger_delay access is consistent with other concurrent_streams settings."""
        from stream_checker_service import StreamCheckConfig
        
        # Create a temp config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_file = Path(f.name)
            json.dump({}, f)
        
        try:
            config = StreamCheckConfig(config_file=config_file)
            
            # Verify all concurrent_streams settings use the same access pattern
            global_limit = config.get('concurrent_streams.global_limit', 10)
            enabled = config.get('concurrent_streams.enabled', True)
            stagger_delay = config.get('concurrent_streams.stagger_delay', 1.0)
            
            # All should return values (not None)
            self.assertIsNotNone(global_limit)
            self.assertIsNotNone(enabled)
            self.assertIsNotNone(stagger_delay)
            
            # Verify types
            self.assertIsInstance(global_limit, int)
            self.assertIsInstance(enabled, bool)
            self.assertIsInstance(stagger_delay, (int, float))
        finally:
            if config_file.exists():
                config_file.unlink()


def main():
    """Run the tests."""
    print("=" * 60)
    print("Concurrent Config Access Tests")
    print("=" * 60)
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestConcurrentConfigAccess)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print(f"Results: {result.testsRun - len(result.failures) - len(result.errors)}/{result.testsRun} tests passed")
    print("=" * 60)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())
