#!/usr/bin/env python3
"""
Unit test for stream_id KeyError fix.

This test verifies that analyze_stream() always returns a complete dictionary
with all required fields, even when retries=0 or when exceptions occur.
"""

import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestStreamIdKeyErrorFix(unittest.TestCase):
    """Test that analyze_stream always returns complete dict with stream_id."""
    
    def test_analyze_stream_with_zero_retries(self):
        """Test that analyze_stream returns complete dict when retries=0.
        
        Note: retries=0 means range(0) which produces no iterations, so the
        function returns the default error result immediately. This test
        verifies that the default result includes all required fields.
        """
        from stream_check_utils import analyze_stream
        
        # Call analyze_stream with retries=0
        # Note: This is an edge case - with the current implementation using range(retries),
        # retries=0 causes zero attempts. This test verifies the bug fix ensures a complete
        # dict is returned even in this edge case.
        result = analyze_stream(
            stream_url='http://example.com/stream.m3u8',
            stream_id=12345,
            stream_name='Test Stream',
            ffmpeg_duration=20,
            timeout=30,
            retries=0,  # Edge case: causes range(0) = empty, so no attempts are made
            retry_delay=10,
            user_agent='VLC/3.0.14'
        )
        
        # Verify all required fields are present
        self.assertIn('stream_id', result)
        self.assertIn('stream_name', result)
        self.assertIn('stream_url', result)
        self.assertIn('timestamp', result)
        self.assertIn('video_codec', result)
        self.assertIn('audio_codec', result)
        self.assertIn('resolution', result)
        self.assertIn('fps', result)
        self.assertIn('bitrate_kbps', result)
        self.assertIn('status', result)
        
        # Verify values match input
        self.assertEqual(result['stream_id'], 12345)
        self.assertEqual(result['stream_name'], 'Test Stream')
        self.assertEqual(result['stream_url'], 'http://example.com/stream.m3u8')
        
        # Verify default error values
        self.assertEqual(result['video_codec'], 'N/A')
        self.assertEqual(result['audio_codec'], 'N/A')
        self.assertEqual(result['resolution'], '0x0')
        self.assertEqual(result['fps'], 0)
        self.assertIsNone(result['bitrate_kbps'])
        self.assertEqual(result['status'], 'Error')
    
    @patch('stream_check_utils.get_stream_info_and_bitrate')
    def test_analyze_stream_with_exception(self, mock_get_info):
        """Test that analyze_stream returns complete dict when exception occurs."""
        from stream_check_utils import analyze_stream
        
        # Make get_stream_info_and_bitrate raise an exception
        mock_get_info.side_effect = Exception("Network error")
        
        # Call analyze_stream
        result = analyze_stream(
            stream_url='http://example.com/stream.m3u8',
            stream_id=67890,
            stream_name='Test Stream 2',
            ffmpeg_duration=20,
            timeout=30,
            retries=1,
            retry_delay=10,
            user_agent='VLC/3.0.14'
        )
        
        # Verify all required fields are present
        self.assertIn('stream_id', result)
        self.assertIn('stream_name', result)
        self.assertIn('stream_url', result)
        self.assertIn('timestamp', result)
        self.assertIn('video_codec', result)
        self.assertIn('audio_codec', result)
        self.assertIn('resolution', result)
        self.assertIn('fps', result)
        self.assertIn('bitrate_kbps', result)
        self.assertIn('status', result)
        
        # Verify values match input
        self.assertEqual(result['stream_id'], 67890)
        self.assertEqual(result['stream_name'], 'Test Stream 2')
    
    def test_defensive_stream_id_access(self):
        """Test that .get() access pattern prevents KeyError with incomplete dicts."""
        # Create analyzed_streams list with one incomplete dict (simulating old bug)
        # and one complete dict
        analyzed_streams = [
            {
                'stream_id': 1,
                'stream_name': 'Stream 1',
                'resolution': '1920x1080',
                'score': 100
            },
            {
                # This dict is missing stream_id (simulating the bug)
                'stream_name': 'Stream 2',
                'resolution': '0x0',
                'score': 0
            }
        ]
        
        dead_stream_ids = {2}
        
        # Test the filtering logic that was causing KeyError
        # This should NOT raise KeyError with our fix
        try:
            filtered = [s for s in analyzed_streams if s.get('stream_id') not in dead_stream_ids]
            reordered_ids = [s.get('stream_id') for s in analyzed_streams if s.get('stream_id') is not None]
            
            # Should successfully filter and extract IDs
            self.assertEqual(len(filtered), 2)  # Both streams remain since one has no ID
            self.assertEqual(reordered_ids, [1])  # Only stream with ID is included
            
        except KeyError as e:
            self.fail(f"KeyError raised with safe .get() access: {e}")


if __name__ == '__main__':
    unittest.main()
