#!/usr/bin/env python3
"""
Unit tests for bitrate detection in stream analysis.

This test module verifies:
1. Primary bitrate detection method (Statistics: line)
2. Fallback methods for various ffmpeg output formats
3. Progress output parsing
4. Warning when bitrate detection fails
"""

import unittest
import subprocess
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the function we're testing from the new module
from stream_check_utils import get_stream_bitrate


class TestBitrateDetection(unittest.TestCase):
    """Test bitrate detection from various ffmpeg output formats."""

    @patch('subprocess.run')
    def test_bitrate_method_1_statistics_line(self, mock_run):
        """Test Method 1: Primary detection via Statistics: line with bytes read."""
        # Simulate ffmpeg output with Statistics line
        mock_result = MagicMock()
        mock_result.stderr = """
[debug] Input stream #0:0: 500 frames decoded; 0 decode errors
Statistics: 15000000 bytes read; 0 seeks
        """
        mock_run.return_value = mock_result
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8', 
            duration=30,
            timeout=10
        )
        
        # Should detect bitrate from Statistics line
        # Expected: (15000000 bytes * 8 bits) / 1000 / 30 seconds = 4000 kbps
        self.assertIsNotNone(bitrate, "Bitrate should be detected from Statistics line")
        self.assertAlmostEqual(bitrate, 4000.0, places=1, msg="Bitrate calculation should be accurate")
        self.assertEqual(status, "OK", "Status should be OK")

    @patch('subprocess.run')
    def test_bitrate_method_2_progress_output(self, mock_run):
        """Test Method 2: Fallback detection via progress output with bitrate= pattern."""
        # Simulate ffmpeg output with progress lines but no Statistics
        mock_result = MagicMock()
        mock_result.stderr = """
frame=  500 fps= 25 q=-1.0 size=   12000kB time=00:00:20.00 bitrate=4800.0kbits/s speed=1.0x
frame=  750 fps= 25 q=-1.0 size=   18000kB time=00:00:30.00 bitrate=4800.0kbits/s speed=1.0x
        """
        mock_run.return_value = mock_result
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=10
        )
        
        # Should detect bitrate from progress output
        self.assertIsNotNone(bitrate, "Bitrate should be detected from progress output")
        self.assertAlmostEqual(bitrate, 4800.0, places=1, msg="Bitrate should match progress value")

    @patch('subprocess.run')
    def test_bitrate_method_3_bytes_read_without_statistics(self, mock_run):
        """Test Method 3: Alternative bytes read pattern without Statistics: prefix."""
        # Simulate ffmpeg output with bytes read but no Statistics: prefix
        mock_result = MagicMock()
        mock_result.stderr = """
[debug] 12000000 bytes read from input
        """
        mock_run.return_value = mock_result
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=10
        )
        
        # Calculate expected: (12000000 bytes * 8 bits) / 1000 / 30 seconds = 3200 kbps
        self.assertIsNotNone(bitrate, "Bitrate should be detected from bytes read")
        self.assertAlmostEqual(bitrate, 3200.0, places=1, msg="Bitrate calculation should work")

    @patch('subprocess.run')
    def test_bitrate_all_methods_fail(self, mock_run):
        """Test that bitrate remains None when all detection methods fail."""
        # Simulate ffmpeg output with no recognizable bitrate patterns
        mock_result = MagicMock()
        mock_result.stderr = """
[info] Stream started
[info] Stream ended
        """
        mock_run.return_value = mock_result
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=10
        )
        
        # Bitrate should remain None when no patterns match
        self.assertIsNone(bitrate, "Bitrate should be None when detection fails")

    @patch('subprocess.run')
    def test_bitrate_multiple_progress_lines(self, mock_run):
        """Test that the last progress bitrate is used when Statistics is missing."""
        # Simulate multiple progress updates - should use the last one
        mock_result = MagicMock()
        mock_result.stderr = """
frame=  250 fps= 25 q=-1.0 size=    6000kB time=00:00:10.00 bitrate=4800.0kbits/s speed=1.0x
frame=  500 fps= 25 q=-1.0 size=   11000kB time=00:00:20.00 bitrate=4400.0kbits/s speed=1.0x
frame=  750 fps= 25 q=-1.0 size=   15000kB time=00:00:30.00 bitrate=4000.0kbits/s speed=1.0x
        """
        mock_run.return_value = mock_result
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=10
        )
        
        # Should use the last progress bitrate
        self.assertIsNotNone(bitrate, "Bitrate should be detected")
        self.assertAlmostEqual(bitrate, 4000.0, places=1, msg="Should use last progress bitrate")

    @patch('subprocess.run')
    def test_bitrate_priority_statistics_over_progress(self, mock_run):
        """Test that Statistics method takes priority over progress output."""
        # Both methods should work, but Statistics should be preferred
        mock_result = MagicMock()
        mock_result.stderr = """
frame=  750 fps= 25 q=-1.0 size=   15000kB time=00:00:30.00 bitrate=4000.0kbits/s speed=1.0x
Statistics: 18000000 bytes read; 0 seeks
        """
        mock_run.return_value = mock_result
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=10
        )
        
        # Should use Statistics method: (18000000 * 8) / 1000 / 30 = 4800 kbps
        self.assertIsNotNone(bitrate, "Bitrate should be detected")
        self.assertAlmostEqual(bitrate, 4800.0, places=1, msg="Should prioritize Statistics method")

    @patch('subprocess.run')
    def test_bitrate_timeout_handling(self, mock_run):
        """Test that timeout is handled gracefully."""
        test_timeout = 10
        expected_timeout = test_timeout + 30 + 10  # timeout + duration + buffer
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='ffmpeg', timeout=expected_timeout)
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=test_timeout
        )
        
        self.assertIsNone(bitrate, "Bitrate should be None on timeout")
        self.assertEqual(status, "Timeout", "Status should indicate timeout")

    @patch('subprocess.run')
    def test_bitrate_error_handling(self, mock_run):
        """Test that general errors are handled gracefully."""
        mock_run.side_effect = Exception("Network error")
        
        bitrate, status, elapsed = get_stream_bitrate(
            'http://test.com/stream.m3u8',
            duration=30,
            timeout=10
        )
        
        self.assertIsNone(bitrate, "Bitrate should be None on error")
        self.assertEqual(status, "Error", "Status should indicate error")


if __name__ == '__main__':
    unittest.main()
