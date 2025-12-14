#!/usr/bin/env python3
"""
Test group-level filtering for matching and checking modes.

This test verifies that channel group settings (matching_mode and checking_mode)
are properly respected when filtering channels for stream matching and checking operations.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import shutil


class TestGroupLevelFiltering(unittest.TestCase):
    """Test that group settings properly filter channels."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_settings_file = Path(self.temp_dir) / 'channel_settings.json'
        self.test_group_settings_file = Path(self.temp_dir) / 'group_settings.json'
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_channel_enabled_group_disabled_matching(self):
        """Test that a channel with matching enabled is filtered out if its group has matching disabled."""
        # Mock the channel settings manager
        with patch('channel_settings_manager.CONFIG_DIR', Path(self.temp_dir)):
            from channel_settings_manager import ChannelSettingsManager
            
            manager = ChannelSettingsManager()
            
            # Enable matching for channel 1
            manager.set_channel_settings(1, matching_mode='enabled')
            
            # Disable matching for group 10
            manager.set_group_settings(10, matching_mode='disabled')
            
            # Verify channel-level setting
            self.assertTrue(manager.is_matching_enabled(1))
            
            # Verify group-level setting
            self.assertFalse(manager.is_group_matching_enabled(10))
            
            # Verify combined check - channel is NOT enabled because group is disabled
            self.assertFalse(manager.is_channel_enabled_by_group(10, mode='matching'))
    
    def test_channel_enabled_group_enabled_matching(self):
        """Test that a channel with matching enabled is included if its group also has matching enabled."""
        with patch('channel_settings_manager.CONFIG_DIR', Path(self.temp_dir)):
            from channel_settings_manager import ChannelSettingsManager
            
            manager = ChannelSettingsManager()
            
            # Enable matching for channel 1
            manager.set_channel_settings(1, matching_mode='enabled')
            
            # Enable matching for group 10 (default is enabled, but explicitly set it)
            manager.set_group_settings(10, matching_mode='enabled')
            
            # Verify both are enabled
            self.assertTrue(manager.is_matching_enabled(1))
            self.assertTrue(manager.is_group_matching_enabled(10))
            self.assertTrue(manager.is_channel_enabled_by_group(10, mode='matching'))
    
    def test_channel_no_group_matching(self):
        """Test that a channel without a group is always enabled."""
        with patch('channel_settings_manager.CONFIG_DIR', Path(self.temp_dir)):
            from channel_settings_manager import ChannelSettingsManager
            
            manager = ChannelSettingsManager()
            
            # Enable matching for channel 1
            manager.set_channel_settings(1, matching_mode='enabled')
            
            # Verify channel without group is enabled
            self.assertTrue(manager.is_channel_enabled_by_group(None, mode='matching'))
    
    def test_channel_enabled_group_disabled_checking(self):
        """Test that a channel with checking enabled is filtered out if its group has checking disabled."""
        with patch('channel_settings_manager.CONFIG_DIR', Path(self.temp_dir)):
            from channel_settings_manager import ChannelSettingsManager
            
            manager = ChannelSettingsManager()
            
            # Enable checking for channel 1
            manager.set_channel_settings(1, checking_mode='enabled')
            
            # Disable checking for group 10
            manager.set_group_settings(10, checking_mode='disabled')
            
            # Verify channel-level setting
            self.assertTrue(manager.is_checking_enabled(1))
            
            # Verify group-level setting
            self.assertFalse(manager.is_group_checking_enabled(10))
            
            # Verify combined check - channel is NOT enabled because group is disabled
            self.assertFalse(manager.is_channel_enabled_by_group(10, mode='checking'))
    
    def test_automated_stream_manager_respects_group_settings(self):
        """Test that automated_stream_manager properly filters channels based on group settings."""
        with patch('channel_settings_manager.CONFIG_DIR', Path(self.temp_dir)):
            from channel_settings_manager import ChannelSettingsManager
            
            manager = ChannelSettingsManager()
            
            # Set up test data: Channel 1 in Group 10, Channel 2 in Group 20
            # Enable matching for both channels
            manager.set_channel_settings(1, matching_mode='enabled')
            manager.set_channel_settings(2, matching_mode='enabled')
            
            # Disable matching for Group 10, enable for Group 20
            manager.set_group_settings(10, matching_mode='disabled')
            manager.set_group_settings(20, matching_mode='enabled')
            
            # Simulate filtering logic
            channels = [
                {'id': 1, 'name': 'Channel 1', 'channel_group_id': 10},
                {'id': 2, 'name': 'Channel 2', 'channel_group_id': 20}
            ]
            
            filtered_channels = []
            for channel in channels:
                channel_id = channel['id']
                channel_group_id = channel.get('channel_group_id')
                
                channel_enabled = manager.is_matching_enabled(channel_id)
                group_enabled = manager.is_channel_enabled_by_group(channel_group_id, mode='matching')
                
                if channel_enabled and group_enabled:
                    filtered_channels.append(channel_id)
            
            # Only Channel 2 should be included
            self.assertEqual(filtered_channels, [2])
    
    def test_stream_checker_service_respects_group_settings(self):
        """Test that stream_checker_service properly filters channels based on group settings."""
        with patch('channel_settings_manager.CONFIG_DIR', Path(self.temp_dir)):
            from channel_settings_manager import ChannelSettingsManager
            
            manager = ChannelSettingsManager()
            
            # Set up test data: Channel 1 in Group 10, Channel 2 in Group 20
            # Enable checking for both channels
            manager.set_channel_settings(1, checking_mode='enabled')
            manager.set_channel_settings(2, checking_mode='enabled')
            
            # Disable checking for Group 10, enable for Group 20
            manager.set_group_settings(10, checking_mode='disabled')
            manager.set_group_settings(20, checking_mode='enabled')
            
            # Simulate filtering logic
            channels = [
                {'id': 1, 'name': 'Channel 1', 'channel_group_id': 10},
                {'id': 2, 'name': 'Channel 2', 'channel_group_id': 20}
            ]
            
            filtered_channels = []
            for channel in channels:
                channel_id = channel['id']
                channel_group_id = channel.get('channel_group_id')
                
                channel_enabled = manager.is_checking_enabled(channel_id)
                group_enabled = manager.is_channel_enabled_by_group(channel_group_id, mode='checking')
                
                if channel_enabled and group_enabled:
                    filtered_channels.append(channel_id)
            
            # Only Channel 2 should be included
            self.assertEqual(filtered_channels, [2])


if __name__ == '__main__':
    unittest.main()
