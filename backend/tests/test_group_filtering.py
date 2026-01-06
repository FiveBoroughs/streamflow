"""
Tests for channel group filtering functionality.

This test ensures that groups without channels are filtered out
from the UDI manager's get_channel_groups() method.
"""

import unittest
from unittest.mock import Mock, patch
from udi.manager import UDIManager


class TestGroupFiltering(unittest.TestCase):
    """Test cases for group filtering based on channel count."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create mock groups with varying channel counts
        self.mock_groups = [
            {"id": 1, "name": "Sports", "channel_count": 10},
            {"id": 2, "name": "Movies", "channel_count": 5},
            {"id": 3, "name": "Empty Group", "channel_count": 0},
            {"id": 4, "name": "News", "channel_count": 15},
            {"id": 5, "name": "Another Empty", "channel_count": 0},
        ]
    
    @patch('udi.manager.UDIStorage')
    @patch('udi.manager.UDIFetcher')
    @patch('udi.manager.UDICache')
    def test_filter_groups_with_no_channels(self, mock_cache, mock_fetcher, mock_storage):
        """Test that groups with channel_count == 0 are filtered out."""
        # Setup
        manager = UDIManager()
        manager._initialized = True
        manager._channel_groups_cache = self.mock_groups
        
        # Execute
        filtered_groups = manager.get_channel_groups()
        
        # Verify
        self.assertEqual(len(filtered_groups), 3, "Should return only groups with channels")
        
        # Check that only groups with channel_count > 0 are returned
        for group in filtered_groups:
            self.assertGreater(
                group['channel_count'], 
                0, 
                f"Group {group['name']} should have channels"
            )
        
        # Verify specific groups are present
        group_ids = [g['id'] for g in filtered_groups]
        self.assertIn(1, group_ids, "Sports group should be included")
        self.assertIn(2, group_ids, "Movies group should be included")
        self.assertIn(4, group_ids, "News group should be included")
        
        # Verify empty groups are not present
        self.assertNotIn(3, group_ids, "Empty Group should be filtered out")
        self.assertNotIn(5, group_ids, "Another Empty should be filtered out")
    
    @patch('udi.manager.UDIStorage')
    @patch('udi.manager.UDIFetcher')
    @patch('udi.manager.UDICache')
    def test_all_groups_have_channels(self, mock_cache, mock_fetcher, mock_storage):
        """Test when all groups have channels."""
        # Setup
        manager = UDIManager()
        manager._initialized = True
        manager._channel_groups_cache = [
            {"id": 1, "name": "Sports", "channel_count": 10},
            {"id": 2, "name": "Movies", "channel_count": 5},
        ]
        
        # Execute
        filtered_groups = manager.get_channel_groups()
        
        # Verify
        self.assertEqual(len(filtered_groups), 2, "All groups should be returned")
    
    @patch('udi.manager.UDIStorage')
    @patch('udi.manager.UDIFetcher')
    @patch('udi.manager.UDICache')
    def test_no_groups_have_channels(self, mock_cache, mock_fetcher, mock_storage):
        """Test when no groups have channels."""
        # Setup
        manager = UDIManager()
        manager._initialized = True
        manager._channel_groups_cache = [
            {"id": 1, "name": "Empty1", "channel_count": 0},
            {"id": 2, "name": "Empty2", "channel_count": 0},
        ]
        
        # Execute
        filtered_groups = manager.get_channel_groups()
        
        # Verify
        self.assertEqual(len(filtered_groups), 0, "No groups should be returned")
    
    @patch('udi.manager.UDIStorage')
    @patch('udi.manager.UDIFetcher')
    @patch('udi.manager.UDICache')
    def test_groups_without_channel_count_field(self, mock_cache, mock_fetcher, mock_storage):
        """Test groups that don't have the channel_count field."""
        # Setup
        manager = UDIManager()
        manager._initialized = True
        manager._channel_groups_cache = [
            {"id": 1, "name": "Normal", "channel_count": 5},
            {"id": 2, "name": "Missing Field"},  # No channel_count field
            {"id": 3, "name": "Zero", "channel_count": 0},
        ]
        
        # Execute
        filtered_groups = manager.get_channel_groups()
        
        # Verify - groups without channel_count should be filtered out (defaults to 0)
        self.assertEqual(len(filtered_groups), 1, "Only group with positive channel_count")
        self.assertEqual(filtered_groups[0]['id'], 1, "Should return Normal group")


if __name__ == '__main__':
    unittest.main()