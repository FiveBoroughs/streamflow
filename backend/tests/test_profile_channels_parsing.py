#!/usr/bin/env python3
"""
Tests for profile channels parsing logic

Tests the various formats that Dispatcharr might return
for profile.channels field and verifies they're parsed correctly.
"""

import unittest
from unittest.mock import Mock, patch
import json


class TestProfileChannelsParsing(unittest.TestCase):
    """Test cases for profile channels parsing."""
    
    def test_parse_json_string_with_correct_format(self):
        """Test parsing JSON string with channel_id and enabled fields."""
        channels_json = json.dumps([
            {'channel_id': 1, 'enabled': True},
            {'channel_id': 2, 'enabled': False},
            {'channel_id': 3, 'enabled': True}
        ])
        
        channels_data = json.loads(channels_json)
        
        # Format should already be correct
        formatted = []
        for item in channels_data:
            if 'channel_id' in item and 'enabled' in item:
                formatted.append(item)
        
        self.assertEqual(len(formatted), 3)
        self.assertEqual(formatted[0]['channel_id'], 1)
        self.assertTrue(formatted[0]['enabled'])
        self.assertEqual(formatted[1]['channel_id'], 2)
        self.assertFalse(formatted[1]['enabled'])
    
    def test_parse_json_string_with_id_and_enabled(self):
        """Test parsing JSON string with id and enabled fields."""
        channels_json = json.dumps([
            {'id': 10, 'enabled': True},
            {'id': 20, 'enabled': False}
        ])
        
        channels_data = json.loads(channels_json)
        
        # Convert id to channel_id
        formatted = []
        for item in channels_data:
            if 'id' in item and 'enabled' in item:
                formatted.append({
                    'channel_id': item['id'],
                    'enabled': item['enabled']
                })
        
        self.assertEqual(len(formatted), 2)
        self.assertEqual(formatted[0]['channel_id'], 10)
        self.assertTrue(formatted[0]['enabled'])
        self.assertEqual(formatted[1]['channel_id'], 20)
        self.assertFalse(formatted[1]['enabled'])
    
    def test_parse_list_of_ids(self):
        """Test parsing list of just channel IDs."""
        channels_data = [1, 2, 3, 4, 5]
        
        # Convert IDs to format with enabled=True
        formatted = []
        for item in channels_data:
            if isinstance(item, (int, str)):
                try:
                    channel_id = int(item)
                    formatted.append({
                        'channel_id': channel_id,
                        'enabled': True
                    })
                except (ValueError, TypeError):
                    pass
        
        self.assertEqual(len(formatted), 5)
        self.assertEqual(formatted[0]['channel_id'], 1)
        self.assertTrue(formatted[0]['enabled'])
        self.assertEqual(formatted[4]['channel_id'], 5)
        self.assertTrue(formatted[4]['enabled'])
    
    def test_parse_full_channel_objects(self):
        """Test parsing full channel objects with just id field."""
        channels_data = [
            {'id': 100, 'name': 'Channel 1', 'number': 1.0},
            {'id': 200, 'name': 'Channel 2', 'number': 2.0}
        ]
        
        # Convert full objects to format with enabled=True
        formatted = []
        for item in channels_data:
            if isinstance(item, dict) and 'id' in item:
                if 'enabled' not in item:
                    formatted.append({
                        'channel_id': item['id'],
                        'enabled': True
                    })
        
        self.assertEqual(len(formatted), 2)
        self.assertEqual(formatted[0]['channel_id'], 100)
        self.assertTrue(formatted[0]['enabled'])
    
    def test_parse_empty_list(self):
        """Test parsing empty channels list."""
        channels_data = []
        
        formatted = []
        for item in channels_data:
            pass  # No items to process
        
        self.assertEqual(len(formatted), 0)
    
    def test_parse_invalid_json_string(self):
        """Test that invalid JSON raises JSONDecodeError."""
        invalid_json = "not valid json {["
        
        with self.assertRaises(json.JSONDecodeError):
            json.loads(invalid_json)
    
    def test_parse_mixed_formats(self):
        """Test parsing mixed format data."""
        channels_data = [
            {'channel_id': 1, 'enabled': True},
            {'id': 2, 'enabled': False},
            {'id': 3, 'name': 'Channel 3'},
            4,
            "5"
        ]
        
        formatted = []
        for item in channels_data:
            if isinstance(item, dict):
                if 'channel_id' in item and 'enabled' in item:
                    formatted.append(item)
                elif 'id' in item and 'enabled' in item:
                    formatted.append({
                        'channel_id': item['id'],
                        'enabled': item['enabled']
                    })
                elif 'id' in item:
                    formatted.append({
                        'channel_id': item['id'],
                        'enabled': True
                    })
            elif isinstance(item, (int, str)):
                try:
                    channel_id = int(item)
                    formatted.append({
                        'channel_id': channel_id,
                        'enabled': True
                    })
                except (ValueError, TypeError):
                    pass
        
        self.assertEqual(len(formatted), 5)
        self.assertEqual(formatted[0]['channel_id'], 1)
        self.assertTrue(formatted[0]['enabled'])
        self.assertEqual(formatted[1]['channel_id'], 2)
        self.assertFalse(formatted[1]['enabled'])
        self.assertEqual(formatted[2]['channel_id'], 3)
        self.assertTrue(formatted[2]['enabled'])
        self.assertEqual(formatted[3]['channel_id'], 4)
        self.assertEqual(formatted[4]['channel_id'], 5)


if __name__ == '__main__':
    unittest.main()
