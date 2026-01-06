#!/usr/bin/env python3
"""
Test for single channel check functionality - simplified version.
"""

import unittest
import json
import tempfile
from pathlib import Path
from datetime import datetime


class ChangelogManagerSimple:
    """Simplified ChangelogManager for testing."""
    
    def __init__(self, changelog_file=None):
        self.changelog_file = Path(changelog_file) if changelog_file else Path('test_changelog.json')
        self.changelog = []
    
    def add_entry(self, action: str, details: dict, timestamp=None, subentries=None):
        """Add a new changelog entry."""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        entry = {
            "timestamp": timestamp,
            "action": action,
            "details": details
        }
        
        if subentries:
            entry["subentries"] = subentries
        
        self.changelog.append(entry)
    
    def add_single_channel_check_entry(self, channel_id: int, channel_name: str, check_stats: dict):
        """Add a single channel check entry."""
        check_subentries = [{
            'type': 'check',
            'channel_id': channel_id,
            'channel_name': channel_name,
            'stats': check_stats
        }]
        
        subentries = [{'group': 'check', 'items': check_subentries}]
        
        self.add_entry(
            action='single_channel_check',
            details={
                'channel_id': channel_id,
                'channel_name': channel_name,
                'total_streams': check_stats.get('total_streams', 0),
                'dead_streams': check_stats.get('dead_streams', 0),
                'avg_resolution': check_stats.get('avg_resolution', 'N/A'),
                'avg_bitrate': check_stats.get('avg_bitrate', 'N/A')
            },
            subentries=subentries
        )
    
    def add_global_check_entry(self, channels_checked: dict, global_stats: dict):
        """Add a global check entry."""
        check_subentries = []
        for channel_id, stats in channels_checked.items():
            check_subentries.append({
                'type': 'check',
                'channel_id': channel_id,
                'channel_name': stats.get('channel_name', f'Channel {channel_id}'),
                'stats': stats
            })
        
        subentries = [{'group': 'check', 'items': check_subentries}] if check_subentries else []
        
        self.add_entry(
            action='global_check',
            details=global_stats,
            subentries=subentries
        )
    
    def add_playlist_update_entry(self, channels_updated: dict, global_stats: dict):
        """Add a playlist update & match entry."""
        update_subentries = []
        for channel_id, info in channels_updated.items():
            if info.get('streams_added'):
                update_subentries.append({
                    'type': 'update_match',
                    'channel_id': channel_id,
                    'channel_name': info.get('channel_name', f'Channel {channel_id}'),
                    'streams': info.get('streams_added', [])
                })
        
        check_subentries = []
        for channel_id, info in channels_updated.items():
            if info.get('check_stats'):
                check_subentries.append({
                    'type': 'check',
                    'channel_id': channel_id,
                    'channel_name': info.get('channel_name', f'Channel {channel_id}'),
                    'stats': info.get('check_stats', {})
                })
        
        subentries = []
        if update_subentries:
            subentries.append({'group': 'update_match', 'items': update_subentries})
        if check_subentries:
            subentries.append({'group': 'check', 'items': check_subentries})
        
        self.add_entry(
            action='playlist_update_match',
            details=global_stats,
            subentries=subentries
        )
    
    def _has_channel_updates(self, entry: dict) -> bool:
        """Check if entry has meaningful updates."""
        action = entry.get('action', '')
        
        if action in ['playlist_update_match', 'global_check', 'single_channel_check']:
            subentries = entry.get('subentries', [])
            return any(group.get('items') for group in subentries)
        
        return True


class TestChangelogManager(unittest.TestCase):
    """Test changelog manager with new structured entries."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.manager = ChangelogManagerSimple()
    
    def test_add_single_channel_check_entry(self):
        """Test adding a single channel check entry."""
        channel_id = 123
        channel_name = "Test Channel"
        check_stats = {
            'total_streams': 10,
            'dead_streams': 2,
            'avg_resolution': '1920x1080',
            'avg_bitrate': '5000 kbps',
            'stream_details': []
        }
        
        self.manager.add_single_channel_check_entry(
            channel_id=channel_id,
            channel_name=channel_name,
            check_stats=check_stats
        )
        
        # Verify entry was added
        self.assertEqual(len(self.manager.changelog), 1)
        entry = self.manager.changelog[0]
        
        # Check action type
        self.assertEqual(entry['action'], 'single_channel_check')
        
        # Check details
        self.assertEqual(entry['details']['channel_id'], channel_id)
        self.assertEqual(entry['details']['channel_name'], channel_name)
        self.assertEqual(entry['details']['total_streams'], 10)
        self.assertEqual(entry['details']['dead_streams'], 2)
        
        # Check subentries structure
        self.assertIn('subentries', entry)
        self.assertEqual(len(entry['subentries']), 1)
        self.assertEqual(entry['subentries'][0]['group'], 'check')
        self.assertEqual(len(entry['subentries'][0]['items']), 1)
        
        # Check subentry item
        item = entry['subentries'][0]['items'][0]
        self.assertEqual(item['channel_id'], channel_id)
        self.assertEqual(item['channel_name'], channel_name)
        self.assertIn('stats', item)
    
    def test_add_global_check_entry(self):
        """Test adding a global check entry."""
        channels_checked = {
            1: {
                'channel_name': 'Channel 1',
                'total_streams': 5,
                'dead_streams': 1,
                'avg_resolution': '1920x1080',
                'avg_bitrate': '4000 kbps'
            },
            2: {
                'channel_name': 'Channel 2',
                'total_streams': 8,
                'dead_streams': 0,
                'avg_resolution': '1280x720',
                'avg_bitrate': '3000 kbps'
            }
        }
        
        global_stats = {
            'total_streams': 13,
            'dead_streams': 1,
            'avg_resolution': '1600x900',
            'avg_bitrate': '3500 kbps'
        }
        
        self.manager.add_global_check_entry(
            channels_checked=channels_checked,
            global_stats=global_stats
        )
        
        # Verify entry was added
        self.assertEqual(len(self.manager.changelog), 1)
        entry = self.manager.changelog[0]
        
        # Check action type
        self.assertEqual(entry['action'], 'global_check')
        
        # Check subentries
        self.assertIn('subentries', entry)
        self.assertEqual(len(entry['subentries']), 1)
        self.assertEqual(entry['subentries'][0]['group'], 'check')
        self.assertEqual(len(entry['subentries'][0]['items']), 2)
    
    def test_add_playlist_update_entry(self):
        """Test adding a playlist update & match entry."""
        channels_updated = {
            1: {
                'channel_name': 'Channel 1',
                'streams_added': [
                    {'id': 101, 'name': 'Stream 1'},
                    {'id': 102, 'name': 'Stream 2'}
                ],
                'check_stats': {
                    'total_streams': 10,
                    'dead_streams': 1,
                    'avg_resolution': '1920x1080',
                    'avg_bitrate': '5000 kbps'
                }
            }
        }
        
        global_stats = {
            'total_streams': 10,
            'dead_streams': 1,
            'avg_resolution': '1920x1080',
            'avg_bitrate': '5000 kbps'
        }
        
        self.manager.add_playlist_update_entry(
            channels_updated=channels_updated,
            global_stats=global_stats
        )
        
        # Verify entry was added
        self.assertEqual(len(self.manager.changelog), 1)
        entry = self.manager.changelog[0]
        
        # Check action type
        self.assertEqual(entry['action'], 'playlist_update_match')
        
        # Check subentries
        self.assertIn('subentries', entry)
        # Should have both update_match and check groups
        self.assertEqual(len(entry['subentries']), 2)
        
        # Check update_match group
        update_group = next(g for g in entry['subentries'] if g['group'] == 'update_match')
        self.assertEqual(len(update_group['items']), 1)
        self.assertEqual(len(update_group['items'][0]['streams']), 2)
        
        # Check check group
        check_group = next(g for g in entry['subentries'] if g['group'] == 'check')
        self.assertEqual(len(check_group['items']), 1)
    
    def test_has_channel_updates(self):
        """Test the _has_channel_updates filter."""
        # Test with single_channel_check
        entry_with_subentries = {
            'action': 'single_channel_check',
            'details': {},
            'subentries': [
                {'group': 'check', 'items': [{'channel_id': 1}]}
            ]
        }
        self.assertTrue(self.manager._has_channel_updates(entry_with_subentries))
        
        # Test with no subentries
        entry_no_subentries = {
            'action': 'single_channel_check',
            'details': {},
            'subentries': []
        }
        self.assertFalse(self.manager._has_channel_updates(entry_no_subentries))
        
        # Test with empty items
        entry_empty_items = {
            'action': 'single_channel_check',
            'details': {},
            'subentries': [
                {'group': 'check', 'items': []}
            ]
        }
        self.assertFalse(self.manager._has_channel_updates(entry_empty_items))


if __name__ == '__main__':
    unittest.main()
