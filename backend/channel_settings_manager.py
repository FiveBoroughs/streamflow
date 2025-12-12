#!/usr/bin/env python3
"""
Channel Settings Manager for StreamFlow.

Manages per-channel settings that are not stored in Dispatcharr's database,
such as matching and checking modes. These settings control which channels
should be included in stream matching and quality checking operations.
"""

import json
import os
import threading
from pathlib import Path
from typing import Dict, Optional, Any

from logging_config import setup_logging

logger = setup_logging(__name__)

# Configuration directory
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
CHANNEL_SETTINGS_FILE = CONFIG_DIR / 'channel_settings.json'


class ChannelSettingsManager:
    """
    Manages channel-specific settings for StreamFlow.
    
    Settings include:
    - matching_mode: Whether to include channel in stream matching ('enabled', 'disabled')
    - checking_mode: Whether to include channel in stream checking ('enabled', 'disabled')
    """
    
    # Mode constants
    MODE_ENABLED = 'enabled'
    MODE_DISABLED = 'disabled'
    
    def __init__(self):
        """Initialize the channel settings manager."""
        self._lock = threading.Lock()
        self._settings: Dict[int, Dict[str, Any]] = {}
        self._load_settings()
        logger.info("Channel settings manager initialized")
    
    def _load_settings(self) -> None:
        """Load channel settings from file."""
        try:
            if CHANNEL_SETTINGS_FILE.exists():
                with open(CHANNEL_SETTINGS_FILE, 'r') as f:
                    data = json.load(f)
                    # Convert string keys back to integers
                    self._settings = {int(k): v for k, v in data.items()}
                    logger.info(f"Loaded settings for {len(self._settings)} channels")
            else:
                self._settings = {}
                logger.info("No existing channel settings file, starting fresh")
        except Exception as e:
            logger.error(f"Error loading channel settings: {e}", exc_info=True)
            self._settings = {}
    
    def _save_settings(self) -> bool:
        """Save channel settings to file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            # Convert integer keys to strings for JSON serialization
            data = {str(k): v for k, v in self._settings.items()}
            with open(CHANNEL_SETTINGS_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug("Channel settings saved successfully")
            return True
        except Exception as e:
            logger.error(f"Error saving channel settings: {e}", exc_info=True)
            return False
    
    def get_channel_settings(self, channel_id: int) -> Dict[str, str]:
        """Get settings for a specific channel.
        
        Args:
            channel_id: The channel ID
            
        Returns:
            Dictionary with channel settings (matching_mode, checking_mode)
            Defaults to 'enabled' for both if not set
        """
        with self._lock:
            settings = self._settings.get(channel_id, {})
            return {
                'matching_mode': settings.get('matching_mode', self.MODE_ENABLED),
                'checking_mode': settings.get('checking_mode', self.MODE_ENABLED)
            }
    
    def set_channel_settings(self, channel_id: int, matching_mode: Optional[str] = None,
                            checking_mode: Optional[str] = None) -> bool:
        """Set settings for a specific channel.
        
        Args:
            channel_id: The channel ID
            matching_mode: Matching mode ('enabled' or 'disabled'), None to keep current
            checking_mode: Checking mode ('enabled' or 'disabled'), None to keep current
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            # Get current settings or initialize new ones
            if channel_id not in self._settings:
                self._settings[channel_id] = {}
            
            # Update only provided fields
            if matching_mode is not None:
                if matching_mode not in [self.MODE_ENABLED, self.MODE_DISABLED]:
                    logger.error(f"Invalid matching_mode: {matching_mode}")
                    return False
                self._settings[channel_id]['matching_mode'] = matching_mode
            
            if checking_mode is not None:
                if checking_mode not in [self.MODE_ENABLED, self.MODE_DISABLED]:
                    logger.error(f"Invalid checking_mode: {checking_mode}")
                    return False
                self._settings[channel_id]['checking_mode'] = checking_mode
            
            # Save to file
            success = self._save_settings()
            if success:
                logger.info(f"Updated settings for channel {channel_id}: "
                          f"matching={self._settings[channel_id].get('matching_mode', 'enabled')}, "
                          f"checking={self._settings[channel_id].get('checking_mode', 'enabled')}")
            return success
    
    def get_all_settings(self) -> Dict[int, Dict[str, str]]:
        """Get all channel settings.
        
        Returns:
            Dictionary mapping channel IDs to their settings
        """
        with self._lock:
            return {
                channel_id: {
                    'matching_mode': settings.get('matching_mode', self.MODE_ENABLED),
                    'checking_mode': settings.get('checking_mode', self.MODE_ENABLED)
                }
                for channel_id, settings in self._settings.items()
            }
    
    def is_matching_enabled(self, channel_id: int) -> bool:
        """Check if matching is enabled for a channel.
        
        Args:
            channel_id: The channel ID
            
        Returns:
            True if matching is enabled (or not explicitly disabled), False otherwise
        """
        settings = self.get_channel_settings(channel_id)
        return settings['matching_mode'] == self.MODE_ENABLED
    
    def is_checking_enabled(self, channel_id: int) -> bool:
        """Check if checking is enabled for a channel.
        
        Args:
            channel_id: The channel ID
            
        Returns:
            True if checking is enabled (or not explicitly disabled), False otherwise
        """
        settings = self.get_channel_settings(channel_id)
        return settings['checking_mode'] == self.MODE_ENABLED
    
    def get_enabled_channels(self, channel_ids: List[int], mode: str = 'checking') -> List[int]:
        """Filter a list of channel IDs to only those with the specified mode enabled.
        
        Args:
            channel_ids: List of channel IDs to filter
            mode: Mode to check ('matching' or 'checking')
            
        Returns:
            List of channel IDs with the specified mode enabled
        """
        if mode == 'matching':
            return [cid for cid in channel_ids if self.is_matching_enabled(cid)]
        elif mode == 'checking':
            return [cid for cid in channel_ids if self.is_checking_enabled(cid)]
        else:
            logger.error(f"Invalid mode: {mode}")
            return channel_ids


# Singleton instance
_channel_settings_manager = None
_manager_lock = threading.Lock()


def get_channel_settings_manager() -> ChannelSettingsManager:
    """Get the singleton channel settings manager instance.
    
    Returns:
        ChannelSettingsManager instance
    """
    global _channel_settings_manager
    
    if _channel_settings_manager is None:
        with _manager_lock:
            if _channel_settings_manager is None:
                _channel_settings_manager = ChannelSettingsManager()
    
    return _channel_settings_manager
