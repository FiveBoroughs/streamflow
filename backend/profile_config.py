#!/usr/bin/env python3
"""
Channel Profile Configuration Manager

Manages channel profile selection and snapshots for dead stream management.
Profiles allow users to:
1. Use general channel list with empty channels disabled in a specific profile
2. Use a specific profile and disable empty channels in that profile with snapshot support
"""

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Any

from logging_config import setup_logging

logger = setup_logging(__name__)

# Configuration directory
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
PROFILE_CONFIG_FILE = CONFIG_DIR / 'profile_config.json'


class ProfileConfig:
    """
    Manages channel profile configuration for Streamflow.
    
    Handles:
    - Selected profile for Streamflow operations
    - Profile snapshots (which channels should be in a profile)
    - Dead stream management configuration per profile
    """
    
    def __init__(self):
        """Initialize the profile configuration manager."""
        self._lock = threading.Lock()
        self._config: Dict[str, Any] = {}
        
        # Reload CONFIG_DIR from environment in case it was changed (for testing)
        global CONFIG_DIR, PROFILE_CONFIG_FILE
        CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
        PROFILE_CONFIG_FILE = CONFIG_DIR / 'profile_config.json'
        
        self._load_config()
        logger.info("Profile configuration manager initialized")
    
    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            if PROFILE_CONFIG_FILE.exists():
                with open(PROFILE_CONFIG_FILE, 'r') as f:
                    self._config = json.load(f)
                    logger.info("Loaded profile configuration from file")
            else:
                # Initialize with default config
                self._config = {
                    'selected_profile_id': None,  # None = use general/all profile
                    'selected_profile_name': None,
                    'use_profile': False,  # Whether to use a specific profile
                    'dead_streams': {
                        'enabled': False,  # Enable empty channel management
                        'target_profile_id': None,  # Profile to disable channels in
                        'target_profile_name': None,
                        'use_snapshot': False,  # Whether to use snapshot for re-enabling
                    },
                    'snapshots': {}  # profile_id -> snapshot data
                }
                self._save_config()
                logger.info("Created default profile configuration")
        except Exception as e:
            logger.error(f"Error loading profile configuration: {e}", exc_info=True)
            self._config = {}
    
    def _save_config(self) -> bool:
        """Save configuration to file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            with open(PROFILE_CONFIG_FILE, 'w') as f:
                json.dump(self._config, f, indent=2)
            logger.info("Profile configuration saved to file")
            return True
        except Exception as e:
            logger.error(f"Error saving profile configuration: {e}", exc_info=True)
            return False
    
    def get_selected_profile(self) -> Optional[int]:
        """Get the selected profile ID for Streamflow operations.
        
        Returns:
            Profile ID or None if using general profile
        """
        with self._lock:
            return self._config.get('selected_profile_id')
    
    def set_selected_profile(self, profile_id: Optional[int], profile_name: Optional[str] = None) -> bool:
        """Set the selected profile for Streamflow operations.
        
        Args:
            profile_id: Profile ID to select, or None for general profile
            profile_name: Profile name (optional, for display purposes)
            
        Returns:
            True if successful
        """
        with self._lock:
            self._config['selected_profile_id'] = profile_id
            self._config['selected_profile_name'] = profile_name
            self._config['use_profile'] = profile_id is not None
            logger.info(f"Selected profile set to: {profile_name or 'General'} (ID: {profile_id})")
            return self._save_config()
    
    def get_dead_stream_config(self) -> Dict[str, Any]:
        """Get dead stream management configuration.
        
        Returns:
            Dictionary with dead stream config
        """
        with self._lock:
            return self._config.get('dead_streams', {}).copy()
    
    def set_dead_stream_config(self, enabled: bool = None, target_profile_id: int = None, 
                               target_profile_name: str = None, use_snapshot: bool = None) -> bool:
        """Set dead stream management configuration.
        
        Args:
            enabled: Enable empty channel management
            target_profile_id: Profile ID to disable channels in
            target_profile_name: Profile name (for display)
            use_snapshot: Whether to use snapshot for re-enabling
            
        Returns:
            True if successful
        """
        with self._lock:
            if 'dead_streams' not in self._config:
                self._config['dead_streams'] = {}
            
            if enabled is not None:
                self._config['dead_streams']['enabled'] = enabled
            if target_profile_id is not None:
                self._config['dead_streams']['target_profile_id'] = target_profile_id
            if target_profile_name is not None:
                self._config['dead_streams']['target_profile_name'] = target_profile_name
            if use_snapshot is not None:
                self._config['dead_streams']['use_snapshot'] = use_snapshot
            
            logger.info(f"Dead stream config updated: enabled={enabled}, profile={target_profile_name}, snapshot={use_snapshot}")
            return self._save_config()
    
    def create_snapshot(self, profile_id: int, profile_name: str, channel_ids: List[int]) -> bool:
        """Create a snapshot of channels in a profile.
        
        This snapshot records which channels the user wants in the profile,
        so we can re-enable them after they've been filled back again.
        
        Args:
            profile_id: Profile ID
            profile_name: Profile name
            channel_ids: List of channel IDs currently in the profile
            
        Returns:
            True if successful
        """
        with self._lock:
            if 'snapshots' not in self._config:
                self._config['snapshots'] = {}
            
            snapshot = {
                'profile_id': profile_id,
                'profile_name': profile_name,
                'channel_ids': channel_ids,
                'created_at': datetime.now().isoformat(),
                'channel_count': len(channel_ids)
            }
            
            self._config['snapshots'][str(profile_id)] = snapshot
            logger.info(f"Created snapshot for profile '{profile_name}' with {len(channel_ids)} channels")
            return self._save_config()
    
    def get_snapshot(self, profile_id: int) -> Optional[Dict[str, Any]]:
        """Get snapshot for a profile.
        
        Args:
            profile_id: Profile ID
            
        Returns:
            Snapshot dictionary or None if not found
        """
        with self._lock:
            snapshots = self._config.get('snapshots', {})
            return snapshots.get(str(profile_id))
    
    def has_snapshot(self, profile_id: int) -> bool:
        """Check if a snapshot exists for a profile.
        
        Args:
            profile_id: Profile ID
            
        Returns:
            True if snapshot exists
        """
        with self._lock:
            snapshots = self._config.get('snapshots', {})
            return str(profile_id) in snapshots
    
    def delete_snapshot(self, profile_id: int) -> bool:
        """Delete snapshot for a profile.
        
        Args:
            profile_id: Profile ID
            
        Returns:
            True if successful
        """
        with self._lock:
            if 'snapshots' not in self._config:
                return True
            
            if str(profile_id) in self._config['snapshots']:
                del self._config['snapshots'][str(profile_id)]
                logger.info(f"Deleted snapshot for profile ID {profile_id}")
                return self._save_config()
            return True
    
    def get_all_snapshots(self) -> Dict[str, Dict[str, Any]]:
        """Get all snapshots.
        
        Returns:
            Dictionary mapping profile_id to snapshot data
        """
        with self._lock:
            return self._config.get('snapshots', {}).copy()
    
    def get_config(self) -> Dict[str, Any]:
        """Get complete profile configuration.
        
        Returns:
            Complete configuration dictionary
        """
        with self._lock:
            return self._config.copy()
    
    def is_using_profile(self) -> bool:
        """Check if Streamflow is configured to use a specific profile.
        
        Returns:
            True if using a specific profile, False if using general
        """
        with self._lock:
            return self._config.get('use_profile', False)
    
    def get_target_profile_for_dead_streams(self) -> Optional[int]:
        """Get the target profile ID for dead stream management.
        
        Returns:
            Profile ID or None
        """
        with self._lock:
            dead_config = self._config.get('dead_streams', {})
            return dead_config.get('target_profile_id')
    
    def is_dead_stream_management_enabled(self) -> bool:
        """Check if dead stream management is enabled.
        
        Returns:
            True if enabled
        """
        with self._lock:
            dead_config = self._config.get('dead_streams', {})
            return dead_config.get('enabled', False)


# Global singleton instance
_profile_config: Optional[ProfileConfig] = None
_config_lock = threading.Lock()


def get_profile_config() -> ProfileConfig:
    """Get the global profile configuration singleton instance.
    
    Returns:
        The profile configuration instance
    """
    global _profile_config
    with _config_lock:
        if _profile_config is None:
            _profile_config = ProfileConfig()
        return _profile_config
