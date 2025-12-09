"""
Scheduling Service for EPG-based channel checks.

This service manages scheduled channel checks based on EPG program data.
It fetches EPG data from Dispatcharr, caches it, and manages scheduled events.
"""

import json
import os
import uuid
import requests
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from logging_config import setup_logging
from udi import get_udi_manager

logger = setup_logging(__name__)

# Configuration
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
SCHEDULING_CONFIG_FILE = CONFIG_DIR / 'scheduling_config.json'
SCHEDULED_EVENTS_FILE = CONFIG_DIR / 'scheduled_events.json'


class SchedulingService:
    """
    Service for managing EPG-based scheduled channel checks.
    """
    
    def __init__(self):
        """Initialize the scheduling service."""
        self._lock = threading.Lock()
        self._epg_cache: List[Dict[str, Any]] = []
        self._epg_cache_time: Optional[datetime] = None
        self._config = self._load_config()
        self._scheduled_events = self._load_scheduled_events()
        logger.info("Scheduling service initialized")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load scheduling configuration from file.
        
        Returns:
            Configuration dictionary
        """
        default_config = {
            'epg_refresh_interval_minutes': 60,  # Default 1 hour
            'enabled': True
        }
        
        try:
            if SCHEDULING_CONFIG_FILE.exists():
                with open(SCHEDULING_CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    logger.info(f"Loaded scheduling config: {config}")
                    return config
        except Exception as e:
            logger.error(f"Error loading scheduling config: {e}")
        
        return default_config
    
    def _save_config(self) -> bool:
        """Save scheduling configuration to file.
        
        Returns:
            True if successful
        """
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            with open(SCHEDULING_CONFIG_FILE, 'w') as f:
                json.dump(self._config, f, indent=2)
            logger.info("Saved scheduling config")
            return True
        except Exception as e:
            logger.error(f"Error saving scheduling config: {e}")
            return False
    
    def _load_scheduled_events(self) -> List[Dict[str, Any]]:
        """Load scheduled events from file.
        
        Returns:
            List of scheduled event dictionaries
        """
        try:
            if SCHEDULED_EVENTS_FILE.exists():
                with open(SCHEDULED_EVENTS_FILE, 'r') as f:
                    events = json.load(f)
                    logger.info(f"Loaded {len(events)} scheduled events")
                    return events
        except Exception as e:
            logger.error(f"Error loading scheduled events: {e}")
        
        return []
    
    def _save_scheduled_events(self) -> bool:
        """Save scheduled events to file.
        
        Returns:
            True if successful
        """
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            with open(SCHEDULED_EVENTS_FILE, 'w') as f:
                json.dump(self._scheduled_events, f, indent=2)
            logger.info(f"Saved {len(self._scheduled_events)} scheduled events")
            return True
        except Exception as e:
            logger.error(f"Error saving scheduled events: {e}")
            return False
    
    def get_config(self) -> Dict[str, Any]:
        """Get scheduling configuration.
        
        Returns:
            Configuration dictionary
        """
        return self._config.copy()
    
    def update_config(self, config: Dict[str, Any]) -> bool:
        """Update scheduling configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            True if successful
        """
        with self._lock:
            self._config.update(config)
            return self._save_config()
    
    def _get_base_url(self) -> Optional[str]:
        """Get Dispatcharr base URL from environment.
        
        Returns:
            Base URL or None
        """
        return os.getenv("DISPATCHARR_BASE_URL")
    
    def _get_auth_token(self) -> Optional[str]:
        """Get authentication token from environment.
        
        Returns:
            Auth token or None
        """
        return os.getenv("DISPATCHARR_TOKEN")
    
    def fetch_epg_grid(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch EPG grid data from Dispatcharr API with caching.
        
        Args:
            force_refresh: If True, bypass cache and fetch fresh data
            
        Returns:
            List of program dictionaries
        """
        with self._lock:
            # Check cache
            if not force_refresh and self._epg_cache and self._epg_cache_time:
                cache_age = datetime.now() - self._epg_cache_time
                refresh_interval = timedelta(minutes=self._config.get('epg_refresh_interval_minutes', 60))
                
                if cache_age < refresh_interval:
                    logger.debug(f"Returning cached EPG data (age: {cache_age})")
                    return self._epg_cache.copy()
            
            # Fetch fresh data
            base_url = self._get_base_url()
            token = self._get_auth_token()
            
            if not base_url or not token:
                logger.error("Missing Dispatcharr configuration")
                return []
            
            try:
                url = f"{base_url}/api/epg/grid/"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json"
                }
                
                logger.info(f"Fetching EPG grid data from {url}")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Handle different response formats from Dispatcharr API
                # According to swagger, it should be an array, but handle edge cases
                if isinstance(data, list):
                    programs = data
                elif isinstance(data, dict):
                    # If wrapped in an object, try to extract the array
                    # Common keys: results, data, programs
                    programs = data.get('results', data.get('data', data.get('programs', [])))
                    if not isinstance(programs, list):
                        logger.error(f"Unexpected EPG grid response format: {type(data)}, keys: {data.keys() if isinstance(data, dict) else 'N/A'}")
                        programs = []
                else:
                    logger.error(f"Unexpected EPG grid response type: {type(data)}")
                    programs = []
                
                # Validate that programs is a list of dictionaries
                if programs:
                    # Filter out any non-dict items
                    valid_programs = [p for p in programs if isinstance(p, dict)]
                    if len(valid_programs) != len(programs):
                        logger.warning(f"Filtered out {len(programs) - len(valid_programs)} invalid program entries")
                    programs = valid_programs
                
                logger.info(f"Fetched {len(programs)} programs from EPG grid")
                
                # Update cache
                self._epg_cache = programs
                self._epg_cache_time = datetime.now()
                
                return programs.copy()
                
            except Exception as e:
                logger.error(f"Error fetching EPG grid: {e}")
                # Return cached data if available, even if stale
                if self._epg_cache:
                    logger.warning("Returning stale cached EPG data due to fetch error")
                    return self._epg_cache.copy()
                return []
    
    def get_programs_by_channel(self, channel_id: int, tvg_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get programs for a specific channel from cached EPG data.
        
        Args:
            channel_id: Channel ID
            tvg_id: Optional TVG ID for filtering
            
        Returns:
            List of program dictionaries
        """
        # Get EPG data (from cache or fetch if needed)
        programs = self.fetch_epg_grid()
        
        if not programs:
            return []
        
        # Get channel from UDI to get its tvg_id if not provided
        if not tvg_id:
            udi = get_udi_manager()
            channel = udi.get_channel_by_id(channel_id)
            if channel:
                tvg_id = channel.get('tvg_id')
        
        if not tvg_id:
            logger.warning(f"No TVG ID found for channel {channel_id}")
            return []
        
        # Filter programs by tvg_id - ensure we only process dictionaries
        channel_programs = [p for p in programs if isinstance(p, dict) and p.get('tvg_id') == tvg_id]
        
        # Sort by start time
        channel_programs.sort(key=lambda p: p.get('start_time', ''))
        
        logger.debug(f"Found {len(channel_programs)} programs for channel {channel_id} (tvg_id: {tvg_id})")
        return channel_programs
    
    def get_scheduled_events(self) -> List[Dict[str, Any]]:
        """Get all scheduled events.
        
        Returns:
            List of scheduled event dictionaries
        """
        return self._scheduled_events.copy()
    
    def create_scheduled_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new scheduled event.
        
        Args:
            event_data: Event data dictionary containing:
                - channel_id: Channel ID
                - program_start_time: Program start time (ISO format)
                - program_end_time: Program end time (ISO format)
                - program_title: Program title
                - minutes_before: Minutes before program start to check
                
        Returns:
            Created event dictionary
        """
        with self._lock:
            # Generate unique ID
            event_id = str(uuid.uuid4())
            
            # Get channel info
            udi = get_udi_manager()
            channel = udi.get_channel_by_id(event_data['channel_id'])
            if not channel:
                raise ValueError(f"Channel {event_data['channel_id']} not found")
            
            # Calculate check time
            program_start = datetime.fromisoformat(event_data['program_start_time'].replace('Z', '+00:00'))
            minutes_before = event_data.get('minutes_before', 0)
            check_time = program_start - timedelta(minutes=minutes_before)
            
            # Get channel logo info
            logo_id = channel.get('logo_id')
            logo_url = None
            if logo_id:
                logo = udi.get_logo_by_id(logo_id)
                if logo:
                    logo_url = logo.get('cache_url') or logo.get('url')
            
            # Create event
            event = {
                'id': event_id,
                'channel_id': event_data['channel_id'],
                'channel_name': channel.get('name', ''),
                'channel_logo_url': logo_url,
                'program_title': event_data['program_title'],
                'program_start_time': event_data['program_start_time'],
                'program_end_time': event_data['program_end_time'],
                'minutes_before': minutes_before,
                'check_time': check_time.isoformat(),
                'tvg_id': channel.get('tvg_id'),
                'created_at': datetime.now().isoformat()
            }
            
            self._scheduled_events.append(event)
            self._save_scheduled_events()
            
            logger.info(f"Created scheduled event {event_id} for channel {channel.get('name')} at {check_time}")
            return event
    
    def delete_scheduled_event(self, event_id: str) -> bool:
        """Delete a scheduled event.
        
        Args:
            event_id: Event ID
            
        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            initial_count = len(self._scheduled_events)
            self._scheduled_events = [e for e in self._scheduled_events if e.get('id') != event_id]
            
            if len(self._scheduled_events) < initial_count:
                self._save_scheduled_events()
                logger.info(f"Deleted scheduled event {event_id}")
                return True
            
            logger.warning(f"Scheduled event {event_id} not found")
            return False
    
    def get_due_events(self) -> List[Dict[str, Any]]:
        """Get all events that are due for execution.
        
        Returns:
            List of events where check_time is in the past or now
        """
        now = datetime.now()
        due_events = []
        
        for event in self._scheduled_events:
            try:
                check_time = datetime.fromisoformat(event['check_time'].replace('Z', '+00:00'))
                # Remove timezone info for comparison if present
                if check_time.tzinfo:
                    check_time = check_time.replace(tzinfo=None)
                if check_time <= now:
                    due_events.append(event)
            except (ValueError, KeyError) as e:
                logger.warning(f"Invalid check_time for event {event.get('id')}: {e}")
        
        return due_events
    
    def execute_scheduled_check(self, event_id: str, stream_checker_service) -> bool:
        """Execute a scheduled channel check and remove the event.
        
        Args:
            event_id: Event ID to execute
            stream_checker_service: Stream checker service instance
            
        Returns:
            True if executed successfully, False otherwise
        """
        # First, find and extract event data while holding the lock
        with self._lock:
            event = None
            for e in self._scheduled_events:
                if e.get('id') == event_id:
                    event = e
                    break
            
            if not event:
                logger.warning(f"Scheduled event {event_id} not found for execution")
                return False
            
            # Extract event data
            channel_id = event.get('channel_id')
            program_title = event.get('program_title', 'Unknown Program')
        
        # Release lock before executing the long-running channel check
        logger.info(f"Executing scheduled check for channel {channel_id} (program: {program_title})")
        
        try:
            # Execute the check with program context (without holding the lock)
            result = stream_checker_service.check_single_channel(
                channel_id, 
                program_name=program_title
            )
            
            if result.get('success'):
                # Re-acquire lock only to delete the event after successful execution
                with self._lock:
                    # Remove the event and check if it was actually present
                    initial_count = len(self._scheduled_events)
                    self._scheduled_events = [e for e in self._scheduled_events if e.get('id') != event_id]
                    
                    if len(self._scheduled_events) < initial_count:
                        self._save_scheduled_events()
                        logger.info(f"Scheduled event {event_id} executed and removed successfully")
                    else:
                        logger.warning(f"Scheduled event {event_id} was already removed by another thread")
                return True
            else:
                logger.error(f"Scheduled check for event {event_id} failed: {result.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing scheduled event {event_id}: {e}", exc_info=True)
            return False


# Global singleton instance
_scheduling_service: Optional[SchedulingService] = None
_scheduling_lock = threading.Lock()


def get_scheduling_service() -> SchedulingService:
    """Get the global scheduling service singleton instance.
    
    Returns:
        The scheduling service instance
    """
    global _scheduling_service
    with _scheduling_lock:
        if _scheduling_service is None:
            _scheduling_service = SchedulingService()
        return _scheduling_service
