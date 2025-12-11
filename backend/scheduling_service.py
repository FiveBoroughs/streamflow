"""
Scheduling Service for EPG-based channel checks.

This service manages scheduled channel checks based on EPG program data.
It fetches EPG data from Dispatcharr, caches it, and manages scheduled events.
"""

import json
import os
import re
import uuid
import requests
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

from logging_config import setup_logging
from udi import get_udi_manager

logger = setup_logging(__name__)

# Configuration
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
SCHEDULING_CONFIG_FILE = CONFIG_DIR / 'scheduling_config.json'
SCHEDULED_EVENTS_FILE = CONFIG_DIR / 'scheduled_events.json'
AUTO_CREATE_RULES_FILE = CONFIG_DIR / 'auto_create_rules.json'

# Constants
DUPLICATE_DETECTION_WINDOW_SECONDS = 300  # 5 minutes window for detecting duplicate events


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
        self._auto_create_rules = self._load_auto_create_rules()
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
        programs_copy = []  # Initialize to ensure it's always defined
        
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
                
                # Copy programs before releasing lock
                programs_copy = programs.copy()
                
            except Exception as e:
                logger.error(f"Error fetching EPG grid: {e}")
                # Return cached data if available, even if stale
                if self._epg_cache:
                    logger.warning("Returning stale cached EPG data due to fetch error")
                    programs_copy = self._epg_cache.copy()
                # programs_copy is already initialized to [] at the start
        
        # Match outside the lock to avoid deadlock
        try:
            self.match_programs_to_rules()
        except Exception as e:
            logger.error(f"Error matching programs to rules: {e}", exc_info=True)
        
        return programs_copy
    
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
            
            # Defensive: Ensure check_time is timezone-aware for proper comparison
            # program_start should already be timezone-aware after parsing, so this
            # should only trigger if the input data was malformed
            if check_time.tzinfo is None:
                logger.warning(f"check_time is missing timezone info, assuming UTC: {check_time}")
                check_time = check_time.replace(tzinfo=timezone.utc)
            
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
        now = datetime.now(timezone.utc)
        due_events = []
        
        for event in self._scheduled_events:
            try:
                check_time = datetime.fromisoformat(event['check_time'].replace('Z', '+00:00'))
                # Ensure check_time is timezone-aware
                if check_time.tzinfo is None:
                    check_time = check_time.replace(tzinfo=timezone.utc)
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
    
    def _load_auto_create_rules(self) -> List[Dict[str, Any]]:
        """Load auto-create rules from file.
        
        Returns:
            List of auto-create rule dictionaries
        """
        try:
            if AUTO_CREATE_RULES_FILE.exists():
                with open(AUTO_CREATE_RULES_FILE, 'r') as f:
                    rules = json.load(f)
                    logger.info(f"Loaded {len(rules)} auto-create rules")
                    return rules
        except Exception as e:
            logger.error(f"Error loading auto-create rules: {e}")
        
        return []
    
    def _save_auto_create_rules(self) -> bool:
        """Save auto-create rules to file.
        
        Returns:
            True if successful
        """
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            with open(AUTO_CREATE_RULES_FILE, 'w') as f:
                json.dump(self._auto_create_rules, f, indent=2)
            logger.info(f"Saved {len(self._auto_create_rules)} auto-create rules")
            return True
        except Exception as e:
            logger.error(f"Error saving auto-create rules: {e}")
            return False
    
    def get_auto_create_rules(self) -> List[Dict[str, Any]]:
        """Get all auto-create rules.
        
        Returns:
            List of auto-create rule dictionaries
        """
        return self._auto_create_rules.copy()
    
    def create_auto_create_rule(self, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new auto-create rule.
        
        Args:
            rule_data: Rule data dictionary containing:
                - name: Rule name
                - channel_id: Channel ID to match
                - regex_pattern: Regex pattern to match program names
                - minutes_before: Minutes before program start to check
                
        Returns:
            Created rule dictionary
        """
        with self._lock:
            # Generate unique ID
            rule_id = str(uuid.uuid4())
            
            # Get channel info
            udi = get_udi_manager()
            channel = udi.get_channel_by_id(rule_data['channel_id'])
            if not channel:
                raise ValueError(f"Channel {rule_data['channel_id']} not found")
            
            # Validate regex pattern
            try:
                re.compile(rule_data['regex_pattern'])
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")
            
            # Get channel logo info
            logo_id = channel.get('logo_id')
            logo_url = None
            if logo_id:
                logo = udi.get_logo_by_id(logo_id)
                if logo:
                    logo_url = logo.get('cache_url') or logo.get('url')
            
            # Create rule
            rule = {
                'id': rule_id,
                'name': rule_data['name'],
                'channel_id': rule_data['channel_id'],
                'channel_name': channel.get('name', ''),
                'channel_logo_url': logo_url,
                'tvg_id': channel.get('tvg_id'),
                'regex_pattern': rule_data['regex_pattern'],
                'minutes_before': rule_data.get('minutes_before', 5),
                'created_at': datetime.now().isoformat()
            }
            
            self._auto_create_rules.append(rule)
            self._save_auto_create_rules()
            
            logger.info(f"Created auto-create rule {rule_id} for channel {channel.get('name')}")
            
            # Schedule matching in background thread to avoid blocking
            def match_in_background():
                try:
                    # Ensure EPG data is fetched before matching
                    self.fetch_epg_grid()
                    # Note: fetch_epg_grid already calls match_programs_to_rules at the end
                except Exception as e:
                    logger.error(f"Error matching programs to rules in background: {e}", exc_info=True)
            
            match_thread = threading.Thread(target=match_in_background, daemon=True)
            match_thread.start()
            
            return rule
    
    def delete_auto_create_rule(self, rule_id: str) -> bool:
        """Delete an auto-create rule.
        
        Args:
            rule_id: Rule ID
            
        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            initial_count = len(self._auto_create_rules)
            self._auto_create_rules = [r for r in self._auto_create_rules if r.get('id') != rule_id]
            
            if len(self._auto_create_rules) < initial_count:
                self._save_auto_create_rules()
                logger.info(f"Deleted auto-create rule {rule_id}")
                return True
            
            logger.warning(f"Auto-create rule {rule_id} not found")
            return False
    
    def test_regex_against_epg(self, channel_id: int, regex_pattern: str) -> List[Dict[str, Any]]:
        """Test a regex pattern against EPG programs for a channel.
        
        Args:
            channel_id: Channel ID
            regex_pattern: Regex pattern to test
            
        Returns:
            List of matching programs
        """
        # Validate regex pattern
        try:
            pattern = re.compile(regex_pattern, re.IGNORECASE)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        
        # Get programs for channel
        programs = self.get_programs_by_channel(channel_id)
        
        # Filter matching programs
        matching_programs = []
        for program in programs:
            title = program.get('title', '')
            if pattern.search(title):
                matching_programs.append(program)
        
        logger.debug(f"Regex '{regex_pattern}' matched {len(matching_programs)} programs for channel {channel_id}")
        return matching_programs
    
    def match_programs_to_rules(self) -> Dict[str, Any]:
        """Match EPG programs to auto-create rules and create/update scheduled events.
        
        This method:
        1. Scans all programs in EPG cache
        2. For each rule, finds matching programs
        3. Creates new events for programs not yet scheduled
        4. Updates existing events if program name or time changed
        
        Returns:
            Dictionary with statistics about created/updated events
        """
        # First, get a snapshot of the data we need without holding the lock for long
        with self._lock:
            if not self._auto_create_rules:
                logger.debug("No auto-create rules to process")
                return {'created': 0, 'updated': 0, 'skipped': 0}
            
            # Make copies of the data we need to avoid holding lock during processing
            rules_snapshot = self._auto_create_rules.copy()
            epg_cache_snapshot = self._epg_cache.copy()
        
        # Now process outside the lock
        created_count = 0
        updated_count = 0
        skipped_count = 0
        
        udi = get_udi_manager()
        events_to_add = []
        events_to_update = []
        
        # Pre-group programs by tvg_id for efficient lookup (performance optimization)
        programs_by_tvg_id = defaultdict(list)
        for program in epg_cache_snapshot:
            if isinstance(program, dict):
                tvg_id = program.get('tvg_id')
                if tvg_id:
                    programs_by_tvg_id[tvg_id].append(program)
        
        # Sort programs by start time for each channel
        for tvg_id in programs_by_tvg_id:
            programs_by_tvg_id[tvg_id].sort(key=lambda p: p.get('start_time', ''))
        
        for rule in rules_snapshot:
            channel_id = rule.get('channel_id')
            regex_pattern = rule.get('regex_pattern')
            minutes_before = rule.get('minutes_before', 5)
            tvg_id = rule.get('tvg_id')
            
            if not tvg_id:
                logger.warning(f"Rule {rule.get('id')} has no TVG ID, skipping")
                continue
            
            try:
                pattern = re.compile(regex_pattern, re.IGNORECASE)
            except re.error as e:
                logger.error(f"Invalid regex pattern in rule {rule.get('id')}: {e}")
                continue
            
            # Get programs for this channel from the pre-grouped dictionary
            programs = programs_by_tvg_id.get(tvg_id, [])
            
            for program in programs:
                title = program.get('title', '')
                if not pattern.search(title):
                    continue
                
                # Program matches! Check if we already have an event for it
                program_start = program.get('start_time')
                program_end = program.get('end_time')
                
                if not program_start or not program_end:
                    logger.warning(f"Program missing start/end time: {title}")
                    continue
                
                # Parse times
                try:
                    start_dt = datetime.fromisoformat(program_start.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(program_end.replace('Z', '+00:00'))
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Invalid program times for {title}: {e}")
                    continue
                
                # Ensure timezone aware
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                
                # Get the date of the program (for duplicate detection)
                program_date = start_dt.date().isoformat()
                
                # Create new event data
                check_time = start_dt - timedelta(minutes=minutes_before)
                
                # Get channel info for logo
                channel = udi.get_channel_by_id(channel_id)
                logo_id = channel.get('logo_id') if channel else None
                logo_url = None
                if logo_id:
                    logo = udi.get_logo_by_id(logo_id)
                    if logo:
                        logo_url = logo.get('cache_url') or logo.get('url')
                
                event_data = {
                    'id': str(uuid.uuid4()),
                    'channel_id': channel_id,
                    'channel_name': channel.get('name', '') if channel else '',
                    'channel_logo_url': logo_url,
                    'program_title': title,
                    'program_start_time': program_start,
                    'program_end_time': program_end,
                    'minutes_before': minutes_before,
                    'check_time': check_time.isoformat(),
                    'tvg_id': tvg_id,
                    'created_at': datetime.now().isoformat(),
                    'auto_created': True,
                    'auto_create_rule_id': rule.get('id'),
                    'program_date': program_date  # For duplicate detection
                }
                
                events_to_add.append(event_data)
        
        # Now acquire the lock briefly to update the scheduled events
        with self._lock:
            for event_data in events_to_add:
                # Look for existing event with same channel, program date within detection window
                program_date = event_data.pop('program_date')
                channel_id = event_data['channel_id']
                title = event_data['program_title']
                program_start = event_data['program_start_time']
                
                try:
                    start_dt = datetime.fromisoformat(program_start.replace('Z', '+00:00'))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    continue
                
                existing_event = None
                for event in self._scheduled_events:
                    if event.get('channel_id') != channel_id:
                        continue
                    
                    event_start = event.get('program_start_time', '')
                    try:
                        event_start_dt = datetime.fromisoformat(event_start.replace('Z', '+00:00'))
                        if event_start_dt.tzinfo is None:
                            event_start_dt = event_start_dt.replace(tzinfo=timezone.utc)
                        event_date = event_start_dt.date().isoformat()
                    except (ValueError, AttributeError):
                        continue
                    
                    # Check if same date and within duplicate detection window
                    if event_date == program_date:
                        time_diff = abs((event_start_dt - start_dt).total_seconds())
                        if time_diff < DUPLICATE_DETECTION_WINDOW_SECONDS:
                            existing_event = event
                            break
                
                if existing_event:
                    # Check if we need to update the event
                    needs_update = False
                    if existing_event.get('program_title') != title:
                        logger.info(f"Updating event title: '{existing_event.get('program_title')}' -> '{title}'")
                        existing_event['program_title'] = title
                        needs_update = True
                    
                    if existing_event.get('program_start_time') != program_start:
                        logger.info(f"Updating event time: {existing_event.get('program_start_time')} -> {program_start}")
                        existing_event['program_start_time'] = event_data['program_start_time']
                        existing_event['program_end_time'] = event_data['program_end_time']
                        existing_event['check_time'] = event_data['check_time']
                        needs_update = True
                    
                    if needs_update:
                        updated_count += 1
                    else:
                        skipped_count += 1
                else:
                    # Add new event
                    self._scheduled_events.append(event_data)
                    created_count += 1
                    channel_name = event_data.get('channel_name', channel_id)
                    logger.info(f"Auto-created event for '{title}' on channel {channel_name}")
            
            # Save if we made changes
            if created_count > 0 or updated_count > 0:
                self._save_scheduled_events()
        
        # Return result outside the lock
        result = {
            'created': created_count,
            'updated': updated_count,
            'skipped': skipped_count
        }
        
        if created_count > 0 or updated_count > 0:
            logger.info(f"Auto-create matching complete: {result}")
        
        return result


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
