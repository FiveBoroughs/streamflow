#!/usr/bin/env python3
"""
Event Time Ordering Service for StreamFlow.

Automatically reorders streams within channels based on event start times
parsed from stream names. Live/upcoming events get prioritized over past ones.
Supports per-channel regex patterns, timezone config, and overflow channel management.
"""

import json
import os
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests

from logging_config import setup_logging
from api_utils import fetch_channel_streams, update_channel_streams, _get_base_url, _get_auth_headers, patch_request

logger = setup_logging(__name__)

# Configuration directory
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
EVENT_ORDERING_CONFIG_FILE = CONFIG_DIR / 'event_ordering_config.json'

# Month name mappings for text month parsing
MONTH_NAMES = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}

# Day name mappings (used for validation, not date calculation)
DAY_NAMES = {
    'mon': 0, 'monday': 0,
    'tue': 1, 'tuesday': 1,
    'wed': 2, 'wednesday': 2,
    'thu': 3, 'thursday': 3,
    'fri': 4, 'friday': 4,
    'sat': 5, 'saturday': 5,
    'sun': 6, 'sunday': 6,
}

DEFAULT_CONFIG = {
    'enabled': False,
    'frequency': 300,
    'channels': {}
}


def _js_to_python_regex(pattern: str) -> str:
    """Convert JS-style named groups (?<name>) to Python (?P<name>)."""
    return re.sub(r'\(\?<([^>]+)>', r'(?P<\1>', pattern)


def _parse_month(value: str) -> Optional[int]:
    """Parse a month value, supporting both numeric and text formats."""
    if value is None:
        return None
    # Try numeric first
    try:
        month = int(value)
        if 1 <= month <= 12:
            return month
        return None
    except (ValueError, TypeError):
        pass
    # Try text month name
    return MONTH_NAMES.get(value.lower().strip())


class EventOrderingService:
    """Service for ordering streams by event time within channels."""

    def __init__(self):
        self._lock = threading.Lock()
        self._config: Dict[str, Any] = {}
        self._last_run_results: Dict[str, Any] = {}
        self._load_config()
        logger.info("Event ordering service initialized")

    def _load_config(self) -> None:
        """Load config from file."""
        try:
            if EVENT_ORDERING_CONFIG_FILE.exists():
                with open(EVENT_ORDERING_CONFIG_FILE, 'r') as f:
                    self._config = json.load(f)
                logger.info(f"Loaded event ordering config with {len(self._config.get('channels', {}))} channels")
            else:
                self._config = DEFAULT_CONFIG.copy()
                logger.info("No event ordering config file, using defaults")
        except Exception as e:
            logger.error(f"Error loading event ordering config: {e}", exc_info=True)
            self._config = DEFAULT_CONFIG.copy()

    def _save_config(self) -> bool:
        """Save config to file."""
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            with open(EVENT_ORDERING_CONFIG_FILE, 'w') as f:
                json.dump(self._config, f, indent=2)
            logger.debug("Event ordering config saved")
            return True
        except Exception as e:
            logger.error(f"Error saving event ordering config: {e}", exc_info=True)
            return False

    def get_config(self) -> Dict[str, Any]:
        """Get current config."""
        with self._lock:
            return self._config.copy()

    def update_config(self, new_config: Dict[str, Any]) -> bool:
        """Update config and save."""
        with self._lock:
            self._config.update(new_config)
            success = self._save_config()
            if success:
                logger.info("Event ordering config updated")
            return success

    def is_enabled(self) -> bool:
        """Check if event ordering is globally enabled."""
        return self._config.get('enabled', False)

    def is_channel_enabled(self, channel_id: int) -> bool:
        """Check if a specific channel has event ordering configured."""
        if not self.is_enabled():
            return False
        channels = self._config.get('channels', {})
        return str(channel_id) in channels

    def get_frequency(self) -> int:
        """Get the ordering cycle frequency in seconds."""
        return self._config.get('frequency', 300)

    def get_last_run_results(self) -> Dict[str, Any]:
        """Get results from the last ordering cycle."""
        with self._lock:
            return self._last_run_results.copy()

    def parse_event_time(
        self,
        stream_name: str,
        pattern: str,
        timezone_str: str = ''
    ) -> Optional[datetime]:
        """Parse event time from a stream name using a regex pattern.

        The pattern should contain named groups for time components:
        year, month, day, hour, minute, second, ampm, order.
        Supports alternate group names (day2, month2, hour2, minute2) for
        multi-format patterns.

        Args:
            stream_name: The stream name to parse
            pattern: Regex pattern with named groups (JS or Python style)
            timezone_str: IANA timezone string (e.g. 'Europe/London')

        Returns:
            Parsed datetime or None if parsing fails
        """
        try:
            python_pattern = _js_to_python_regex(pattern)
            match = re.search(python_pattern, stream_name)
            if not match:
                return None

            groups = match.groupdict()

            # Extract time components, supporting alternate group names
            year_str = groups.get('year')
            month_str = groups.get('month') or groups.get('month2')
            # Check 'date'/'date2' before 'day2' since day2 might be a day name like "Sat"
            day_str = groups.get('day') or groups.get('date') or groups.get('date2') or groups.get('day2')
            hour_str = groups.get('hour') or groups.get('hour2')
            minute_str = groups.get('minute') or groups.get('minute2')
            second_str = groups.get('second')
            ampm = groups.get('ampm')

            if hour_str is None or minute_str is None:
                return None

            hour = int(hour_str)
            minute = int(minute_str)
            second = int(second_str) if second_str else 0

            # Handle AM/PM
            if ampm:
                ampm_upper = ampm.upper()
                if ampm_upper == 'PM' and hour != 12:
                    hour += 12
                elif ampm_upper == 'AM' and hour == 12:
                    hour = 0

            now = datetime.now()

            # Parse year
            year = int(year_str) if year_str else now.year

            # Parse month
            month = _parse_month(month_str) if month_str else now.month

            # Parse day - could be a day name or a date number
            day = None
            if day_str:
                try:
                    day = int(day_str)
                except (ValueError, TypeError):
                    # It's a day name like "Monday" - not useful for date, skip
                    pass

            if day is None:
                day = now.day

            # Handle timezone offset
            tz_offset = timedelta(0)
            if timezone_str:
                try:
                    import zoneinfo
                    tz = zoneinfo.ZoneInfo(timezone_str)
                    # Get current UTC offset for this timezone
                    tz_now = datetime.now(tz)
                    local_now = datetime.now()
                    tz_offset = tz_now.utcoffset() - (local_now - datetime.utcnow())
                except Exception:
                    pass  # Fall back to no offset

            try:
                event_time = datetime(year, month, day, hour, minute, second)
            except ValueError:
                return None

            return event_time

        except re.error as e:
            logger.warning(f"Invalid regex pattern: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing event time from '{stream_name}': {e}")
            return None

    def categorize_streams(
        self,
        streams: List[Dict[str, Any]],
        pattern: str,
        timezone_str: str = '',
        grace_hours: float = 2.0
    ) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Categorize streams into upcoming, past, and unparseable.

        Args:
            streams: List of stream dicts (must have 'id' and 'name')
            pattern: Regex pattern for time extraction
            timezone_str: IANA timezone string
            grace_hours: Hours after start to still consider "upcoming"

        Returns:
            Tuple of (upcoming, past, unparseable) stream lists.
            Each item is a dict with stream info plus 'event_time' and 'order'.
        """
        now = datetime.now()
        grace_cutoff = now - timedelta(hours=grace_hours)

        python_pattern = _js_to_python_regex(pattern)
        upcoming = []
        past = []
        unparseable = []

        for stream in streams:
            stream_name = stream.get('name', '')
            stream_id = stream.get('id')

            event_time = self.parse_event_time(stream_name, pattern, timezone_str)

            # Extract order number if present
            order = None
            try:
                match = re.search(python_pattern, stream_name)
                if match:
                    order_str = match.groupdict().get('order')
                    if order_str:
                        order = int(order_str)
            except Exception:
                pass

            entry = {
                'stream_id': stream_id,
                'stream_name': stream_name,
                'event_time': event_time,
                'order': order,
            }

            if event_time is None:
                unparseable.append(entry)
            elif event_time >= grace_cutoff:
                upcoming.append(entry)
            else:
                past.append(entry)

        return upcoming, past, unparseable

    def sort_streams(
        self,
        upcoming: List[Dict],
        past: List[Dict],
        unparseable: List[Dict]
    ) -> List[int]:
        """Sort streams: upcoming first (by time), then past (by time desc), then unparseable.

        Returns:
            Ordered list of stream IDs
        """
        # Sort upcoming by event time (soonest first), then by order number
        upcoming.sort(key=lambda x: (
            x['event_time'] or datetime.max,
            x['order'] if x['order'] is not None else 9999
        ))

        # Sort past by event time descending (most recent first)
        past.sort(key=lambda x: (
            x['event_time'] or datetime.min,
        ), reverse=True)

        # Combine: upcoming first, then past, then unparseable
        ordered = upcoming + past + unparseable

        return [s['stream_id'] for s in ordered if s['stream_id'] is not None]

    def preview_channel(self, channel_id: int) -> Dict[str, Any]:
        """Preview ordering for a channel without applying changes.

        Returns:
            Dict with upcoming/past/unparseable counts and proposed order
        """
        channel_id_str = str(channel_id)
        channels_config = self._config.get('channels', {})

        if channel_id_str not in channels_config:
            return {'error': f'Channel {channel_id} not configured for event ordering'}

        ch_config = channels_config[channel_id_str]
        pattern = ch_config.get('pattern', '')
        timezone_str = ch_config.get('stream_timezone', '')
        grace_hours = ch_config.get('return_after_hours', 6)

        streams = fetch_channel_streams(channel_id)
        if streams is None:
            return {'error': f'Could not fetch streams for channel {channel_id}'}

        upcoming, past, unparseable = self.categorize_streams(
            streams, pattern, timezone_str, grace_hours
        )
        ordered_ids = self.sort_streams(upcoming, past, unparseable)

        # Build detailed results
        current_order = [s.get('id') for s in streams]

        return {
            'channel_id': channel_id,
            'channel_name': ch_config.get('name', f'Channel {channel_id}'),
            'total_streams': len(streams),
            'upcoming': [
                {
                    'stream_id': s['stream_id'],
                    'stream_name': s['stream_name'],
                    'event_time': s['event_time'].isoformat() if s['event_time'] else None,
                    'order': s['order']
                }
                for s in upcoming
            ],
            'past': [
                {
                    'stream_id': s['stream_id'],
                    'stream_name': s['stream_name'],
                    'event_time': s['event_time'].isoformat() if s['event_time'] else None,
                    'order': s['order']
                }
                for s in past
            ],
            'unparseable': [
                {
                    'stream_id': s['stream_id'],
                    'stream_name': s['stream_name'],
                }
                for s in unparseable
            ],
            'proposed_order': ordered_ids,
            'current_order': current_order,
            'order_changed': ordered_ids != current_order,
        }

    def reorder_channel(self, channel_id: int) -> Dict[str, Any]:
        """Apply event-time ordering to a channel.

        Returns:
            Dict with result info
        """
        channel_id_str = str(channel_id)
        channels_config = self._config.get('channels', {})

        if channel_id_str not in channels_config:
            return {'success': False, 'error': f'Channel {channel_id} not configured'}

        ch_config = channels_config[channel_id_str]
        pattern = ch_config.get('pattern', '')
        timezone_str = ch_config.get('stream_timezone', '')
        grace_hours = ch_config.get('return_after_hours', 6)

        streams = fetch_channel_streams(channel_id)
        if streams is None:
            return {'success': False, 'error': f'Could not fetch streams for channel {channel_id}'}

        if not streams:
            return {'success': True, 'message': 'No streams to reorder', 'reordered': False}

        upcoming, past, unparseable = self.categorize_streams(
            streams, pattern, timezone_str, grace_hours
        )
        ordered_ids = self.sort_streams(upcoming, past, unparseable)

        current_order = [s.get('id') for s in streams]
        if ordered_ids == current_order:
            logger.info(f"Channel {channel_id} already in correct event-time order")
            return {'success': True, 'reordered': False, 'message': 'Already in correct order'}

        success = update_channel_streams(channel_id, ordered_ids, allow_dead_streams=True)

        result = {
            'success': success,
            'reordered': success,
            'upcoming_count': len(upcoming),
            'past_count': len(past),
            'unparseable_count': len(unparseable),
        }

        if success:
            logger.info(
                f"Reordered channel {channel_id}: "
                f"{len(upcoming)} upcoming, {len(past)} past, {len(unparseable)} unparseable"
            )
        else:
            logger.error(f"Failed to reorder channel {channel_id}")

        return result

    def handle_overflow(self, channel_id: int) -> Dict[str, Any]:
        """Move conflicting/past streams to overflow channels.

        When multiple events share the same time slot, overflow channels
        are used to hold the extra streams.

        Returns:
            Dict with overflow result info
        """
        channel_id_str = str(channel_id)
        channels_config = self._config.get('channels', {})

        if channel_id_str not in channels_config:
            return {'success': False, 'error': f'Channel {channel_id} not configured'}

        ch_config = channels_config[channel_id_str]
        overflow_ids = ch_config.get('overflow_channel_ids', [])

        if not overflow_ids:
            return {'success': True, 'message': 'No overflow channels configured'}

        pattern = ch_config.get('pattern', '')
        timezone_str = ch_config.get('stream_timezone', '')
        grace_hours = ch_config.get('return_after_hours', 6)

        streams = fetch_channel_streams(channel_id)
        if streams is None:
            return {'success': False, 'error': f'Could not fetch streams for channel {channel_id}'}

        upcoming, past, unparseable = self.categorize_streams(
            streams, pattern, timezone_str, grace_hours
        )

        # Group upcoming streams by time slot (same hour)
        time_slots: Dict[str, List[Dict]] = {}
        for s in upcoming:
            if s['event_time']:
                slot_key = s['event_time'].strftime('%Y-%m-%d %H:00')
                time_slots.setdefault(slot_key, []).append(s)

        # Find overflow streams: when multiple streams share a time slot,
        # keep the first (by order number) and overflow the rest
        overflow_streams = []
        for slot_key, slot_streams in time_slots.items():
            if len(slot_streams) > 1:
                slot_streams.sort(key=lambda x: x['order'] if x['order'] is not None else 9999)
                overflow_streams.extend(slot_streams[1:])  # All but the first

        if not overflow_streams:
            return {'success': True, 'message': 'No streams need overflow', 'moved': 0}

        # Distribute overflow streams across overflow channels
        moved = 0
        for i, stream in enumerate(overflow_streams):
            if i >= len(overflow_ids):
                logger.warning(f"Not enough overflow channels for channel {channel_id}")
                break

            target_channel = overflow_ids[i % len(overflow_ids)]
            target_streams = fetch_channel_streams(target_channel)
            if target_streams is None:
                target_streams = []

            # Add stream to overflow channel
            target_stream_ids = [s.get('id') for s in target_streams]
            if stream['stream_id'] not in target_stream_ids:
                target_stream_ids.append(stream['stream_id'])
                if update_channel_streams(target_channel, target_stream_ids, allow_dead_streams=True):
                    moved += 1
                    logger.info(
                        f"Moved stream {stream['stream_id']} ({stream['stream_name']}) "
                        f"to overflow channel {target_channel}"
                    )

        return {'success': True, 'moved': moved, 'total_overflow': len(overflow_streams)}

    def format_channel_name(
        self,
        template: str,
        stream_name: str,
        pattern: str,
        base_name: str,
        timezone_str: str = '',
        display_timezone: str = ''
    ) -> str:
        """Format a channel name using a template and regex groups from the top stream.

        Template variables:
        - {base_name}: Original channel name from config
        - {event_name}: Full stream name
        - {event_time}: Formatted time (HH:MM) in display timezone
        - {event_date}: Formatted date in display timezone
        - {timezone}: Timezone abbreviation
        - Any named regex group: {league}, {hour}, {minute}, {ampm}, {team1}, {team2}, etc.

        Args:
            template: Template string, e.g. "{base_name} - {team1} vs {team2} {event_time} {timezone}"
            stream_name: Name of the top/current stream
            pattern: Regex pattern with named groups
            base_name: The original channel display name
            timezone_str: IANA timezone for parsing stream times
            display_timezone: IANA timezone for displaying times (defaults to timezone_str)

        Returns:
            Formatted channel name
        """
        try:
            python_pattern = _js_to_python_regex(pattern)
            match = re.search(python_pattern, stream_name)
            groups = match.groupdict() if match else {}

            # Build variables dict
            variables = {
                'base_name': base_name,
                'event_name': stream_name,
            }
            variables.update(groups)

            # Add formatted time if available
            event_time = self.parse_event_time(stream_name, pattern, timezone_str)
            if event_time:
                import pytz
                display_tz = display_timezone or timezone_str
                if display_tz:
                    try:
                        tz = pytz.timezone(display_tz)
                        localized = event_time.astimezone(tz) if event_time.tzinfo else tz.localize(event_time)
                        variables['event_time'] = localized.strftime('%H:%M')
                        variables['event_date'] = localized.strftime('%Y-%m-%d')
                        variables['timezone'] = localized.strftime('%Z')
                    except Exception:
                        variables['event_time'] = event_time.strftime('%H:%M')
                        variables['event_date'] = event_time.strftime('%Y-%m-%d')
                        variables['timezone'] = ''
                else:
                    variables['event_time'] = event_time.strftime('%H:%M')
                    variables['event_date'] = event_time.strftime('%Y-%m-%d')
                    variables['timezone'] = ''

            # Use safe substitution so missing keys don't crash
            from string import Template as StrTemplate
            # Convert {var} style to $var for string.Template
            safe_template = re.sub(r'\{(\w+)\}', r'${\1}', template)
            result = StrTemplate(safe_template).safe_substitute(variables)

            return result
        except Exception as e:
            logger.warning(f"Error formatting channel name: {e}")
            return base_name

    def rename_channel(self, channel_id: int, new_name: str) -> bool:
        """Rename a channel in Dispatcharr via PATCH API.

        Args:
            channel_id: The Dispatcharr channel ID
            new_name: New channel name

        Returns:
            True if successful
        """
        try:
            base_url = _get_base_url()
            if not base_url:
                logger.error("Cannot rename channel: Dispatcharr base URL not configured")
                return False

            url = f"{base_url}/api/channels/channels/{channel_id}/"
            # Use patch_request which has 401 retry logic
            resp = patch_request(url, {'name': new_name})

            if resp and resp.status_code in [200, 204]:
                logger.info(f"Renamed channel {channel_id} to '{new_name}'")
                return True
            else:
                status = resp.status_code if resp else 'No response'
                text = resp.text[:200] if resp else 'N/A'
                logger.warning(
                    f"Failed to rename channel {channel_id}: "
                    f"{status} - {text}"
                )
                return False
        except Exception as e:
            logger.error(f"Error renaming channel {channel_id}: {e}")
            return False

    def test_pattern(
        self,
        pattern: str,
        stream_names: List[str],
        timezone_str: str = ''
    ) -> List[Dict[str, Any]]:
        """Test a regex pattern against a list of stream names.

        Returns:
            List of dicts with stream_name, matched, event_time, groups
        """
        results = []
        python_pattern = _js_to_python_regex(pattern)

        for name in stream_names:
            try:
                match = re.search(python_pattern, name)
                if match:
                    groups = match.groupdict()
                    event_time = self.parse_event_time(name, pattern, timezone_str)
                    results.append({
                        'stream_name': name,
                        'matched': True,
                        'event_time': event_time.isoformat() if event_time else None,
                        'groups': groups,
                    })
                else:
                    results.append({
                        'stream_name': name,
                        'matched': False,
                        'event_time': None,
                        'groups': {},
                    })
            except re.error as e:
                results.append({
                    'stream_name': name,
                    'matched': False,
                    'event_time': None,
                    'groups': {},
                    'error': str(e),
                })

        return results

    def run_ordering_cycle(self) -> Dict[str, Any]:
        """Run a full ordering cycle across all configured channels.

        Returns:
            Dict with per-channel results
        """
        if not self.is_enabled():
            return {'success': False, 'error': 'Event ordering is disabled'}

        channels_config = self._config.get('channels', {})
        if not channels_config:
            return {'success': True, 'message': 'No channels configured', 'results': {}}

        results = {}
        for channel_id_str, ch_config in channels_config.items():
            channel_id = int(channel_id_str)
            channel_name = ch_config.get('name', f'Channel {channel_id}')

            try:
                logger.info(f"Processing event ordering for {channel_name} (ID: {channel_id})")
                result = self.reorder_channel(channel_id)
                results[channel_id_str] = result

                # Handle overflow if configured
                overflow_ids = ch_config.get('overflow_channel_ids', [])
                if overflow_ids and result.get('success'):
                    overflow_result = self.handle_overflow(channel_id)
                    results[channel_id_str]['overflow'] = overflow_result

                # Handle channel renaming if enabled and template is configured
                name_template = ch_config.get('channel_name_template', '')
                renaming_enabled = ch_config.get('channel_renaming_enabled', bool(name_template))
                if renaming_enabled and name_template and result.get('success'):
                    pattern = ch_config.get('pattern', '')
                    timezone_str = ch_config.get('stream_timezone', '')
                    display_tz = ch_config.get('display_timezone', '')
                    # Get the top stream (first upcoming or first overall)
                    streams = fetch_channel_streams(channel_id)
                    if streams:
                        top_stream_name = streams[0].get('name', '')
                        new_name = self.format_channel_name(
                            name_template, top_stream_name, pattern,
                            channel_name, timezone_str, display_tz
                        )
                        if new_name and new_name != channel_name:
                            renamed = self.rename_channel(channel_id, new_name)
                            results[channel_id_str]['renamed'] = renamed
                            results[channel_id_str]['new_channel_name'] = new_name if renamed else None

            except Exception as e:
                logger.error(f"Error processing channel {channel_id}: {e}", exc_info=True)
                results[channel_id_str] = {'success': False, 'error': str(e)}

        cycle_result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'channels_processed': len(results),
            'results': results,
        }

        with self._lock:
            self._last_run_results = cycle_result

        return cycle_result


# Global singleton
_event_ordering_service: Optional[EventOrderingService] = None
_service_lock = threading.Lock()


def get_event_ordering_service() -> EventOrderingService:
    """Get the global EventOrderingService singleton."""
    global _event_ordering_service
    with _service_lock:
        if _event_ordering_service is None:
            _event_ordering_service = EventOrderingService()
        return _event_ordering_service
