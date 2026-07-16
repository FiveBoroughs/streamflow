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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pytz
import requests

from apps.core.logging_config import setup_logging
from apps.core.api_utils import (
    fetch_channel_streams,
    update_channel_streams,
    _get_base_url,
    _get_auth_headers,
    patch_request,
)
from event_ordering_parser_schema import validate_parser_v1, ensure_channel_parser_defaults

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


def _localize_to_utc(naive_dt: Optional[datetime], timezone_str: str) -> Optional[datetime]:
    """Convert a naive datetime in the given timezone to a UTC-aware datetime."""
    if naive_dt is None:
        return None
    if not timezone_str:
        # No timezone info — attach UTC so comparisons work
        return naive_dt.replace(tzinfo=pytz.utc)
    try:
        tz = pytz.timezone(timezone_str)
        localized = tz.localize(naive_dt, is_dst=None)
        return localized.astimezone(pytz.utc)
    except Exception:
        return naive_dt.replace(tzinfo=pytz.utc)


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
            # Validate parser schema (if provided) and apply defaults.
            channels = (new_config or {}).get('channels', {})
            if isinstance(channels, dict):
                normalized_channels = {}
                for channel_id, channel_cfg in channels.items():
                    cfg = ensure_channel_parser_defaults(channel_cfg if isinstance(channel_cfg, dict) else {})
                    parser = cfg.get('parser')
                    if parser is not None:
                        ok, err = validate_parser_v1(parser)
                        if not ok:
                            logger.error(f"Invalid parser schema for channel {channel_id}: {err}")
                            return False
                    normalized_channels[channel_id] = cfg
                new_config = {**new_config, 'channels': normalized_channels}

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

    def _parse_event_time_from_groups(self, groups: Dict[str, Any]) -> Optional[datetime]:
        """Parse datetime from already-extracted named groups."""
        try:
            year_str = groups.get('year')
            month_str = groups.get('month') or groups.get('month2')
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

            if ampm:
                ampm_upper = str(ampm).upper()
                if ampm_upper == 'PM' and hour != 12:
                    hour += 12
                elif ampm_upper == 'AM' and hour == 12:
                    hour = 0

            now = datetime.now()
            year = int(year_str) if year_str else now.year
            month = _parse_month(month_str) if month_str else now.month

            day = None
            if day_str:
                try:
                    day = int(day_str)
                except (ValueError, TypeError):
                    pass
            if day is None:
                day = now.day

            return datetime(year, month, day, hour, minute, second)
        except Exception:
            return None

    def _match_with_pattern(self, stream_name: str, pattern: str) -> Optional[Dict[str, Any]]:
        try:
            python_pattern = _js_to_python_regex(pattern)
            m = re.search(python_pattern, stream_name)
            if not m:
                return None
            return m.groupdict() or {}
        except Exception:
            return None

    def _extract_order_from_groups(self, groups: Dict[str, Any]) -> Optional[int]:
        try:
            order_str = groups.get('order') if groups else None
            return int(order_str) if order_str not in (None, '') else None
        except Exception:
            return None

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

            groups = match.groupdict() or {}
            naive_dt = self._parse_event_time_from_groups(groups)
            return _localize_to_utc(naive_dt, timezone_str)

        except re.error as e:
            logger.warning(f"Invalid regex pattern: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing event time from '{stream_name}': {e}")
            return None

    def parse_event_time_with_parser(self, stream_name: str, parser_cfg: Dict[str, Any], timezone_str: str = '') -> Optional[datetime]:
        """Parse event time using parser.v1 datetime candidate_patterns."""
        try:
            dt_cfg = (parser_cfg or {}).get('datetime', {})
            candidates = dt_cfg.get('candidate_patterns', []) if isinstance(dt_cfg, dict) else []
            # Highest priority first
            sorted_candidates = sorted(
                [c for c in candidates if isinstance(c, dict) and c.get('pattern')],
                key=lambda c: int(c.get('priority', 0) or 0),
                reverse=True,
            )
            for cand in sorted_candidates:
                groups = self._match_with_pattern(stream_name, cand.get('pattern', ''))
                if groups:
                    naive_dt = self._parse_event_time_from_groups(groups)
                    parsed = _localize_to_utc(naive_dt, timezone_str)
                    if parsed is not None:
                        return parsed
            return None
        except Exception:
            return None

    def extract_order_with_parser(self, stream_name: str, parser_cfg: Dict[str, Any]) -> Optional[int]:
        """Extract order using parser.v1 fields.extractors.order."""
        try:
            extractors = (((parser_cfg or {}).get('fields') or {}).get('extractors') or {}).get('order', [])
            sorted_extractors = sorted(
                [e for e in extractors if isinstance(e, dict) and e.get('pattern')],
                key=lambda e: int(e.get('priority', 0) or 0),
                reverse=True,
            )
            for ext in sorted_extractors:
                groups = self._match_with_pattern(stream_name, ext.get('pattern', ''))
                if groups:
                    order = self._extract_order_from_groups(groups)
                    if order is not None:
                        return order
            return None
        except Exception:
            return None

    def categorize_streams(
        self,
        streams: List[Dict[str, Any]],
        pattern: str,
        timezone_str: str = '',
        grace_hours: float = 2.0,
        parser_cfg: Optional[Dict[str, Any]] = None,
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
        now = datetime.now(tz=pytz.utc)
        grace_cutoff = now - timedelta(hours=grace_hours)

        python_pattern = _js_to_python_regex(pattern) if pattern else None
        upcoming = []
        past = []
        unparseable = []

        for stream in streams:
            stream_name = stream.get('name', '')
            stream_id = stream.get('id')

            if parser_cfg:
                event_time = self.parse_event_time_with_parser(stream_name, parser_cfg, timezone_str)
                order = self.extract_order_with_parser(stream_name, parser_cfg)
            else:
                event_time = self.parse_event_time(stream_name, pattern, timezone_str)
                # Extract order number if present
                order = None
                try:
                    if python_pattern:
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
        _max = datetime.max.replace(tzinfo=pytz.utc)
        _min = datetime.min.replace(tzinfo=pytz.utc)

        # Sort upcoming by event time (soonest first), then by order number
        upcoming.sort(key=lambda x: (
            x['event_time'] or _max,
            x['order'] if x['order'] is not None else 9999
        ))

        # Sort past by event time descending (most recent first)
        past.sort(key=lambda x: (
            x['event_time'] or _min,
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

        parser_cfg = ch_config.get('parser') if isinstance(ch_config.get('parser'), dict) else None
        upcoming, past, unparseable = self.categorize_streams(
            streams, pattern, timezone_str, grace_hours, parser_cfg=parser_cfg
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

        parser_cfg = ch_config.get('parser') if isinstance(ch_config.get('parser'), dict) else None
        upcoming, past, unparseable = self.categorize_streams(
            streams, pattern, timezone_str, grace_hours, parser_cfg=parser_cfg
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
        """Assign conflicting events to overflow channels.

        Streams with the same order number are backup feeds of the same event and stay
        together in the same channel. Only streams with different order numbers at the
        same time slot are conflicting events that need separate overflow channels.

        Collects streams from main + all overflow channels so backup feeds that ended
        up in the wrong place from a previous cycle are reassigned correctly.

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
        parser_cfg = ch_config.get('parser') if isinstance(ch_config.get('parser'), dict) else None

        # Collect streams from main + all overflow channels to get the full picture —
        # backup feeds may have ended up in overflow channels from previous cycles.
        all_streams: List[Dict] = []
        for cid in [channel_id] + list(overflow_ids):
            ch_streams = fetch_channel_streams(cid)
            if ch_streams:
                all_streams.extend(ch_streams)

        if not all_streams:
            return {'success': False, 'error': f'Could not fetch streams for channel {channel_id}'}

        upcoming, past, unparseable = self.categorize_streams(
            all_streams, pattern, timezone_str, grace_hours, parser_cfg=parser_cfg
        )

        # Group upcoming by time slot, then by order number within each slot.
        # Same order number = same event (backup feeds); different order = different event.
        time_slots: Dict[str, Dict[int, List[Dict]]] = {}
        for s in upcoming:
            if s['event_time']:
                slot_key = s['event_time'].strftime('%Y-%m-%d %H:00')
                order = s['order'] if s['order'] is not None else 9999
                time_slots.setdefault(slot_key, {}).setdefault(order, []).append(s)

        # Assign events to channels:
        # - Main channel: primary event per slot (lowest order) + non-conflicting events
        # - Overflow channels: one per conflicting event (all its backup feeds together)
        main_stream_ids: List[int] = []
        overflow_event_groups: List[List[int]] = []  # [i] -> stream_ids for overflow_ids[i]

        for slot_key in sorted(time_slots.keys()):
            order_groups = time_slots[slot_key]
            for i, order in enumerate(sorted(order_groups.keys())):
                stream_ids = [s['stream_id'] for s in order_groups[order]]
                if i == 0:
                    main_stream_ids.extend(stream_ids)
                else:
                    overflow_event_groups.append(stream_ids)

        # Past and unparseable streams stay on the main channel
        for s in past + unparseable:
            if s['stream_id'] not in main_stream_ids:
                main_stream_ids.append(s['stream_id'])

        # Update main channel — preserve current ordering for streams already there,
        # append any that are being pulled back from overflow channels
        current_main = fetch_channel_streams(channel_id) or []
        main_id_set = set(main_stream_ids)
        ordered_main = [s['id'] for s in current_main if s['id'] in main_id_set]
        for sid in main_stream_ids:
            if sid not in set(ordered_main):
                ordered_main.append(sid)
        update_channel_streams(channel_id, ordered_main, allow_dead_streams=True)

        # Assign each conflicting event group to an overflow channel
        moved = 0
        for i, event_stream_ids in enumerate(overflow_event_groups):
            if i >= len(overflow_ids):
                logger.warning(f"Not enough overflow channels for channel {channel_id}")
                break
            target_channel = overflow_ids[i]
            update_channel_streams(target_channel, event_stream_ids, allow_dead_streams=True)
            moved += len(event_stream_ids)
            logger.info(f"Assigned {len(event_stream_ids)} stream(s) to overflow channel {target_channel}")

        # Clear overflow channels that have no current event assignment
        for i in range(len(overflow_event_groups), len(overflow_ids)):
            update_channel_streams(overflow_ids[i], [], allow_dead_streams=True)

        # Return which overflow channels received events so the renaming step
        # can use this instead of re-fetching stale UDI cache data.
        assigned_overflow_ids = set(
            overflow_ids[i] for i in range(len(overflow_event_groups)) if i < len(overflow_ids)
        )
        return {
            'success': True,
            'moved': moved,
            'total_overflow': len(overflow_event_groups),
            'assigned_overflow_ids': list(assigned_overflow_ids),
        }

    def format_channel_name(
        self,
        template: str,
        stream_name: str,
        pattern: str,
        base_name: str,
        timezone_str: str = '',
        display_timezone: str = '',
        parser_cfg: Optional[Dict[str, Any]] = None,
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
            groups: Dict[str, Any] = {}
            if parser_cfg and isinstance(parser_cfg, dict):
                # Extract fields from parser.v1 field extractors (highest-priority first per field)
                extractors = (((parser_cfg.get('fields') or {}).get('extractors')) or {})
                if isinstance(extractors, dict):
                    for field_name, defs in extractors.items():
                        if not isinstance(defs, list):
                            continue
                        sorted_defs = sorted(
                            [d for d in defs if isinstance(d, dict) and d.get('pattern')],
                            key=lambda d: int(d.get('priority', 0) or 0),
                            reverse=True,
                        )
                        for d in sorted_defs:
                            m_groups = self._match_with_pattern(stream_name, d.get('pattern', ''))
                            if m_groups and m_groups.get(field_name) not in (None, ''):
                                groups[field_name] = m_groups.get(field_name)
                                break
                # Also include first successful datetime groups for year/month/day/hour/minute/second/ampm
                dt_cfg = (parser_cfg.get('datetime') or {})
                dt_defs = dt_cfg.get('candidate_patterns', []) if isinstance(dt_cfg, dict) else []
                dt_sorted = sorted(
                    [d for d in dt_defs if isinstance(d, dict) and d.get('pattern')],
                    key=lambda d: int(d.get('priority', 0) or 0),
                    reverse=True,
                )
                for d in dt_sorted:
                    m_groups = self._match_with_pattern(stream_name, d.get('pattern', ''))
                    if m_groups:
                        for k, v in m_groups.items():
                            if v not in (None, '') and k not in groups:
                                groups[k] = v
                        break
            else:
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
            event_time = self.parse_event_time_with_parser(stream_name, parser_cfg, timezone_str) if parser_cfg else self.parse_event_time(stream_name, pattern, timezone_str)
            if event_time:
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
        timezone_str: str = '',
        parser_cfg: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Test a regex pattern against a list of stream names.

        Returns:
            List of dicts with stream_name, matched, event_time, groups
        """
        results = []
        python_pattern = _js_to_python_regex(pattern) if pattern else None

        for name in stream_names:
            try:
                if parser_cfg and isinstance(parser_cfg, dict):
                    extracted_groups: Dict[str, Any] = {}
                    extractors = (((parser_cfg.get('fields') or {}).get('extractors')) or {})
                    if isinstance(extractors, dict):
                        for field_name, defs in extractors.items():
                            if not isinstance(defs, list):
                                continue
                            sorted_defs = sorted(
                                [d for d in defs if isinstance(d, dict) and d.get('pattern')],
                                key=lambda d: int(d.get('priority', 0) or 0),
                                reverse=True,
                            )
                            for d in sorted_defs:
                                mg = self._match_with_pattern(name, d.get('pattern', ''))
                                if mg and mg.get(field_name) not in (None, ''):
                                    extracted_groups[field_name] = mg.get(field_name)
                                    break

                    event_time = self.parse_event_time_with_parser(name, parser_cfg)
                    matched = bool(event_time or extracted_groups)
                    results.append({
                        'stream_name': name,
                        'matched': matched,
                        'event_time': event_time.isoformat() if event_time else None,
                        'groups': extracted_groups,
                    })
                else:
                    match = re.search(python_pattern, name) if python_pattern else None
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
                if not renaming_enabled and name_template and result.get('success'):
                    # Renaming was explicitly disabled — restore base names for main + overflow channels
                    self.rename_channel(channel_id, channel_name)
                    for i, overflow_id in enumerate(overflow_ids):
                        self.rename_channel(overflow_id, f"{channel_name} {i + 2}")
                elif renaming_enabled and name_template and result.get('success'):
                    pattern = ch_config.get('pattern', '')
                    timezone_str = ch_config.get('stream_timezone', '')
                    display_tz = ch_config.get('display_timezone', '')
                    parser_cfg = ch_config.get('parser') if isinstance(ch_config.get('parser'), dict) else None
                    # Rename main channel based on its top stream
                    streams = fetch_channel_streams(channel_id)
                    if streams:
                        top_stream_name = streams[0].get('name', '')
                        new_name = self.format_channel_name(
                            name_template, top_stream_name, pattern,
                            channel_name, timezone_str, display_tz,
                            parser_cfg=parser_cfg,
                        )
                        if new_name and new_name != channel_name:
                            renamed = self.rename_channel(channel_id, new_name)
                            results[channel_id_str]['renamed'] = renamed
                            results[channel_id_str]['new_channel_name'] = new_name if renamed else None
                    # Rename each overflow channel.
                    # Use overflow assignment info from handle_overflow rather than
                    # re-fetching streams — the UDI cache is stale immediately after
                    # handle_overflow PATCHes Dispatcharr, so fetch_channel_streams
                    # would return old (pre-clear) data for emptied channels.
                    assigned_overflow_ids = set(
                        results[channel_id_str].get('overflow', {}).get('assigned_overflow_ids', [])
                    )
                    for i, overflow_id in enumerate(overflow_ids):
                        if overflow_id in assigned_overflow_ids:
                            # Channel has streams — rename to its top stream's event
                            overflow_streams = fetch_channel_streams(overflow_id)
                            if overflow_streams:
                                overflow_top_name = overflow_streams[0].get('name', '')
                                overflow_new_name = self.format_channel_name(
                                    name_template, overflow_top_name, pattern,
                                    channel_name, timezone_str, display_tz,
                                    parser_cfg=parser_cfg,
                                )
                                if overflow_new_name:
                                    self.rename_channel(overflow_id, overflow_new_name)
                        else:
                            # Channel was cleared — reset to "{base_name} {n}"
                            self.rename_channel(overflow_id, f"{channel_name} {i + 2}")

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
