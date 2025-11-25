#!/usr/bin/env python3
"""
Stream Checker Service for Dispatcharr.

This service manages stream quality checking, rating, and ordering for
Dispatcharr channels. It implements a comprehensive system for maintaining
optimal stream quality across all channels.

Features:
    - Queue-based channel checking with priority support
    - Tracking of M3U playlist update events
    - Scheduled global checks during configurable off-peak hours
    - Progressive stream rating and automatic ordering
    - Real-time progress reporting via web API
    - Thread-safe operations with proper synchronization

The service runs continuously in the background, monitoring for channel
updates and maintaining a queue of channels that need checking. It
integrates with the dispatcharr-stream-sorter.py module for actual
stream analysis.
"""

import json
import logging
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import queue

from api_utils import (
    fetch_channel_streams,
    fetch_data_from_url,
    update_channel_streams,
    _get_base_url,
    patch_request,
    get_valid_stream_ids
)

# Import dead streams tracker
from dead_streams_tracker import DeadStreamsTracker

# Import changelog manager
try:
    from automated_stream_manager import ChangelogManager
    CHANGELOG_AVAILABLE = True
except ImportError:
    CHANGELOG_AVAILABLE = False
    logging.warning("ChangelogManager not available. Stream check changelog will be disabled.")

# Custom logging filter to exclude HTTP-related logs
class HTTPLogFilter(logging.Filter):
    """Filter out HTTP-related log messages."""
    def filter(self, record):
        message = record.getMessage().lower()
        http_indicators = [
            'http request',
            'http response',
            'status code',
            'get /',
            'post /',
            'put /',
            'delete /',
            'patch /',
            '" with',
            '- - [',
            'werkzeug',
        ]
        return not any(indicator in message for indicator in http_indicators)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
for handler in logging.root.handlers:
    handler.addFilter(HTTPLogFilter())

# Configuration directory
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))

# Regular expression for parsing event times from stream names
import re
EVENT_TIME_PATTERN = re.compile(r'start:(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})')


def parse_event_time(stream_name: str) -> Optional[datetime]:
    """Parse event start time from stream name.

    Looks for pattern like 'start:2025-11-21 14:55:00' in stream name.

    Args:
        stream_name: The stream name to parse

    Returns:
        datetime object if found, None otherwise
    """
    match = EVENT_TIME_PATTERN.search(stream_name)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    return None


def reorder_streams_by_event_time(channel_id: str, stream_ids: List[int]) -> List[int]:
    """Reorder streams within a channel by event time.

    Streams are ordered by:
    1. Currently live events (start time <= now <= stop time)
    2. Upcoming events (sorted by start time, soonest first)
    3. Past events or streams without event times (original order)

    Args:
        channel_id: The channel ID
        stream_ids: List of stream IDs to reorder

    Returns:
        Reordered list of stream IDs
    """
    if not stream_ids:
        return stream_ids

    try:
        base_url = _get_base_url()

        # Fetch stream details for each stream ID
        streams_with_times = []
        for idx, stream_id in enumerate(stream_ids):
            stream_data = fetch_data_from_url(f"{base_url}/api/channels/streams/{stream_id}/")
            if stream_data:
                stream_name = stream_data.get('name', '')
                event_time = parse_event_time(stream_name)
                streams_with_times.append({
                    'id': stream_id,
                    'name': stream_name,
                    'event_time': event_time,
                    'original_index': idx  # Preserve provider order for ties
                })
            else:
                streams_with_times.append({
                    'id': stream_id,
                    'name': '',
                    'event_time': None,
                    'original_index': idx
                })

        now = datetime.utcnow()  # Use UTC since stream times are in UTC

        # Separate streams into categories
        with_time = [(s, s['event_time']) for s in streams_with_times if s['event_time']]
        without_time = [s for s in streams_with_times if not s['event_time']]

        # Sort streams with event times by start time, then by original index for ties
        with_time.sort(key=lambda x: (x[1], x[0]['original_index']))

        # Separate into live/upcoming and past
        live_upcoming = []
        past = []

        for stream, event_time in with_time:
            if event_time >= now - timedelta(hours=2):  # Allow 2 hour buffer for "live"
                live_upcoming.append(stream)
            else:
                past.append(stream)

        # Final order: live/upcoming first, then past, then no time
        ordered = live_upcoming + past + without_time

        logging.info(f"Reordered channel {channel_id}: {len(live_upcoming)} upcoming/live, {len(past)} past, {len(without_time)} no time")

        return [s['id'] for s in ordered]

    except Exception as e:
        logging.error(f"Failed to reorder streams for channel {channel_id}: {e}")
        return stream_ids


def parse_event_time_with_pattern(stream_name: str, pattern: str) -> tuple:
    """Parse event time from stream name using a custom regex pattern with named groups.

    Expected named groups: year, month, day, hour, minute, second, ampm, order, league

    Args:
        stream_name: The stream name to parse
        pattern: Regex pattern with named capture groups

    Returns:
        tuple of (datetime, order_num) - datetime object if found (None otherwise), order number for tiebreaking
    """
    try:
        # Convert JavaScript-style named groups (?<name>) to Python-style (?P<name>)
        python_pattern = re.sub(r'\(\?<([^>]+)>', r'(?P<\1>', pattern)

        match = re.search(python_pattern, stream_name, re.IGNORECASE)
        if not match:
            logging.warning(f"Pattern did not match stream: {stream_name[:80]}")
            logging.debug(f"Pattern used: {pattern}")
            return (None, 999)

        groups = match.groupdict()

        # Extract order for tiebreaking
        order_val = groups.get('order')
        order_num = int(order_val) if order_val and order_val.isdigit() else 999

        # Extract components with defaults
        now = datetime.utcnow()

        # Year
        year = int(groups.get('year', now.year)) if groups.get('year') else now.year

        # Month - can be numeric or text
        month_val = groups.get('month')
        if month_val:
            if month_val.isdigit():
                month = int(month_val)
            else:
                months = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                         'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
                month = months.get(month_val.lower()[:3], now.month)
        else:
            month = now.month

        # Day
        day = int(groups.get('day', now.day)) if groups.get('day') else now.day

        # Hour
        hour = int(groups.get('hour', 0)) if groups.get('hour') else 0

        # Handle AM/PM
        ampm = groups.get('ampm', '').upper()
        if ampm == 'PM' and hour != 12:
            hour += 12
        elif ampm == 'AM' and hour == 12:
            hour = 0

        # Minute and second
        minute = int(groups.get('minute', 0)) if groups.get('minute') else 0
        second = int(groups.get('second', 0)) if groups.get('second') else 0

        return (datetime(year, month, day, hour, minute, second), order_num)

    except Exception as e:
        logging.debug(f"Failed to parse event time with pattern: {e}")
        return (None, 999)


def parse_event_time_multi_format(stream_name: str) -> Optional[datetime]:
    """Parse event time from stream name supporting multiple formats.

    Supported formats:
    1. start:YYYY-MM-DD HH:MM:SS (UFC/NBA Events)
    2. - 7PM EventName (LIVE EVENT PPV with time only)
    3. / Nov 22 : 8PM UK (LIVE EVENT PPV with date)

    Args:
        stream_name: The stream name to parse

    Returns:
        datetime object if found, None otherwise
    """
    # Format 1: start:YYYY-MM-DD HH:MM:SS
    match = EVENT_TIME_PATTERN.search(stream_name)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass

    # Format 2: / Nov 22 : 8PM UK (with date)
    match = re.search(r'/\s*(\w+)\s+(\d+)\s*:\s*(\d+)(AM|PM)\s*UK', stream_name, re.IGNORECASE)
    if match:
        try:
            month_str = match.group(1)
            day = int(match.group(2))
            hour = int(match.group(3))
            ampm = match.group(4).upper()

            if ampm == 'PM' and hour != 12:
                hour += 12
            elif ampm == 'AM' and hour == 12:
                hour = 0

            months = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                     'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
            month = months.get(month_str.lower()[:3], 1)
            return datetime(2025, month, day, hour, 0, 0)
        except:
            pass

    # Format 3: - 7PM EventName (time only, assume today)
    match = re.search(r'-\s*(\d+)(AM|PM)\s+\w', stream_name, re.IGNORECASE)
    if match:
        try:
            hour = int(match.group(1))
            ampm = match.group(2).upper()

            if ampm == 'PM' and hour != 12:
                hour += 12
            elif ampm == 'AM' and hour == 12:
                hour = 0

            now = datetime.utcnow()
            return datetime(now.year, now.month, now.day, hour, 0, 0)
        except:
            pass

    return None


def get_moved_streams_file():
    """Get path to the moved streams tracking file."""
    return Path(__file__).parent / 'data' / 'moved_streams.json'


def load_moved_streams():
    """Load tracking data for moved streams."""
    filepath = get_moved_streams_file()
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading moved streams: {e}")
    return {"moved_streams": []}


def save_moved_streams(data):
    """Save tracking data for moved streams."""
    filepath = get_moved_streams_file()
    filepath.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving moved streams: {e}")


def move_streams_to_channel(stream_ids: List[int], target_channel_id: int, valid_stream_ids: set = None) -> bool:
    """Move streams to a target channel in Dispatcharr.

    Args:
        stream_ids: List of stream IDs to move
        target_channel_id: Channel ID to move streams to
        valid_stream_ids: Pre-fetched set of valid stream IDs to avoid redundant API calls

    Returns:
        True if successful, False otherwise
    """
    try:
        base_url = _get_base_url()

        # Get current streams in target channel
        target_streams = fetch_data_from_url(f"{base_url}/api/channels/channels/{target_channel_id}/streams/")
        if target_streams is None:
            target_streams = []

        current_ids = [s['id'] for s in target_streams if isinstance(s, dict) and 'id' in s]

        # Add new streams to the end
        new_ids = current_ids + stream_ids

        # Safeguard: ensure we're not creating duplicates
        if len(new_ids) != len(set(new_ids)):
            logging.warning(f"Removing {len(new_ids) - len(set(new_ids))} duplicate streams for channel {target_channel_id}")
            # Keep order, remove duplicates
            seen = set()
            new_ids = [x for x in new_ids if not (x in seen or seen.add(x))]

        # Build valid set from current + new streams (no need to fetch all)
        local_valid_ids = set(new_ids)

        # Update target channel
        success = update_channel_streams(str(target_channel_id), new_ids, local_valid_ids, allow_dead_streams=True)
        if success:
            logging.info(f"Moved {len(stream_ids)} streams to channel {target_channel_id}")
        return success

    except Exception as e:
        logging.error(f"Error moving streams to channel {target_channel_id}: {e}")
        return False


def remove_streams_from_channel(stream_ids: List[int], channel_id: int, valid_stream_ids: set = None) -> bool:
    """Remove streams from a channel in Dispatcharr.

    Args:
        stream_ids: List of stream IDs to remove
        channel_id: Channel ID to remove streams from
        valid_stream_ids: Pre-fetched set of valid stream IDs to avoid redundant API calls

    Returns:
        True if successful, False otherwise
    """
    try:
        base_url = _get_base_url()

        # Get current streams in channel
        channel_streams = fetch_data_from_url(f"{base_url}/api/channels/channels/{channel_id}/streams/")
        if not channel_streams:
            return True

        current_ids = [s['id'] for s in channel_streams if isinstance(s, dict) and 'id' in s]

        # Remove specified streams
        new_ids = [sid for sid in current_ids if sid not in stream_ids]

        # Safeguard: verify expected removal count
        expected_removed = len([sid for sid in stream_ids if sid in current_ids])
        actual_removed = len(current_ids) - len(new_ids)
        if expected_removed != actual_removed:
            logging.warning(f"Stream removal mismatch for channel {channel_id}. Expected: {expected_removed}, Actual: {actual_removed}")

        # Build valid set from remaining streams (no need to fetch all)
        local_valid_ids = set(new_ids)

        # Update channel
        success = update_channel_streams(str(channel_id), new_ids, local_valid_ids, allow_dead_streams=True)
        if success:
            logging.info(f"Removed {len(stream_ids)} streams from channel {channel_id}")
        return success

    except Exception as e:
        logging.error(f"Error removing streams from channel {channel_id}: {e}")
        return False


def handle_overflow_conflicts(streams_data: list, channel_id: int, overflow_channel_ids: list, return_after_hours: int, valid_stream_ids: set = None):
    """Handle overflow for conflicting events at the same time.

    Groups streams by name (to keep primary+backup together), detects time conflicts,
    and moves conflicting events to overflow channels.

    Args:
        streams_data: List of stream data dicts with event_time and item_num
        channel_id: Source channel ID
        overflow_channel_ids: List of target overflow channel IDs
        return_after_hours: Hours after which to return streams
        valid_stream_ids: Pre-fetched set of valid stream IDs to avoid redundant API calls
    """
    try:
        if not overflow_channel_ids:
            return

        # Group streams by event (using item_num as event identifier)
        events = {}
        for stream in streams_data:
            item_num = stream.get('item_num', 999)
            if item_num not in events:
                events[item_num] = []
            events[item_num].append(stream)

        # Build list of events with time ranges (start + duration)
        events_with_ranges = []
        logging.info(f"Processing {len(events)} unique events for overflow check")
        for item_num, event_streams in events.items():
            # Use the event time from the first stream of this event
            event_time = event_streams[0].get('event_time')
            if event_time:
                # Calculate end time using return_after_hours as event duration
                event_end = event_time + timedelta(hours=return_after_hours)
                events_with_ranges.append({
                    'item_num': item_num,
                    'streams': event_streams,
                    'start_time': event_time,
                    'end_time': event_end
                })
            else:
                logging.info(f"Event {item_num} has no event_time: {event_streams[0].get('name', 'unknown')[:50]}")

        # Sort events by start time
        events_with_ranges.sort(key=lambda x: (x['start_time'], x['item_num']))

        # Find overlapping events (channel can only play one event at a time)
        moves_by_channel = {}  # overflow_channel_id -> list of stream_ids
        streams_to_remove_by_channel = {}  # channel_id -> list of stream_ids (track where to remove from)
        move_idx = 0
        conflicts_found = []

        if events_with_ranges:
            # Keep first event, check subsequent events for overlap
            kept_events = [events_with_ranges[0]]

            for event in events_with_ranges[1:]:
                # Check if this event overlaps with any kept event
                has_conflict = False
                for kept in kept_events:
                    # Two events overlap if: event1.start < event2.end AND event2.start < event1.end
                    if event['start_time'] < kept['end_time'] and kept['start_time'] < event['end_time']:
                        has_conflict = True
                        conflicts_found.append({
                            'event': event,
                            'conflicts_with': kept
                        })

                        # Move to overflow
                        overflow_id = int(overflow_channel_ids[move_idx % len(overflow_channel_ids)])
                        move_idx += 1

                        if overflow_id not in moves_by_channel:
                            moves_by_channel[overflow_id] = []

                        for stream in event['streams']:
                            moves_by_channel[overflow_id].append(stream['id'])

                            # Track which channel to remove this stream from
                            source_channel = stream.get('current_channel', channel_id)
                            if source_channel not in streams_to_remove_by_channel:
                                streams_to_remove_by_channel[source_channel] = []
                            streams_to_remove_by_channel[source_channel].append(stream['id'])

                        logging.info(f"Conflict: Event {event['item_num']} ({event['start_time'].strftime('%H:%M')}-{event['end_time'].strftime('%H:%M')}) overlaps with event {kept['item_num']} ({kept['start_time'].strftime('%H:%M')}-{kept['end_time'].strftime('%H:%M')}). Moving {len(event['streams'])} streams to channel {overflow_id}")
                        break

                if not has_conflict:
                    # No conflict, keep this event on the channel
                    kept_events.append(event)

            logging.info(f"Found {len(conflicts_found)} conflicting events out of {len(events_with_ranges)} total events")

        if not moves_by_channel:
            return

        # Move streams to their respective overflow channels
        tracking = load_moved_streams()
        now = datetime.utcnow()
        return_at = now + timedelta(hours=return_after_hours)

        for overflow_id, stream_ids in moves_by_channel.items():
            if move_streams_to_channel(stream_ids, overflow_id, valid_stream_ids):
                # Remove from their actual source channels (might be main or another overflow)
                for source_channel_id, streams_to_remove in streams_to_remove_by_channel.items():
                    # Only remove streams that are going to this overflow channel
                    streams_for_this_overflow = [sid for sid in streams_to_remove if sid in stream_ids]
                    if streams_for_this_overflow:
                        remove_streams_from_channel(streams_for_this_overflow, source_channel_id, valid_stream_ids)
                        logging.info(f"Removed {len(streams_for_this_overflow)} streams from channel {source_channel_id}")

                # Track for return to main channel
                tracking['moved_streams'].append({
                    'stream_ids': stream_ids,
                    'from_channel': channel_id,
                    'to_channel': overflow_id,
                    'moved_at': now.isoformat(),
                    'return_at': return_at.isoformat()
                })
                logging.info(f"Tracked {len(stream_ids)} streams for return from channel {overflow_id} at {return_at.isoformat()}")

        save_moved_streams(tracking)

    except Exception as e:
        logging.error(f"Error handling overflow conflicts: {e}")


def return_moved_streams():
    """Return streams that have exceeded their overflow time back to original channel."""
    try:
        tracking = load_moved_streams()
        now = datetime.utcnow()

        remaining = []
        for entry in tracking.get('moved_streams', []):
            return_at = datetime.fromisoformat(entry['return_at'])

            if now >= return_at:
                # Time to return these streams
                stream_ids = entry['stream_ids']
                from_channel = entry['from_channel']
                to_channel = entry['to_channel']

                logging.info(f"Returning {len(stream_ids)} streams from channel {to_channel} to {from_channel}")

                # Move back to original channel
                if move_streams_to_channel(stream_ids, from_channel):
                    # Remove from overflow channel
                    remove_streams_from_channel(stream_ids, to_channel)
                    logging.info(f"Successfully returned streams to channel {from_channel}")
                else:
                    # Keep in tracking if failed
                    remaining.append(entry)
            else:
                # Not yet time to return
                remaining.append(entry)

        # Update tracking file
        tracking['moved_streams'] = remaining
        save_moved_streams(tracking)

    except Exception as e:
        logging.error(f"Error returning moved streams: {e}")


def apply_event_time_ordering_for_channels(channel_ids: List[int], channels_config: dict = None):
    """Apply event time ordering to specific channels.

    Args:
        channel_ids: List of channel IDs to order
        channels_config: Optional dict with per-channel config including custom patterns
    """
    try:
        base_url = _get_base_url()
        reordered_count = 0

        # Collect all stream IDs we work with - they're valid since they come from channels
        all_stream_ids = set()

        for channel_id in channel_ids:
            try:
                # Get channel info
                channel = fetch_data_from_url(f"{base_url}/api/channels/channels/{channel_id}/")
                if not channel:
                    logging.warning(f"Channel {channel_id} not found")
                    continue

                channel_name = channel.get('name', f'Channel {channel_id}')

                # Get custom pattern and overflow config for this channel if available
                custom_pattern = None
                overflow_channel_ids = []
                return_after_hours = 6
                if channels_config and str(channel_id) in channels_config:
                    channel_config = channels_config[str(channel_id)]
                    custom_pattern = channel_config.get('pattern')
                    # Handle both old single ID and new array format
                    overflow_channel_ids = channel_config.get('overflow_channel_ids', [])
                    if not overflow_channel_ids and channel_config.get('overflow_channel_id'):
                        overflow_channel_ids = [channel_config.get('overflow_channel_id')]
                    return_after_hours = channel_config.get('return_after_hours', 6)

                # Get current streams
                streams = fetch_data_from_url(f"{base_url}/api/channels/channels/{channel_id}/streams/")
                if not streams or not isinstance(streams, list):
                    continue

                stream_ids = [s['id'] for s in streams if isinstance(s, dict) and 'id' in s]
                if len(stream_ids) < 2:
                    continue

                # Use stream IDs from channel as valid set - no need to fetch all streams
                # These streams came from the channel so they're already valid
                valid_stream_ids = set(stream_ids)
                all_stream_ids.update(stream_ids)

                # Parse stream data - names are already in the streams response
                streams_data = []
                for idx, stream in enumerate(streams):
                    if isinstance(stream, dict) and 'id' in stream:
                        stream_id = stream['id']
                        stream_name = stream.get('name', '')

                        # Use custom pattern if available, otherwise fall back to multi-format parser
                        if custom_pattern:
                            event_time, item_num = parse_event_time_with_pattern(stream_name, custom_pattern)
                            if idx == 0:  # Log first stream for debugging
                                logging.info(f"Parsed first stream: event_time={event_time}, item_num={item_num}")
                        else:
                            event_time = parse_event_time_multi_format(stream_name)
                            # Fall back to extracting number for ordering (e.g., "PPV 1", "EVENT 06", "UFC 02")
                            num_match = re.search(r'(?:PPV|EVENT|UFC|NBA)\s*(\d+)', stream_name, re.IGNORECASE)
                            item_num = int(num_match.group(1)) if num_match else 999

                        streams_data.append({
                            'id': stream_id,
                            'name': stream_name,
                            'event_time': event_time,
                            'item_num': item_num,
                            'original_index': idx
                        })

                if not streams_data:
                    continue

                # Check if any streams have event times
                has_times = any(s['event_time'] for s in streams_data)

                if has_times:
                    # Sort by event time
                    now = datetime.utcnow()
                    buffer_hours = 2

                    upcoming = []
                    past = []

                    for s in streams_data:
                        if s['event_time']:
                            hours_diff = (now - s['event_time']).total_seconds() / 3600
                            if hours_diff < buffer_hours:
                                upcoming.append(s)
                            else:
                                past.append(s)
                        else:
                            past.append(s)

                    # Sort upcoming by time, past by time desc
                    upcoming.sort(key=lambda x: (x['event_time'], x['item_num']))
                    past.sort(key=lambda x: (x['event_time'] or datetime.min, x['item_num']), reverse=True)

                    ordered = upcoming + past
                else:
                    # No times, sort by item number
                    streams_data.sort(key=lambda x: x['item_num'])
                    ordered = streams_data

                reordered_ids = [s['id'] for s in ordered]

                if reordered_ids != stream_ids:
                    # Safeguard: ensure we're not losing streams (same set, different order)
                    if not reordered_ids:
                        logging.error(f"✗ Safeguard: Refusing to clear channel {channel_name}")
                        continue
                    if set(reordered_ids) != set(stream_ids):
                        logging.error(f"✗ Safeguard: Stream set mismatch for {channel_name}. Original: {len(stream_ids)}, New: {len(reordered_ids)}")
                        continue

                    success = update_channel_streams(str(channel_id), reordered_ids, valid_stream_ids, allow_dead_streams=True)
                    if success:
                        logging.info(f"✓ Reordered {channel_name}: {len([s for s in ordered if s.get('event_time') and (datetime.utcnow() - s['event_time']).total_seconds() / 3600 < 2])} upcoming")
                        reordered_count += 1
                    else:
                        logging.error(f"✗ Failed to update {channel_name}")

                # Handle overflow conflicts if configured
                if overflow_channel_ids and streams_data:
                    logging.info(f"Checking overflow conflicts for {channel_name} with {len(overflow_channel_ids)} overflow channels")

                    # Also load streams from overflow channels to ensure primary/backup pairs stay together
                    all_streams_data = list(streams_data)  # Copy main channel streams
                    for overflow_id in overflow_channel_ids:
                        overflow_streams = fetch_data_from_url(f"{base_url}/api/channels/channels/{overflow_id}/streams/")
                        if overflow_streams and isinstance(overflow_streams, list):
                            for stream in overflow_streams:
                                if isinstance(stream, dict) and 'id' in stream:
                                    stream_id = stream['id']
                                    stream_name = stream.get('name', '')

                                    # Parse this stream with the same pattern
                                    if custom_pattern:
                                        event_time, item_num = parse_event_time_with_pattern(stream_name, custom_pattern)
                                    else:
                                        event_time = parse_event_time_multi_format(stream_name)
                                        num_match = re.search(r'(?:PPV|EVENT|UFC|NBA)\s*(\d+)', stream_name, re.IGNORECASE)
                                        item_num = int(num_match.group(1)) if num_match else 999

                                    all_streams_data.append({
                                        'id': stream_id,
                                        'name': stream_name,
                                        'event_time': event_time,
                                        'item_num': item_num,
                                        'original_index': len(all_streams_data),
                                        'current_channel': overflow_id  # Track where this stream is now
                                    })

                    handle_overflow_conflicts(
                        all_streams_data,
                        channel_id,
                        overflow_channel_ids,
                        return_after_hours,
                        valid_stream_ids
                    )
                elif not overflow_channel_ids:
                    logging.debug(f"No overflow channels configured for {channel_name}")

            except Exception as e:
                logging.error(f"Error ordering channel {channel_id}: {e}")

        logging.info(f"Event ordering complete: {reordered_count}/{len(channel_ids)} channels updated")

    except Exception as e:
        logging.error(f"Failed to apply event time ordering: {e}")


def apply_event_time_ordering():
    """Apply event time ordering to all channels.

    This function fetches all channels and reorders their streams
    based on event time, prioritizing live and upcoming events.
    """
    try:
        base_url = _get_base_url()

        # Get all channels
        all_channels = fetch_data_from_url(f"{base_url}/api/channels/channels/")
        if not all_channels:
            logging.warning("No channels found for event time ordering")
            return

        reordered_count = 0

        for channel in all_channels:
            if not isinstance(channel, dict) or 'id' not in channel:
                continue

            channel_id = str(channel['id'])
            channel_name = channel.get('name', f'Channel {channel_id}')

            # Get current streams for this channel
            streams = fetch_data_from_url(f"{base_url}/api/channels/channels/{channel_id}/streams/")
            if not streams or not isinstance(streams, list):
                continue

            # Get stream IDs in current order
            stream_ids = [s['id'] for s in streams if isinstance(s, dict) and 'id' in s]

            if len(stream_ids) < 2:
                continue  # No need to reorder single stream or empty

            # Check if any streams have event times
            has_event_times = False
            for s in streams:
                if parse_event_time(s.get('name', '')):
                    has_event_times = True
                    break

            if not has_event_times:
                continue  # Skip channels without event times

            # Reorder streams
            reordered_ids = reorder_streams_by_event_time(channel_id, stream_ids)

            if reordered_ids != stream_ids:
                # Update channel with new stream order
                success = update_channel_streams(channel_id, reordered_ids)
                if success:
                    logging.info(f"✓ Reordered streams for channel {channel_name} by event time")
                    reordered_count += 1
                else:
                    logging.error(f"✗ Failed to update stream order for channel {channel_name}")

        logging.info(f"Event time ordering complete: {reordered_count} channels reordered")

    except Exception as e:
        logging.error(f"Failed to apply event time ordering: {e}")


class StreamCheckConfig:
    """Configuration for stream checking service."""
    
    DEFAULT_CONFIG = {
        'enabled': True,
        'check_interval': 300,  # DEPRECATED - checks now only triggered by M3U refresh
        'pipeline_mode': 'pipeline_1_5',  # Pipeline mode: 'disabled', 'pipeline_1', 'pipeline_1_5', 'pipeline_2', 'pipeline_2_5', 'pipeline_3', 'pipeline_4'
        'global_check_schedule': {
            'enabled': True,
            'cron_expression': '0 3 * * *',  # Cron expression: default is daily at 3:00 AM
            'frequency': 'daily',  # DEPRECATED: kept for backward compatibility - 'daily' or 'monthly'
            'hour': 3,  # DEPRECATED: kept for backward compatibility - 3 AM for off-peak checking
            'minute': 0,  # DEPRECATED: kept for backward compatibility
            'day_of_month': 1  # DEPRECATED: kept for backward compatibility - Day of month for monthly checks (1-31)
        },
        'stream_analysis': {
            'ffmpeg_duration': 30,  # seconds to analyze each stream
            'idet_frames': 500,  # frames to check for interlacing
            'timeout': 30,  # timeout for operations
            'retries': 1,  # retry attempts
            'retry_delay': 10,  # seconds between retries
            'user_agent': 'VLC/3.0.14'  # user agent for ffmpeg/ffprobe
        },
        'scoring': {
            'weights': {
                'bitrate': 0.30,
                'resolution': 0.25,
                'fps': 0.15,
                'codec': 0.10,
                'errors': 0.20
            },
            'min_score': 0.0,  # minimum score to keep stream
            'prefer_h265': True,  # prefer h265 over h264
            'penalize_interlaced': True,
            'penalize_dropped_frames': True
        },
        'queue': {
            'max_size': 1000,
            'check_on_update': True,  # check channels when they receive M3U updates
            'max_channels_per_run': 50  # limit channels per check cycle
        }
    }
    
    def __init__(self, config_file: Optional[str] = None) -> None:
        """
        Initialize the StreamCheckConfig.
        
        Parameters:
            config_file (Optional[str]): Path to config file. Defaults
                to CONFIG_DIR/stream_checker_config.json.
        """
        if config_file is None:
            config_file = CONFIG_DIR / 'stream_checker_config.json'
        self.config_file = Path(config_file)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from file or create default.
        
        Merges loaded config with DEFAULT_CONFIG to ensure all
        required keys exist even if config file is incomplete.
        
        Returns:
            Dict[str, Any]: The configuration dictionary.
        """
        import copy
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    # Deep copy defaults to avoid mutating DEFAULT_CONFIG
                    config = copy.deepcopy(self.DEFAULT_CONFIG)
                    config.update(loaded)
                    return config
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logging.warning(
                    f"Could not load config from "
                    f"{self.config_file}: {e}, using defaults"
                )
        
        # Create default config - use deep copy to avoid mutation
        self._save_config(copy.deepcopy(self.DEFAULT_CONFIG))
        return copy.deepcopy(self.DEFAULT_CONFIG)
    
    def _save_config(
        self, config: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Save configuration to file.
        
        Parameters:
            config (Optional[Dict[str, Any]]): Config to save.
                Defaults to self.config.
        """
        if config is None:
            config = self.config
        
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
    
    def update(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration with new values.
        
        Performs deep update to handle nested dictionaries.
        
        Parameters:
            updates (Dict[str, Any]): Configuration updates to apply.
        """
        def deep_update(
            base: Dict[str, Any], updates: Dict[str, Any]
        ) -> None:
            """Recursively update nested dictionaries."""
            for key, value in updates.items():
                if (isinstance(value, dict) and key in base and
                        isinstance(base[key], dict)):
                    deep_update(base[key], value)
                else:
                    base[key] = value
        
        deep_update(self.config, updates)
        self._save_config()
        logging.info("Stream checker configuration updated")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Supports nested keys like 'queue.max_size'.
        
        Parameters:
            key (str): Configuration key (supports dot notation).
            default (Any): Default value if key not found.
            
        Returns:
            Any: The configuration value or default.
        """
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default


class ChannelUpdateTracker:
    """Tracks which channels have received M3U updates."""
    
    def __init__(self, tracker_file=None):
        if tracker_file is None:
            tracker_file = CONFIG_DIR / 'channel_updates.json'
        self.tracker_file = Path(tracker_file)
        self.updates = self._load_updates()
        self.lock = threading.Lock()
        # Ensure the file is created on initialization
        self._save_updates()
    
    def _load_updates(self) -> Dict:
        """Load update tracking data."""
        if self.tracker_file.exists():
            try:
                with open(self.tracker_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logging.warning(f"Could not load updates from {self.tracker_file}, creating new")
        return {'channels': {}, 'last_global_check': None}
    
    def _save_updates(self):
        """Save update tracking data."""
        try:
            self.tracker_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.tracker_file, 'w') as f:
                json.dump(self.updates, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save channel updates: {e}")
    
    def mark_channel_updated(self, channel_id: int, timestamp: str = None, stream_count: int = None):
        """Mark a channel as having received an update.
        
        Args:
            channel_id: The channel ID to mark as updated
            timestamp: When the update occurred (defaults to now)
            stream_count: Number of streams in the channel after update
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        with self.lock:
            if 'channels' not in self.updates:
                self.updates['channels'] = {}
            
            channel_key = str(channel_id)
            
            # Always mark channel as needing check if stream count changed
            # This ensures new streams are analyzed even during invulnerability period
            if channel_key in self.updates['channels']:
                channel_info = self.updates['channels'][channel_key]
                # Preserve checked_stream_ids if they exist
                checked_stream_ids = channel_info.get('checked_stream_ids', [])
                
                self.updates['channels'][channel_key] = {
                    'last_update': timestamp,
                    'needs_check': True,
                    'stream_count': stream_count,
                    'checked_stream_ids': checked_stream_ids
                }
            else:
                self.updates['channels'][channel_key] = {
                    'last_update': timestamp,
                    'needs_check': True,
                    'stream_count': stream_count,
                    'checked_stream_ids': []
                }
            self._save_updates()
    
    def mark_channels_updated(self, channel_ids: List[int], timestamp: str = None, stream_counts: Dict[int, int] = None):
        """Mark multiple channels as updated.
        
        Args:
            channel_ids: List of channel IDs to mark
            timestamp: When the update occurred (defaults to now)
            stream_counts: Optional dict mapping channel_id to stream count
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        if stream_counts is None:
            stream_counts = {}
        
        marked_count = 0
        
        with self.lock:
            if 'channels' not in self.updates:
                self.updates['channels'] = {}
            
            for channel_id in channel_ids:
                channel_key = str(channel_id)
                stream_count = stream_counts.get(channel_id)
                
                # Always mark channel if stream count changed (new streams added)
                # Preserve checked_stream_ids if they exist
                if channel_key in self.updates['channels']:
                    channel_info = self.updates['channels'][channel_key]
                    checked_stream_ids = channel_info.get('checked_stream_ids', [])
                    
                    self.updates['channels'][channel_key] = {
                        'last_update': timestamp,
                        'needs_check': True,
                        'stream_count': stream_count,
                        'checked_stream_ids': checked_stream_ids
                    }
                else:
                    self.updates['channels'][channel_key] = {
                        'last_update': timestamp,
                        'needs_check': True,
                        'stream_count': stream_count,
                        'checked_stream_ids': []
                    }
                marked_count += 1
            
            if marked_count > 0:
                self._save_updates()
        
        logging.info(f"Marked {marked_count} channels as updated")
    
    def get_channels_needing_check(self) -> List[int]:
        """Get list of channel IDs that need checking (read-only, doesn't clear flag).
        
        For actual queueing operations, use get_and_clear_channels_needing_check() instead
        to prevent race conditions.
        """
        with self.lock:
            channels = []
            for channel_id, info in self.updates.get('channels', {}).items():
                if info.get('needs_check', False):
                    channels.append(int(channel_id))
            return channels
    
    def get_and_clear_channels_needing_check(self, max_channels: int = None) -> List[int]:
        """Get list of channel IDs that need checking and atomically clear their needs_check flag.
        
        This atomic operation prevents race conditions where M3U refresh could
        re-mark channels while they're being queued.
        
        Args:
            max_channels: Maximum number of channels to return (None = all)
            
        Returns:
            List of channel IDs that were marked as needing check
        """
        with self.lock:
            channels = []
            timestamp = datetime.now().isoformat()
            
            for channel_id, info in self.updates.get('channels', {}).items():
                if info.get('needs_check', False):
                    channels.append(int(channel_id))
                    # Clear the flag immediately
                    info['needs_check'] = False
                    info['queued_at'] = timestamp
                    
                    if max_channels and len(channels) >= max_channels:
                        break
            
            if channels:
                self._save_updates()
                logging.debug(f"Atomically retrieved and cleared {len(channels)} channels needing check")
            
            return channels
    
    def mark_channel_checked(self, channel_id: int, timestamp: str = None, stream_count: int = None, checked_stream_ids: List[int] = None):
        """Mark a channel as checked (completed).
        
        Args:
            channel_id: The channel ID to mark as checked
            timestamp: When the check was completed (defaults to now)
            stream_count: Number of streams in the channel
            checked_stream_ids: List of stream IDs that were checked
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        with self.lock:
            if 'channels' not in self.updates:
                self.updates['channels'] = {}
            
            channel_key = str(channel_id)
            if channel_key in self.updates['channels']:
                # Update existing entry
                self.updates['channels'][channel_key]['needs_check'] = False
                self.updates['channels'][channel_key]['last_check'] = timestamp
                if stream_count is not None:
                    self.updates['channels'][channel_key]['stream_count'] = stream_count
                if checked_stream_ids is not None:
                    self.updates['channels'][channel_key]['checked_stream_ids'] = checked_stream_ids
            else:
                # Create new entry
                self.updates['channels'][channel_key] = {
                    'needs_check': False,
                    'last_check': timestamp,
                    'stream_count': stream_count,
                    'checked_stream_ids': checked_stream_ids if checked_stream_ids is not None else []
                }
            self._save_updates()
    
    def get_checked_stream_ids(self, channel_id: int) -> List[int]:
        """Get the list of stream IDs that have been checked for a channel.
        
        Args:
            channel_id: The channel ID to query
            
        Returns:
            List of stream IDs that have been checked (empty list if none or channel not tracked)
        """
        with self.lock:
            channel_key = str(channel_id)
            if channel_key in self.updates.get('channels', {}):
                return self.updates['channels'][channel_key].get('checked_stream_ids', [])
            return []
    
    def mark_channel_for_force_check(self, channel_id: int):
        """Mark a channel for force checking (bypasses 2-hour immunity).
        
        Args:
            channel_id: The channel ID to mark for force check
        """
        with self.lock:
            if 'channels' not in self.updates:
                self.updates['channels'] = {}
            
            channel_key = str(channel_id)
            if channel_key not in self.updates['channels']:
                self.updates['channels'][channel_key] = {}
            
            self.updates['channels'][channel_key]['force_check'] = True
            self._save_updates()
    
    def should_force_check(self, channel_id: int) -> bool:
        """Check if a channel should be force checked (bypassing immunity).
        
        Args:
            channel_id: The channel ID to check
            
        Returns:
            True if force check is enabled for this channel
        """
        with self.lock:
            channel_key = str(channel_id)
            if channel_key in self.updates.get('channels', {}):
                return self.updates['channels'][channel_key].get('force_check', False)
            return False
    
    def clear_force_check(self, channel_id: int):
        """Clear the force check flag for a channel.
        
        Args:
            channel_id: The channel ID to clear force check for
        """
        with self.lock:
            channel_key = str(channel_id)
            if channel_key in self.updates.get('channels', {}):
                self.updates['channels'][channel_key]['force_check'] = False
                self._save_updates()
    
    def mark_global_check(self, timestamp: str = None):
        """Mark that a global check was initiated.
        
        This only updates the timestamp to prevent duplicate global checks.
        It does NOT clear needs_check flags - those should only be cleared
        when channels are actually checked via mark_channel_checked().
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        with self.lock:
            self.updates['last_global_check'] = timestamp
            self._save_updates()
    
    def get_last_global_check(self) -> Optional[str]:
        """Get timestamp of last global check."""
        return self.updates.get('last_global_check')


class StreamCheckQueue:
    """Queue manager for channel stream checking."""
    
    def __init__(self, max_size=1000):
        self.queue = queue.Queue(maxsize=max_size)
        self.queued = set()  # Track channels already in queue
        self.in_progress = set()
        self.completed = set()
        self.failed = {}
        self.lock = threading.Lock()
        self.stats = {
            'total_queued': 0,
            'total_completed': 0,
            'total_failed': 0,
            'current_channel': None,
            'queue_size': 0
        }
    
    def add_channel(self, channel_id: int, priority: int = 0):
        """Add a channel to the checking queue."""
        with self.lock:
            # Check if channel is already queued, in progress, or completed
            if channel_id not in self.queued and channel_id not in self.in_progress and channel_id not in self.completed:
                try:
                    self.queue.put((priority, channel_id), block=False)
                    self.queued.add(channel_id)
                    self.stats['total_queued'] += 1
                    self.stats['queue_size'] = self.queue.qsize()
                    logging.debug(f"Added channel {channel_id} to queue (priority: {priority})")
                    return True
                except queue.Full:
                    logging.warning(f"Queue is full, cannot add channel {channel_id}")
                    return False
        return False
    
    def add_channels(self, channel_ids: List[int], priority: int = 0):
        """Add multiple channels to the queue."""
        added = 0
        for channel_id in channel_ids:
            if self.add_channel(channel_id, priority):
                added += 1
        logging.info(f"Added {added}/{len(channel_ids)} channels to checking queue")
        return added
    
    def remove_from_completed(self, channel_id: int):
        """Remove a channel from the completed set to allow re-queueing.
        
        This is used when a channel receives new streams and needs to be
        checked again, even if it was previously completed.
        """
        with self.lock:
            if channel_id in self.completed:
                self.completed.discard(channel_id)
                logging.debug(f"Removed channel {channel_id} from completed set")
                return True
        return False
    
    def get_next_channel(self, timeout: float = 1.0) -> Optional[int]:
        """Get the next channel to check."""
        try:
            priority, channel_id = self.queue.get(timeout=timeout)
            with self.lock:
                self.queued.discard(channel_id)  # Remove from queued set
                self.in_progress.add(channel_id)
                self.stats['current_channel'] = channel_id
                self.stats['queue_size'] = self.queue.qsize()
            return channel_id
        except queue.Empty:
            return None
    
    def mark_completed(self, channel_id: int):
        """Mark a channel check as completed."""
        with self.lock:
            if channel_id in self.in_progress:
                self.in_progress.remove(channel_id)
            self.completed.add(channel_id)
            self.stats['total_completed'] += 1
            if self.stats['current_channel'] == channel_id:
                self.stats['current_channel'] = None
            logging.debug(f"Marked channel {channel_id} as completed")
    
    def mark_failed(self, channel_id: int, error: str):
        """Mark a channel check as failed."""
        with self.lock:
            if channel_id in self.in_progress:
                self.in_progress.remove(channel_id)
            self.failed[channel_id] = {
                'error': error,
                'timestamp': datetime.now().isoformat()
            }
            self.stats['total_failed'] += 1
            if self.stats['current_channel'] == channel_id:
                self.stats['current_channel'] = None
            logging.warning(f"Marked channel {channel_id} as failed: {error}")
    
    def get_status(self) -> Dict:
        """Get current queue status."""
        with self.lock:
            return {
                'queue_size': self.queue.qsize(),
                'queued': len(self.queued),
                'in_progress': len(self.in_progress),
                'completed': len(self.completed),
                'failed': len(self.failed),
                'current_channel': self.stats['current_channel'],
                'total_queued': self.stats['total_queued'],
                'total_completed': self.stats['total_completed'],
                'total_failed': self.stats['total_failed']
            }
    
    def clear(self):
        """Clear the queue and reset stats."""
        with self.lock:
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    break
            self.queued.clear()
            self.in_progress.clear()
            self.completed.clear()
            self.failed.clear()
            self.stats = {
                'total_queued': 0,
                'total_completed': 0,
                'total_failed': 0,
                'current_channel': None,
                'queue_size': 0
            }
        logging.info("Queue cleared")


class StreamCheckerProgress:
    """Manages progress tracking for stream checker operations."""
    
    def __init__(self, progress_file=None):
        if progress_file is None:
            progress_file = CONFIG_DIR / 'stream_checker_progress.json'
        self.progress_file = Path(progress_file)
        self.lock = threading.Lock()
    
    def update(self, channel_id: int, channel_name: str, current: int, total: int,
               current_stream: str = '', status: str = 'checking', step: str = '', step_detail: str = ''):
        """Update progress information.
        
        Args:
            channel_id: The ID of the channel being checked
            channel_name: The name of the channel
            current: Current stream number being processed
            total: Total number of streams
            current_stream: Name of the current stream
            status: Overall status (checking, analyzing, updating, etc.)
            step: Current step in the process (e.g., "Fetching streams", "Analyzing", "Scoring", "Reordering")
            step_detail: Additional detail about the current step
        """
        with self.lock:
            progress_data = {
                'channel_id': channel_id,
                'channel_name': channel_name,
                'current_stream': current,
                'total_streams': total,
                'percentage': round((current / total * 100) if total > 0 else 0, 1),
                'current_stream_name': current_stream,
                'status': status,
                'step': step,
                'step_detail': step_detail,
                'timestamp': datetime.now().isoformat()
            }
            
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(self.progress_file, 'w') as f:
                    json.dump(progress_data, f)
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as e:
                logging.warning(f"Failed to write progress file: {e}")
    
    def clear(self):
        """Clear progress tracking."""
        with self.lock:
            if self.progress_file.exists():
                try:
                    self.progress_file.unlink()
                except Exception as e:
                    logging.warning(f"Failed to delete progress file: {e}")
    
    def get(self) -> Optional[Dict]:
        """Get current progress."""
        with self.lock:
            if self.progress_file.exists():
                try:
                    with open(self.progress_file, 'r') as f:
                        return json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    pass
        return None


class StreamCheckerService:
    """Main service for managing stream checking operations."""
    
    def __init__(self):
        self.config = StreamCheckConfig()
        self.update_tracker = ChannelUpdateTracker()
        self.check_queue = StreamCheckQueue(
            max_size=self.config.get('queue.max_size', 1000)
        )
        self.progress = StreamCheckerProgress()
        self.dead_streams_tracker = DeadStreamsTracker()
        
        # Initialize changelog manager
        self.changelog = None
        if CHANGELOG_AVAILABLE:
            try:
                self.changelog = ChangelogManager(changelog_file=CONFIG_DIR / "stream_checker_changelog.json")
                logging.info("Stream checker changelog manager initialized")
            except Exception as e:
                logging.warning(f"Failed to initialize changelog manager: {e}")
        
        self.running = False
        self.checking = False
        self.global_action_in_progress = False
        self.worker_thread = None
        self.scheduler_thread = None
        self.lock = threading.Lock()
        
        # Event for immediate triggering of updated channels check
        self.check_trigger = threading.Event()
        
        # Event for immediate config change notification
        self.config_changed = threading.Event()

        # Track last event ordering time
        self.last_event_ordering_time = None

        logging.info("Stream Checker Service initialized")
    
    def start(self):
        """Start the stream checker service."""
        with self.lock:
            if self.running:
                logging.warning("Stream checker service is already running")
                return
            
            self.running = True
            
            # Start worker thread for processing queue
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            
            # Start scheduler thread for periodic checks
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()
            
            logging.info("Stream checker service started")
    
    def stop(self):
        """Stop the stream checker service."""
        with self.lock:
            if not self.running:
                logging.warning("Stream checker service is not running")
                return
            
            self.running = False
            logging.info("Stream checker service stopping...")
        
        # Wait for threads to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        self.progress.clear()
        logging.info("Stream checker service stopped")
    
    def _worker_loop(self):
        """Main worker loop for processing the check queue."""
        logging.info("Stream checker worker started")
        
        while self.running:
            try:
                channel_id = self.check_queue.get_next_channel(timeout=1.0)
                if channel_id is None:
                    continue
                
                # Check this channel
                self._check_channel(channel_id)
                
            except Exception as e:
                logging.error(f"Error in worker loop: {e}", exc_info=True)
        
        logging.info("Stream checker worker stopped")
    
    def _scheduler_loop(self):
        """Scheduler loop for M3U update-triggered and scheduled checks."""
        logging.info("Stream checker scheduler started")
        
        while self.running:
            try:
                # Wait for either a trigger event or timeout (60 seconds for global check monitoring)
                triggered = self.check_trigger.wait(timeout=60)
                
                # Handle trigger for M3U updates
                if triggered:
                    self.check_trigger.clear()
                    # Only process channel queueing if this was a real M3U update trigger
                    # (not a config change wake-up) AND no global action is in progress
                    if not self.config_changed.is_set():
                        if self.global_action_in_progress:
                            logging.info("Skipping channel queueing - global action in progress")
                        else:
                            # Call _queue_updated_channels() directly - it handles pipeline mode checking internally
                            self._queue_updated_channels()
                
                # Check if config was changed
                if self.config_changed.is_set():
                    self.config_changed.clear()
                    logging.info("Configuration change detected, applying new settings immediately")
                
                # Check if it's time for a global check (checked on every iteration)
                # This will set global_action_in_progress if a global action is triggered
                if not self.global_action_in_progress:
                    self._check_global_schedule()

                # Check if it's time for event ordering
                self._check_event_ordering_schedule()

            except Exception as e:
                logging.error(f"Error in scheduler loop: {e}", exc_info=True)

        logging.info("Stream checker scheduler stopped")

    def _check_event_ordering_schedule(self):
        """Check if it's time to run event ordering on configured channels."""
        try:
            # Load event ordering config from channel_regex_config.json
            config_file = CONFIG_DIR / 'channel_regex_config.json'
            if not config_file.exists():
                return

            with open(config_file, 'r', encoding='utf-8') as f:
                regex_config = json.load(f)

            event_config = regex_config.get('event_ordering', {})
            if not event_config.get('enabled', False):
                return

            frequency = event_config.get('frequency', 300)  # Default 5 minutes
            channels_data = event_config.get('channels', {})

            # Handle backward compatibility: channels can be array (old) or object (new)
            if isinstance(channels_data, list):
                # Old format: list of channel IDs
                channel_ids = [int(cid) for cid in channels_data]
                channels_config = {}
            else:
                # New format: object with channel IDs as keys
                channel_ids = [int(cid) for cid in channels_data.keys()]
                channels_config = channels_data

            if not channel_ids:
                return

            now = datetime.now()

            # Check if enough time has passed since last ordering
            if self.last_event_ordering_time:
                elapsed = (now - self.last_event_ordering_time).total_seconds()
                if elapsed < frequency:
                    return

            # Run event ordering
            logging.info(f"Running event time ordering on {len(channel_ids)} channels")
            self.last_event_ordering_time = now

            apply_event_time_ordering_for_channels(channel_ids, channels_config)

            # Check for streams that need to be returned from overflow
            return_moved_streams()

        except Exception as e:
            logging.error(f"Error in event ordering schedule: {e}")
    
    def _queue_updated_channels(self):
        """Queue channels that have received M3U updates.
        
        This respects the pipeline mode:
        - Disabled: Skip all automation
        - Pipeline 1/1.5: Queue channels for checking
        - Pipeline 2/2.5: Skip checking (only update and match)
        - Pipeline 3: Skip checking (only scheduled global actions)
        """
        pipeline_mode = self.config.get('pipeline_mode', 'pipeline_1_5')
        
        # Disabled and Pipelines 2, 2.5, 3, and 4 don't check on update
        if pipeline_mode in ['disabled', 'pipeline_2', 'pipeline_2_5', 'pipeline_3', 'pipeline_4']:
            logging.info(f"Skipping channel queueing - {pipeline_mode} mode does not check on update")
            return
        
        max_channels = self.config.get('queue.max_channels_per_run', 50)
        
        # Atomically get channels and clear their needs_check flag
        # This prevents duplicate queueing if M3U refresh happens during check
        channels_to_queue = self.update_tracker.get_and_clear_channels_needing_check(max_channels)
        
        if channels_to_queue:
            # Remove channels from completed set to allow re-queueing
            # This is necessary when channels receive new streams after being checked
            for channel_id in channels_to_queue:
                self.check_queue.remove_from_completed(channel_id)
            
            added = self.check_queue.add_channels(channels_to_queue, priority=10)
            logging.info(f"Queued {added}/{len(channels_to_queue)} updated channels for checking (mode: {pipeline_mode})")
        else:
            logging.debug(f"No channels need checking (mode: {pipeline_mode})")
    
    def _check_global_schedule(self):
        """Check if it's time for a scheduled global action.
        
        Uses cron expression to determine when to run the global action.
        
        On fresh start (no previous check recorded):
        - Only runs if current time is within ±10 minutes of the next scheduled time
        - Otherwise waits for the scheduled time to arrive
        
        On subsequent checks (previous check exists):
        - Runs if the next scheduled time has passed since the last check
        - Prevents duplicate runs by tracking the last check time
        """
        if not self.config.get('global_check_schedule.enabled', True):
            logging.debug("Global check schedule is disabled")
            return
        
        # Get pipeline mode
        pipeline_mode = self.config.get('pipeline_mode', 'pipeline_1_5')
        
        # Only pipelines with .5 suffix, pipeline_3, and pipeline_4 have scheduled global actions
        # Disabled mode skips all automation
        if pipeline_mode not in ['pipeline_1_5', 'pipeline_2_5', 'pipeline_3', 'pipeline_4']:
            logging.debug(f"Skipping global schedule check - {pipeline_mode} mode does not have scheduled global actions")
            return
        
        now = datetime.now()
        
        # Get cron expression, with backward compatibility for old config format
        cron_expression = self.config.get('global_check_schedule.cron_expression')
        if not cron_expression:
            # Backward compatibility: convert old format to cron
            cron_expression = self._convert_legacy_schedule_to_cron()
        
        try:
            from croniter import croniter
        except ImportError:
            logging.error("croniter library not installed. Please install it with: pip install croniter")
            return
        
        # Validate cron expression
        if not croniter.is_valid(cron_expression):
            logging.error(f"Invalid cron expression: {cron_expression}")
            return
        
        last_global = self.update_tracker.get_last_global_check()
        
        # Calculate next scheduled time from now
        cron = croniter(cron_expression, now)
        next_scheduled_time = cron.get_next(datetime)
        
        # Calculate previous scheduled time (going back from now)
        cron_prev = croniter(cron_expression, now)
        prev_scheduled_time = cron_prev.get_prev(datetime)
        
        # On fresh start (no previous check), only run if within the scheduled time window (±10 minutes)
        # Otherwise, do nothing and wait for the scheduled time to arrive
        if last_global is None:
            time_diff_minutes = abs((now - prev_scheduled_time).total_seconds() / 60)
            if time_diff_minutes <= 10:
                # We're within the scheduled window on fresh start, run the check
                logging.info(f"Starting scheduled global action (mode: {pipeline_mode}, cron: {cron_expression})")
                self._perform_global_action()
                self.update_tracker.mark_global_check()
            else:
                # Fresh start but not within scheduled window, do nothing and wait
                # The scheduler will check again later when the scheduled time arrives
                logging.debug(f"Fresh start outside scheduled window (±10 min of {prev_scheduled_time.strftime('%Y-%m-%d %H:%M')}), waiting for scheduled time")
            return
        
        # Parse last check time
        last_check_time = datetime.fromisoformat(last_global)
        
        # Check if we've passed the previous scheduled time since the last check
        # This prevents running multiple times between scheduled intervals
        if prev_scheduled_time > last_check_time:
            # We've passed a scheduled time since the last check, so we should run
            logging.info(f"Starting scheduled global action (mode: {pipeline_mode}, cron: {cron_expression})")
            self._perform_global_action()
            # Mark that global check has been initiated to prevent duplicate queueing
            self.update_tracker.mark_global_check()
    
    def _convert_legacy_schedule_to_cron(self):
        """Convert legacy schedule format (hour/minute/frequency) to cron expression.
        
        This provides backward compatibility for existing configurations.
        """
        frequency = self.config.get('global_check_schedule.frequency', 'daily')
        hour = self.config.get('global_check_schedule.hour', 3)
        minute = self.config.get('global_check_schedule.minute', 0)
        
        if frequency == 'monthly':
            day_of_month = self.config.get('global_check_schedule.day_of_month', 1)
            # Monthly on specific day: minute hour day * *
            cron_expression = f"{minute} {hour} {day_of_month} * *"
        else:
            # Daily: minute hour * * *
            cron_expression = f"{minute} {hour} * * *"
        
        logging.info(f"Converted legacy schedule to cron: {cron_expression}")
        return cron_expression
    
    def _perform_global_action(self):
        """Perform a complete global action: Update M3U, Match streams, and Check all channels.

        This is the comprehensive global action that:
        1. Reloads enabled M3U accounts
        2. Matches new streams with regex patterns
        3. (Pipeline 4 only) Reorders streams by event time
        4. Checks every channel from every stream (bypassing 2-hour immunity)

        During this operation, regular automated updates, matching, and checking are paused.
        """
        try:
            # Set global action flag to prevent concurrent operations
            self.global_action_in_progress = True
            pipeline_mode = self.config.get('pipeline_mode', 'pipeline_1_5')
            is_pipeline_4 = pipeline_mode == 'pipeline_4'
            total_steps = 4 if is_pipeline_4 else 3

            logging.info("=" * 80)
            logging.info("STARTING GLOBAL ACTION")
            if is_pipeline_4:
                logging.info("Pipeline 4 mode: Event time ordering enabled")
            logging.info("Regular automation paused during global action")
            logging.info("=" * 80)

            automation_manager = None

            # Step 1: Update M3U playlists
            logging.info(f"Step 1/{total_steps}: Updating M3U playlists...")
            try:
                from automated_stream_manager import AutomatedStreamManager
                automation_manager = AutomatedStreamManager()
                update_success = automation_manager.refresh_playlists()
                if update_success:
                    logging.info("✓ M3U playlists updated successfully")
                else:
                    logging.warning("⚠ M3U playlist update had issues")
            except Exception as e:
                logging.error(f"✗ Failed to update M3U playlists: {e}")

            # Step 2: Match and assign streams
            logging.info(f"Step 2/{total_steps}: Matching and assigning streams...")
            try:
                if automation_manager is not None:
                    assignments = automation_manager.discover_and_assign_streams()
                    if assignments:
                        logging.info(f"✓ Assigned streams to {len(assignments)} channels")
                    else:
                        logging.info("✓ No new stream assignments")
                else:
                    logging.warning("⚠ Skipping stream matching - automation manager not available")
            except Exception as e:
                logging.error(f"✗ Failed to match streams: {e}")

            # Step 3 (Pipeline 4 only): Apply event time ordering
            if is_pipeline_4:
                logging.info(f"Step 3/{total_steps}: Applying event time ordering...")
                try:
                    apply_event_time_ordering()
                    logging.info("✓ Event time ordering applied successfully")
                except Exception as e:
                    logging.error(f"✗ Failed to apply event time ordering: {e}")

            # Step 3/4: Check all channels (force check to bypass immunity)
            check_step = 4 if is_pipeline_4 else 3
            logging.info(f"Step {check_step}/{total_steps}: Queueing all channels for checking...")
            self._queue_all_channels(force_check=True)
            
            logging.info("=" * 80)
            logging.info("GLOBAL ACTION INITIATED SUCCESSFULLY")
            logging.info("Regular automation will resume")
            logging.info("=" * 80)
            
        except Exception as e:
            logging.error(f"Error performing global action: {e}", exc_info=True)
        finally:
            # Always clear the flag, even if there was an error
            self.global_action_in_progress = False
    
    def _queue_all_channels(self, force_check: bool = False):
        """Queue all channels for checking (global check).
        
        Args:
            force_check: If True, marks channels for force checking which bypasses 2-hour immunity
        """
        try:
            base_url = _get_base_url()
            channels_data = fetch_data_from_url(f"{base_url}/api/channels/channels/")
            
            if channels_data:
                if isinstance(channels_data, dict) and 'results' in channels_data:
                    channels = channels_data['results']
                else:
                    channels = channels_data
                
                channel_ids = [ch['id'] for ch in channels if isinstance(ch, dict) and 'id' in ch]
                
                if force_check:
                    # Mark all channels for force check (bypasses immunity)
                    for channel_id in channel_ids:
                        self.update_tracker.mark_channel_for_force_check(channel_id)
                
                # Remove channels from completed set to allow re-queueing
                # This is necessary for global checks to re-check all channels
                for channel_id in channel_ids:
                    self.check_queue.remove_from_completed(channel_id)
                
                max_channels = self.config.get('queue.max_channels_per_run', 50)
                
                # Queue in batches with higher priority for global checks
                total_added = 0
                for i in range(0, len(channel_ids), max_channels):
                    batch = channel_ids[i:i+max_channels]
                    added = self.check_queue.add_channels(batch, priority=5)
                    total_added += added
                
                logging.info(f"Queued {total_added}/{len(channel_ids)} channels for global check (force_check={force_check})")
        except Exception as e:
            logging.error(f"Failed to queue all channels: {e}")
    
    def _is_stream_dead(self, stream_data: Dict) -> bool:
        """Check if a stream should be considered dead based on analysis results.
        
        A stream is dead if:
        - Resolution is '0x0' or contains 0 in width or height
        - Bitrate is 0 or None
        
        Args:
            stream_data: Analyzed stream data dictionary
            
        Returns:
            bool: True if stream is dead, False otherwise
        """
        # Check resolution
        resolution = stream_data.get('resolution', '')
        if resolution and resolution != 'N/A':
            resolution_str = str(resolution)
            # Check if resolution is exactly 0x0 or starts/ends with 0
            if resolution_str == '0x0':
                return True
            # Check if width or height is 0 (e.g., "0x1080" or "1920x0")
            if 'x' in resolution_str:
                try:
                    parts = resolution_str.split('x')
                    if len(parts) == 2:
                        width, height = int(parts[0]), int(parts[1])
                        if width == 0 or height == 0:
                            return True
                except (ValueError, IndexError):
                    pass
        
        # Check bitrate
        bitrate = stream_data.get('bitrate_kbps', 0)
        if bitrate in [0, None, 'N/A'] or (isinstance(bitrate, (int, float)) and bitrate == 0):
            return True
        
        return False
    
    
    def _update_stream_stats(self, stream_data: Dict) -> bool:
        """Update stream stats for a single stream on the server."""
        base_url = _get_base_url()
        if not base_url:
            logging.error("DISPATCHARR_BASE_URL not set.")
            return False
        
        stream_id = stream_data.get("stream_id")
        if not stream_id:
            logging.warning("No stream_id in stream data. Skipping stats update.")
            return False
        
        # Construct the stream stats payload from the analyzed stream data
        stream_stats_payload = {
            "resolution": stream_data.get("resolution"),
            "source_fps": stream_data.get("fps"),
            "video_codec": stream_data.get("video_codec"),
            "audio_codec": stream_data.get("audio_codec"),
            "ffmpeg_output_bitrate": int(stream_data.get("bitrate_kbps")) if stream_data.get("bitrate_kbps") not in ["N/A", None] and stream_data.get("bitrate_kbps") else None,
        }
        
        # Clean up the payload, removing any None values or N/A values
        stream_stats_payload = {k: v for k, v in stream_stats_payload.items() if v not in [None, "N/A"]}
        
        if not stream_stats_payload:
            logging.debug(f"No data to update for stream {stream_id}. Skipping.")
            return False
        
        # Construct the URL for the specific stream
        stream_url = f"{base_url}/api/channels/streams/{int(stream_id)}/"
        
        try:
            # Fetch the existing stream data to get the current stream_stats
            existing_stream_data = fetch_data_from_url(stream_url)
            if not existing_stream_data:
                logging.warning(f"Could not fetch existing data for stream {stream_id}. Skipping stats update.")
                return False
            
            # Get the existing stream_stats or an empty dict
            existing_stats = existing_stream_data.get("stream_stats") or {}
            if isinstance(existing_stats, str):
                try:
                    existing_stats = json.loads(existing_stats)
                except json.JSONDecodeError:
                    existing_stats = {}
            
            # Merge the existing stats with the new payload
            updated_stats = {**existing_stats, **stream_stats_payload}
            
            # Send the PATCH request with the updated stream_stats
            patch_payload = {"stream_stats": updated_stats}
            logging.info(f"Updating stream {stream_id} stats with: {stream_stats_payload}")
            patch_request(stream_url, patch_payload)
            return True
        
        except Exception as e:
            logging.error(f"Error updating stats for stream {stream_id}: {e}")
            return False
    
    def _check_channel(self, channel_id: int):
        """Check and reorder streams for a specific channel."""
        self.checking = True
        logging.info(f"=" * 80)
        logging.info(f"Checking channel {channel_id}")
        logging.info(f"=" * 80)
        
        try:
            # Get channel information
            self.progress.update(
                channel_id=channel_id,
                channel_name='Loading...',
                current=0,
                total=0,
                status='initializing',
                step='Fetching channel info',
                step_detail='Retrieving channel data from API'
            )
            
            base_url = _get_base_url()
            channel_data = fetch_data_from_url(f"{base_url}/api/channels/channels/{channel_id}/")
            if not channel_data:
                raise Exception(f"Could not fetch channel {channel_id}")
            
            channel_name = channel_data.get('name', f'Channel {channel_id}')
            
            # Get streams for this channel
            self.progress.update(
                channel_id=channel_id,
                channel_name=channel_name,
                current=0,
                total=0,
                status='initializing',
                step='Fetching streams',
                step_detail=f'Loading streams for {channel_name}'
            )
            
            streams = fetch_channel_streams(channel_id)
            if not streams or len(streams) == 0:
                logging.info(f"No streams found for channel {channel_name}")
                self.check_queue.mark_completed(channel_id)
                self.update_tracker.mark_channel_checked(channel_id)
                return
            
            logging.info(f"Found {len(streams)} streams for channel {channel_name}")
            
            # Check if this is a force check (bypasses 2-hour immunity)
            force_check = self.update_tracker.should_force_check(channel_id)
            
            # Get list of already checked streams to avoid re-analyzing
            checked_stream_ids = self.update_tracker.get_checked_stream_ids(channel_id)
            current_stream_ids = [s['id'] for s in streams]
            
            # Identify which streams need analysis (new or unchecked)
            # If force_check is True, check ALL streams regardless of immunity
            if force_check:
                streams_to_check = streams
                streams_already_checked = []
                logging.info(f"Force check enabled: analyzing all {len(streams)} streams (bypassing 2-hour immunity)")
                # Clear the force check flag after acknowledging it
                self.update_tracker.clear_force_check(channel_id)
            else:
                streams_to_check = [s for s in streams if s['id'] not in checked_stream_ids]
                streams_already_checked = [s for s in streams if s['id'] in checked_stream_ids]
                
                if streams_to_check:
                    logging.info(f"Found {len(streams_to_check)} new/unchecked streams (out of {len(streams)} total)")
                else:
                    logging.info(f"All {len(streams)} streams have been recently checked, using cached scores")
            
            # Import stream analysis functions from dispatcharr-stream-sorter
            # Note: The file has a dash in the name, so we need to import it specially
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "stream_sorter", 
                Path(__file__).parent / "dispatcharr-stream-sorter.py"
            )
            stream_sorter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(stream_sorter)
            
            load_sorter_config = stream_sorter.load_config
            _analyze_stream_task = stream_sorter._analyze_stream_task
            
            # Load sorter configuration
            sorter_config = load_sorter_config()
            
            # Analyze new/unchecked streams
            analyzed_streams = []
            dead_stream_ids = []
            revived_stream_ids = []
            total_streams = len(streams_to_check)
            
            for idx, stream in enumerate(streams_to_check, 1):
                self.progress.update(
                    channel_id=channel_id,
                    channel_name=channel_name,
                    current=idx,
                    total=total_streams,
                    current_stream=stream.get('name', 'Unknown'),
                    status='analyzing',
                    step='Analyzing stream quality',
                    step_detail=f'Checking bitrate, resolution, codec ({idx}/{total_streams})'
                )
                
                # Prepare stream row for analysis
                stream_row = {
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'stream_id': stream['id'],
                    'stream_name': stream.get('name', 'Unknown'),
                    'stream_url': stream.get('url', '')
                }
                
                # Analyze stream
                analysis_params = self.config.get('stream_analysis', {})
                analyzed = _analyze_stream_task(
                    stream_row,
                    ffmpeg_duration=analysis_params.get('ffmpeg_duration', 20),
                    idet_frames=analysis_params.get('idet_frames', 500),
                    timeout=analysis_params.get('timeout', 30),
                    retries=analysis_params.get('retries', 1),
                    retry_delay=analysis_params.get('retry_delay', 10),
                    config=sorter_config,
                    user_agent=analysis_params.get('user_agent', 'VLC/3.0.14')
                )
                
                # Update stream stats on dispatcharr with ffmpeg-extracted data
                self._update_stream_stats(analyzed)
                
                # Check if stream is dead (resolution=0 or bitrate=0)
                is_dead = self._is_stream_dead(analyzed)
                stream_url = stream.get('url', '')
                stream_name = stream.get('name', 'Unknown')
                was_dead = self.dead_streams_tracker.is_dead(stream_url)
                
                if is_dead and not was_dead:
                    # Mark as dead in tracker
                    if self.dead_streams_tracker.mark_as_dead(stream_url, stream['id'], stream_name):
                        dead_stream_ids.append(stream['id'])
                        logging.warning(f"Stream {stream['id']} detected as DEAD: {stream_name}")
                    else:
                        logging.error(f"Failed to mark stream {stream['id']} as DEAD, will not remove from channel")
                elif not is_dead and was_dead:
                    # Stream was revived!
                    if self.dead_streams_tracker.mark_as_alive(stream_url):
                        revived_stream_ids.append(stream['id'])
                        logging.info(f"Stream {stream['id']} REVIVED: {stream_name}")
                    else:
                        logging.error(f"Failed to mark stream {stream['id']} as alive")
                
                # Calculate score
                score = self._calculate_stream_score(analyzed)
                analyzed['score'] = score
                analyzed_streams.append(analyzed)
                
                logging.info(f"Stream {idx}/{total_streams}: {stream.get('name')} - Score: {score:.2f}")
            
            # For already-checked streams, retrieve their cached data from API
            for stream in streams_already_checked:
                stream_data = fetch_data_from_url(f"{base_url}/api/channels/streams/{stream['id']}/")
                if stream_data:
                    stream_stats = stream_data.get('stream_stats', {})
                    # Handle None case explicitly
                    if stream_stats is None:
                        stream_stats = {}
                    if isinstance(stream_stats, str):
                        try:
                            stream_stats = json.loads(stream_stats)
                            # Handle case where JSON string is "null"
                            if stream_stats is None:
                                stream_stats = {}
                        except json.JSONDecodeError:
                            stream_stats = {}
                    
                    # Reconstruct analyzed format from stored stats
                    # Use "0x0" for resolution, 0 for FPS and bitrate when not available
                    analyzed = {
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'stream_id': stream['id'],
                        'stream_name': stream.get('name', 'Unknown'),
                        'stream_url': stream.get('url', ''),
                        'resolution': stream_stats.get('resolution', '0x0'),
                        'fps': stream_stats.get('source_fps', 0),
                        'video_codec': stream_stats.get('video_codec', 'N/A'),
                        'audio_codec': stream_stats.get('audio_codec', 'N/A'),
                        'bitrate_kbps': stream_stats.get('ffmpeg_output_bitrate', 0),
                        'status': 'OK'  # Assume OK for previously checked streams
                    }
                    
                    # Check if this cached stream is dead and add to dead_stream_ids
                    stream_url = stream.get('url', '')
                    stream_name = stream.get('name', 'Unknown')
                    is_dead = self._is_stream_dead(analyzed)
                    was_dead = self.dead_streams_tracker.is_dead(stream_url)
                    
                    # If stream is dead (either was already marked or is detected as dead), track it
                    if is_dead or was_dead:
                        # Only add to dead_stream_ids if either:
                        # 1. Stream was already marked (safe to remove)
                        # 2. Stream is newly detected as dead AND marking succeeds
                        if was_dead:
                            dead_stream_ids.append(stream['id'])
                        elif not was_dead:
                            # If it wasn't marked but is dead, mark it now
                            if self.dead_streams_tracker.mark_as_dead(stream_url, stream['id'], stream_name):
                                dead_stream_ids.append(stream['id'])
                                logging.warning(f"Cached stream {stream['id']} detected as DEAD: {stream_name}")
                            else:
                                logging.error(f"Failed to mark cached stream {stream['id']} as DEAD, will not remove from channel")
                    
                    # Recalculate score from cached data
                    score = self._calculate_stream_score(analyzed)
                    analyzed['score'] = score
                    analyzed_streams.append(analyzed)
                    logging.debug(f"Using cached data for stream {stream['id']}: {stream.get('name')} - Score: {score:.2f}")
                else:
                    # If we can't fetch cached data, analyze this stream
                    logging.warning(f"Could not fetch cached data for stream {stream['id']}, will analyze")
                    stream_row = {
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'stream_id': stream['id'],
                        'stream_name': stream.get('name', 'Unknown'),
                        'stream_url': stream.get('url', '')
                    }
                    analysis_params = self.config.get('stream_analysis', {})
                    analyzed = _analyze_stream_task(
                        stream_row,
                        ffmpeg_duration=analysis_params.get('ffmpeg_duration', 20),
                        idet_frames=analysis_params.get('idet_frames', 500),
                        timeout=analysis_params.get('timeout', 30),
                        retries=analysis_params.get('retries', 1),
                        retry_delay=analysis_params.get('retry_delay', 10),
                        config=sorter_config,
                        user_agent=analysis_params.get('user_agent', 'VLC/3.0.14')
                    )
                    self._update_stream_stats(analyzed)
                    score = self._calculate_stream_score(analyzed)
                    analyzed['score'] = score
                    analyzed_streams.append(analyzed)
            
            # Sort streams by score (highest first)
            self.progress.update(
                channel_id=channel_id,
                channel_name=channel_name,
                current=len(streams),
                total=len(streams),
                status='processing',
                step='Calculating scores',
                step_detail='Sorting streams by quality score'
            )
            analyzed_streams.sort(key=lambda x: x.get('score', 0), reverse=True)
            
            # Remove dead streams from the channel (unless it's a force check/global check)
            # During global checks, we want to give dead streams a chance to be revived
            if dead_stream_ids and not force_check:
                logging.warning(f"🔴 Removing {len(dead_stream_ids)} dead streams from channel {channel_name}")
                # Log which streams are being removed
                for stream_id in dead_stream_ids:
                    dead_stream = next((s for s in analyzed_streams if s['stream_id'] == stream_id), None)
                    if dead_stream:
                        logging.info(f"  - Removing dead stream {stream_id}: {dead_stream.get('stream_name', 'Unknown')}")
                analyzed_streams = [s for s in analyzed_streams if s['stream_id'] not in dead_stream_ids]
            elif dead_stream_ids and force_check:
                logging.info(f"Global check mode: keeping {len(dead_stream_ids)} dead streams to check for revival")
            
            if revived_stream_ids:
                logging.info(f"{len(revived_stream_ids)} streams were revived in channel {channel_name}")
            
            # Update channel with reordered streams
            self.progress.update(
                channel_id=channel_id,
                channel_name=channel_name,
                current=len(streams),
                total=len(streams),
                status='updating',
                step='Reordering streams',
                step_detail='Applying new stream order to channel'
            )
            reordered_ids = [s['stream_id'] for s in analyzed_streams]
            # Allow dead streams during force_check (global checks) to give them a second chance
            update_channel_streams(channel_id, reordered_ids, allow_dead_streams=force_check)
            
            # Verify the update was applied correctly
            self.progress.update(
                channel_id=channel_id,
                channel_name=channel_name,
                current=len(streams),
                total=len(streams),
                status='verifying',
                step='Verifying update',
                step_detail='Confirming stream order was applied'
            )
            time.sleep(0.5)  # Brief delay to ensure API has processed the update
            # Use include_streams=true to get correct order from database (workaround for Dispatcharr bug)
            updated_channel_data = fetch_data_from_url(f"{base_url}/api/channels/channels/{channel_id}/?include_streams=true")
            if updated_channel_data:
                # Extract stream IDs from full stream objects
                streams = updated_channel_data.get('streams', [])
                updated_stream_ids = [s['id'] if isinstance(s, dict) else s for s in streams]
                logging.info(f"🔍 DEBUG GET: Verification streams order: {updated_stream_ids[:10]}...")
                if updated_stream_ids == reordered_ids:
                    logging.info(f"✓ Verified: Channel {channel_name} streams reordered correctly")
                else:
                    logging.warning(f"⚠ Verification failed: Stream order mismatch for channel {channel_name}")
                    logging.warning(f"Expected: {reordered_ids[:5]}... Got: {updated_stream_ids[:5]}...")
            else:
                logging.warning(f"⚠ Could not verify stream update for channel {channel_name}")
            
            logging.info(f"✓ Channel {channel_name} checked and streams reordered")
            
            # Add changelog entry with stream stats
            if self.changelog:
                try:
                    # Prepare stream stats summary for changelog
                    stream_stats = []
                    for analyzed in analyzed_streams:
                        stream_stat = {
                            'stream_id': analyzed.get('stream_id'),
                            'stream_name': analyzed.get('stream_name'),
                            'score': round(analyzed.get('score', 0), 2),
                            'resolution': analyzed.get('resolution'),
                            'fps': analyzed.get('fps'),
                            'video_codec': analyzed.get('video_codec'),
                            'audio_codec': analyzed.get('audio_codec'),
                            'bitrate_kbps': analyzed.get('bitrate_kbps'),
                            'status': analyzed.get('status')
                        }
                        # Clean up N/A values for cleaner output
                        stream_stat = {k: v for k, v in stream_stat.items() if v not in [None, "N/A"]}
                        stream_stats.append(stream_stat)
                    
                    # Create changelog entry
                    changelog_details = {
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'total_streams': len(streams),
                        'streams_analyzed': len(analyzed_streams),
                        'success': True,
                        'stream_stats': stream_stats[:10]  # Limit to top 10 for brevity
                    }
                    
                    self.changelog.add_entry('stream_check', changelog_details)
                    logging.info(f"Changelog entry added for channel {channel_name}")
                except Exception as e:
                    logging.warning(f"Failed to add changelog entry: {e}")
            
            # Mark as completed with stream count and checked stream IDs
            self.check_queue.mark_completed(channel_id)
            self.update_tracker.mark_channel_checked(
                channel_id, 
                stream_count=len(streams),
                checked_stream_ids=current_stream_ids
            )
            
        except Exception as e:
            logging.error(f"Error checking channel {channel_id}: {e}", exc_info=True)
            self.check_queue.mark_failed(channel_id, str(e))
            
            # Add changelog entry for failed check
            if self.changelog:
                try:
                    # Try to get channel name if available
                    try:
                        channel_name = channel_data.get('name', f'Channel {channel_id}')
                    except:
                        channel_name = f'Channel {channel_id}'
                    
                    changelog_details = {
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'success': False,
                        'error': str(e)
                    }
                    self.changelog.add_entry('stream_check', changelog_details)
                except Exception as changelog_error:
                    logging.warning(f"Failed to add changelog entry for failed check: {changelog_error}")
        
        finally:
            self.checking = False
            self.progress.clear()
    
    def _calculate_stream_score(self, stream_data: Dict) -> float:
        """Calculate a quality score for a stream based on analysis."""
        # Dead streams always get a score of 0
        if self._is_stream_dead(stream_data):
            return 0.0
        
        weights = self.config.get('scoring.weights', {})
        score = 0.0
        
        # Bitrate score (0-1, normalized to typical range 1000-8000 kbps)
        bitrate = stream_data.get('bitrate_kbps', 0)
        if isinstance(bitrate, (int, float)) and bitrate > 0:
            bitrate_score = min(bitrate / 8000, 1.0)
            score += bitrate_score * weights.get('bitrate', 0.30)
        
        # Resolution score (0-1)
        resolution = stream_data.get('resolution', 'N/A')
        resolution_score = 0.0
        if 'x' in str(resolution):
            try:
                width, height = map(int, resolution.split('x'))
                # Score based on vertical resolution
                if height >= 1080:
                    resolution_score = 1.0
                elif height >= 720:
                    resolution_score = 0.7
                elif height >= 576:
                    resolution_score = 0.5
                else:
                    resolution_score = 0.3
            except (ValueError, AttributeError):
                pass
        score += resolution_score * weights.get('resolution', 0.25)
        
        # FPS score (0-1)
        fps = stream_data.get('fps', 0)
        if isinstance(fps, (int, float)) and fps > 0:
            fps_score = min(fps / 60, 1.0)
            score += fps_score * weights.get('fps', 0.15)
        
        # Codec score (0-1)
        codec = stream_data.get('video_codec', '').lower()
        codec_score = 0.0
        if codec:
            if 'h265' in codec or 'hevc' in codec:
                codec_score = 1.0 if self.config.get('scoring.prefer_h265', True) else 0.8
            elif 'h264' in codec or 'avc' in codec:
                codec_score = 0.8 if self.config.get('scoring.prefer_h265', True) else 1.0
            elif codec != 'n/a':
                codec_score = 0.5
        score += codec_score * weights.get('codec', 0.10)
        
        # Error penalty (0-1, inverted - fewer errors = higher score)
        error_score = 1.0
        if stream_data.get('status') != 'OK':
            error_score -= 0.5
        if stream_data.get('err_decode', False):
            error_score -= 0.2
        if stream_data.get('err_discontinuity', False):
            error_score -= 0.2
        if stream_data.get('err_timeout', False):
            error_score -= 0.3
        
        # Interlaced penalty
        if self.config.get('scoring.penalize_interlaced', True):
            interlaced = stream_data.get('interlaced_status', 'N/A')
            if 'interlaced' in str(interlaced).lower():
                error_score -= 0.1
        
        # Dropped frames penalty
        if self.config.get('scoring.penalize_dropped_frames', True):
            dropped = stream_data.get('frames_dropped', 0)
            decoded = stream_data.get('frames_decoded', 0)
            if isinstance(dropped, (int, float)) and isinstance(decoded, (int, float)) and decoded > 0:
                drop_rate = dropped / decoded
                if drop_rate > 0.01:  # More than 1% dropped
                    error_score -= min(drop_rate * 5, 0.3)  # Up to 0.3 penalty
        
        error_score = max(error_score, 0.0)
        score += error_score * weights.get('errors', 0.20)
        
        return round(score, 2)
    
    def get_status(self) -> Dict:
        """Get current service status."""
        queue_status = self.check_queue.get_status()
        progress = self.progress.get()
        
        return {
            'running': self.running,
            'checking': self.checking,
            'global_action_in_progress': self.global_action_in_progress,
            'enabled': self.config.get('enabled', True),
            'queue': queue_status,
            'progress': progress,
            'last_global_check': self.update_tracker.get_last_global_check(),
            'config': {
                'pipeline_mode': self.config.get('pipeline_mode'),
                'check_interval': self.config.get('check_interval'),
                'global_check_schedule': self.config.get('global_check_schedule'),
                'queue_settings': self.config.get('queue')
            }
        }
    
    def queue_channel(self, channel_id: int, priority: int = 10) -> bool:
        """Manually queue a channel for checking."""
        return self.check_queue.add_channel(channel_id, priority)
    
    def queue_channels(self, channel_ids: List[int], priority: int = 10) -> int:
        """Manually queue multiple channels for checking."""
        return self.check_queue.add_channels(channel_ids, priority)
    
    def clear_queue(self):
        """Clear the checking queue."""
        self.check_queue.clear()
        logging.info("Checking queue cleared")
    
    def trigger_check_updated_channels(self):
        """Trigger immediate check of channels with M3U updates.
        
        This method signals the scheduler to immediately process any channels
        that have been marked as updated, instead of waiting for the next
        scheduled check interval.
        """
        if self.running:
            logging.info("Triggering immediate check for updated channels")
            self.check_trigger.set()
        else:
            logging.warning("Cannot trigger check - service is not running")
    
    def update_config(self, updates: Dict):
        """Update service configuration and apply changes immediately."""
        # Sanitize user_agent if present
        if 'stream_analysis' in updates and 'user_agent' in updates['stream_analysis']:
            user_agent = updates['stream_analysis']['user_agent']
            # Sanitize user agent: allow alphanumeric, spaces, dots, slashes, dashes, underscores, parentheses
            import re
            sanitized = re.sub(r'[^a-zA-Z0-9 ./_\-()]+', '', str(user_agent))
            # Limit length to 200 characters
            sanitized = sanitized[:200].strip()
            if not sanitized:
                sanitized = 'VLC/3.0.14'  # Default fallback
            updates['stream_analysis']['user_agent'] = sanitized
            if sanitized != user_agent:
                logging.warning(f"User agent sanitized from '{user_agent}' to '{sanitized}'")
        
        # Log what's being updated
        config_changes = []
        if 'pipeline_mode' in updates:
            old_mode = self.config.get('pipeline_mode', 'pipeline_1_5')
            new_mode = updates['pipeline_mode']
            if old_mode != new_mode:
                config_changes.append(f"Pipeline mode: {old_mode} → {new_mode}")
        
        if 'global_check_schedule' in updates:
            schedule_changes = []
            schedule = updates['global_check_schedule']
            if 'hour' in schedule or 'minute' in schedule:
                old_hour = self.config.get('global_check_schedule.hour', 3)
                old_minute = self.config.get('global_check_schedule.minute', 0)
                new_hour = schedule.get('hour', old_hour)
                new_minute = schedule.get('minute', old_minute)
                if old_hour != new_hour or old_minute != new_minute:
                    schedule_changes.append(f"Time: {old_hour:02d}:{old_minute:02d} → {new_hour:02d}:{new_minute:02d}")
            if 'frequency' in schedule:
                old_freq = self.config.get('global_check_schedule.frequency', 'daily')
                new_freq = schedule['frequency']
                if old_freq != new_freq:
                    schedule_changes.append(f"Frequency: {old_freq} → {new_freq}")
            if 'enabled' in schedule:
                old_enabled = self.config.get('global_check_schedule.enabled', True)
                new_enabled = schedule['enabled']
                if old_enabled != new_enabled:
                    schedule_changes.append(f"Enabled: {old_enabled} → {new_enabled}")
            if schedule_changes:
                config_changes.append(f"Global check schedule: {', '.join(schedule_changes)}")
        
        # Apply the configuration update
        self.config.update(updates)
        
        # Log the changes
        if config_changes:
            logging.info(f"Configuration updated: {'; '.join(config_changes)}")
        else:
            logging.info("Configuration updated")
        
        # Signal that config has changed for immediate application
        if self.running:
            self.config_changed.set()
            # Wake up the scheduler immediately by setting the trigger
            # The scheduler will check config_changed and skip channel queueing
            self.check_trigger.set()
            logging.info("Configuration changes will be applied immediately")
        
        # Reload queue max size if changed
        if 'queue' in updates and 'max_size' in updates['queue']:
            # Can't resize existing queue, but will apply on next restart
            logging.info("Queue max size updated, will apply on next restart")
    
    def trigger_global_action(self):
        """Manually trigger a global action (Update, Match, Check all channels).
        
        This can be called at any time to perform a complete global action,
        regardless of the scheduled time.
        """
        if not self.running:
            logging.warning("Cannot trigger global action - service is not running")
            return False
        
        logging.info("Manual global action triggered")
        try:
            self._perform_global_action()
            self.update_tracker.mark_global_check()
            return True
        except Exception as e:
            logging.error(f"Failed to trigger global action: {e}")
            return False


# Global service instance
_service_instance = None
_service_lock = threading.Lock()

def get_stream_checker_service() -> StreamCheckerService:
    """Get or create the global stream checker service instance."""
    global _service_instance
    with _service_lock:
        if _service_instance is None:
            _service_instance = StreamCheckerService()
        return _service_instance
