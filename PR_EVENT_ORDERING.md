# Event Time Ordering Feature

## Overview

Automatically orders streams based on event times with support for custom time patterns, overflow channel management, and conflict detection. Streams are reordered so that upcoming/live events appear first, with past events pushed to the bottom. When multiple events would air simultaneously, conflicts are automatically resolved by moving events to overflow channels.

## Goals

- Automatically keep event channels sorted by relevance (live/upcoming first)
- Support custom regex patterns for any time format from any provider
- Detect and resolve scheduling conflicts using overflow channels
- Keep primary/backup stream pairs together across all operations
- Configurable per-channel with overflow settings
- Minimal API load with performance optimizations
- Interactive web UI with visual regex pattern builder

## Key Features

### 1. Custom Regex Pattern Builder
- **Visual drag-and-drop interface** - Categorize words from stream names (year, month, day, hour, minute, etc.)
- **Automatic pattern generation** - Builds regex with named capture groups
- **Live testing** - Test patterns against actual streams with immediate feedback
- **Per-channel patterns** - Each channel can have its own custom pattern
- **JavaScript/Python compatibility** - Auto-converts `(?<name>)` to `(?P<name>)` for Python

### 2. Overflow Conflict Management
- **Overlap detection** - Detects when events overlap in time (not just identical start times)
- **Smart distribution** - Moves conflicting events to overflow channels automatically
- **Primary/backup tracking** - Keeps stream pairs (same name/number) together as a unit
- **Multi-channel awareness** - Loads streams from all channels to prevent splitting pairs
- **Automatic return** - Streams return to main channel after configured duration

### 3. Performance Optimizations
- **Eliminated redundant API calls** - Reduced from 27+ calls to <10 per operation
- **Local validation sets** - Build valid stream lists from channel data instead of fetching all
- **Skip dead stream filtering** - Event ordering doesn't need URL validation
- **10-15x faster** - Reduced execution time from 47+ seconds to 3-4 seconds

## Changes

### Backend (`stream_checker_service.py`)

**Custom Pattern Parsing:**
- `parse_event_time_with_pattern()` - Parse times using custom regex patterns with named groups
- **Named groups supported**: `year`, `month`, `day`, `hour`, `minute`, `second`, `ampm`, `order`
- **Pattern conversion**: Automatically converts JavaScript-style `(?<name>)` to Python `(?P<name>)`

**Overlap Detection:**
- Events treated as time ranges (start_time + duration)
- Duration determined by `return_after_hours` setting
- Overlap formula: `event1.start < event2.end AND event2.start < event1.end`
- Only first event at a time slot stays, rest moved to overflow

**Multi-Channel Awareness:**
- Loads streams from main channel + all overflow channels before detecting conflicts
- Tracks `current_channel` for each stream to know where it currently is
- Removes streams from their actual location (not just main channel)
- Prevents primary/backup pairs from being split across channels

**Optimizations:**
- Uses stream names from channel endpoint (no individual stream fetches)
- Builds local valid stream sets instead of fetching all streams
- Added `allow_dead_streams=True` flag to skip URL validation
- Passes pre-fetched valid_stream_ids through function calls

**Safeguards:**
- Blocks reordering if stream set doesn't match (prevents data loss)
- Auto-removes duplicates when moving to overflow
- Warns if removal count doesn't match expected

### Backend (`web_api.py`)

API endpoints:
- `GET /api/event-ordering` - Get current config
- `PUT /api/event-ordering` - Update config with per-channel patterns
- `POST /api/event-ordering/trigger` - Manual trigger

### Frontend (`EventOrderingConfig.js`)

**New Features:**
- **Custom Regex Builder** - Visual drag-and-drop interface for building patterns
- **Stream sample parsing** - Tokenizes stream names into categorizable words
- **Category zones** - Drop zones for League, Order, Year, Month, Day, Hour, Minute, Second, AM/PM, Ignore
- **Live pattern testing** - Test generated pattern against all streams in real-time
- **Ordering preview** - Shows how streams will be reordered with channel assignments
- **Overflow settings** - Configure overflow channels and return duration per channel
- **Overflow preview** - Visual preview of conflict resolution with color-coded channels
- **Multi-channel stream loading** - Loads and displays streams from main + overflow channels
- **24-hour time format** - Consistent military time display throughout UI

**UI Improvements:**
- Multi-select overflow channel picker
- Return duration slider (1-24 hours)
- Channel column in ordering preview showing current location
- Color-coded channel chips (different colors per channel)
- Conflict visualization showing which events overlap

### Configuration Format

Stored in `data/channel_regex_config.json`:

```json
{
  "event_ordering": {
    "enabled": true,
    "frequency": 300,
    "channels": {
      "129": {
        "name": "🇺🇸 NBA Events",
        "pattern": "NBA\\s+(?<order>\\d+).*start:(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})\\s+(?<hour>\\d{2}):(?<minute>\\d{2})",
        "overflow_channel_ids": [130, 131, 132],
        "return_after_hours": 4
      }
    }
  }
}
```

**Fields:**
- `enabled` - Global enable/disable toggle
- `frequency` - Seconds between reordering runs (300 = 5 minutes)
- `channels` - Object mapping channel IDs to configurations
  - `name` - Channel display name
  - `pattern` - Custom regex pattern with named capture groups
  - `overflow_channel_ids` - Array of channel IDs for conflict overflow
  - `return_after_hours` - Hours before returning streams to main channel

## How It Works

### 1. Event Ordering Process

1. Scheduler checks every 60 seconds if reordering is due
2. For each configured channel:
   - Fetch streams from main channel
   - Parse event times using custom pattern or multi-format parser
   - Extract order number for tiebreaking (e.g., "NBA 01" → order=1)
   - Separate into upcoming/live (within 2 hours) and past events
   - Sort upcoming by (event_time, order_num) ASC
   - Sort past by (event_time, order_num) DESC
   - Update channel with reordered stream IDs

### 2. Overflow Conflict Resolution

1. After reordering, load streams from ALL channels (main + overflow)
2. Group streams by event number (e.g., all "NBA 01" streams together)
3. Build event time ranges (start + duration)
4. Sort events by start time
5. Keep first event, check subsequent events for overlap:
   - If overlap detected → move entire event (all streams) to overflow channel
   - Track source channel for proper removal
6. Distribute conflicts round-robin across overflow channels
7. Track moved streams with return timestamp
8. Return streams to main channel after configured hours

### 3. Primary/Backup Pair Tracking

- Streams grouped by `item_num` extracted from name (e.g., "NBA 05" → 5)
- All streams with same item_num treated as one logical event
- When moving to overflow, entire event group moves together
- Prevents situations like:
  - ❌ NBA 05 Primary → Main Channel
  - ❌ NBA 05 Backup → Overflow Channel
- Ensures:
  - ✅ NBA 05 Primary → Same Channel
  - ✅ NBA 05 Backup → Same Channel

## Performance Metrics

**Before optimizations:**
- Execution time: 47-90+ seconds for 2 channels
- API calls: 27+ individual stream fetches + multiple validation fetches
- Bottleneck: `get_valid_stream_ids()` called 7+ times

**After optimizations:**
- Execution time: 3-4 seconds for 2 channels
- API calls: <10 per operation
- Improvement: **10-15x faster**

**Optimizations applied:**
1. Use stream names from channel endpoint (saved 27+ API calls)
2. Build local valid sets from channel data
3. Skip dead stream filtering for event ordering
4. Cache and pass valid_stream_ids through calls

## Testing

### Manual API Trigger
```bash
curl -X POST http://localhost:5000/api/event-ordering/trigger
```

### Web UI Testing
1. Navigate to Settings → Event Time Ordering Configuration
2. Select channel and sample stream
3. Categorize time components using drag-and-drop
4. Generate pattern and test against all streams
5. Configure overflow channels and return duration
6. Save configuration
7. Use "Trigger Event Ordering" button in Settings

### Checking Logs
```bash
docker compose logs stream-checker --tail=100 | grep -E "(Conflict|overflow|Reordered)"
```

Expected output:
```
Checking overflow conflicts for 🇺🇸 NBA Events with 3 overflow channels
Processing 8 unique events for overflow check
Conflict: Event 8 (02:20-06:20) overlaps with event 5 (00:50-04:50). Moving 2 streams to channel 130
Found 5 conflicting events out of 8 total events
✓ Reordered 🇺🇸 NBA Events: 3 upcoming
```

## Configuration Examples

### UFC Events
```json
"channels": {
  "100": {
    "name": "UFC Events",
    "pattern": "UFC\\s+(?<order>\\d+).*?(?<month>\\w+)\\s+(?<day>\\d{1,2}).*?(?<hour>\\d{1,2}):(?<minute>\\d{2})\\s*(?<ampm>[AP]M)",
    "overflow_channel_ids": [101, 102],
    "return_after_hours": 6
  }
}
```

### Soccer Matches
```json
"channels": {
  "200": {
    "name": "⚽ Premier League",
    "pattern": "(?<order>\\d+).*?start:(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})\\s+(?<hour>\\d{2}):(?<minute>\\d{2})",
    "overflow_channel_ids": [201],
    "return_after_hours": 3
  }
}
```

### PPV Events
```json
"channels": {
  "300": {
    "name": "PPV Events",
    "pattern": "PPV\\s+(?<order>\\d+).*?(?<month>\\w+)\\s+(?<day>\\d+)\\s+(?<hour>\\d+)(?<ampm>am|pm)",
    "overflow_channel_ids": [301, 302, 303],
    "return_after_hours": 8
  }
}
```

## Troubleshooting

### Pattern Not Matching
- Check pattern uses Python syntax: `(?P<name>)` not `(?<name>)`
- System auto-converts, but verify in logs
- Use test results panel to see what's matching
- Ensure all required named groups present (at least hour/minute for time)

### Streams Split Across Channels
- Check logs for "Removed X streams from channel Y"
- Verify item_num extraction is consistent (same regex for all channels)
- Ensure overflow channels configured before first run
- Primary/backup pairs should have identical names

### Performance Issues
- Check if `allow_dead_streams=True` is set (should be in event ordering calls)
- Verify not fetching all streams globally
- Look for "get_valid_stream_ids" in logs (should not appear for event ordering)
- Expected: 3-5 seconds for 2 channels with 20+ streams

### Conflicts Not Detected
- Verify `return_after_hours` represents actual event duration
- Check time parsing is correct (logs show parsed times)
- Ensure overlap formula is working: event1.start < event2.end AND event2.start < event1.end
- Look for "Found X conflicting events" in logs

## Future Improvements

- [x] Custom regex pattern builder per channel
- [x] Overflow conflict detection and management
- [x] Performance optimizations
- [x] Primary/backup pair tracking
- [x] Multi-channel stream loading in UI
- [x] 24-hour time format
- [ ] Add last run time display in UI
- [ ] Show next scheduled run countdown
- [ ] Per-channel dry-run mode
- [ ] Conflict resolution history/logs
- [ ] Support for timezone handling
- [ ] Option to disable past events entirely
- [ ] Webhook notifications for conflicts
- [ ] Analytics dashboard for conflict patterns
