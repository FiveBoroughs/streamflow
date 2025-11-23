# Event Time Ordering Feature

## Overview

Adds automatic stream ordering based on event times for channels containing live events. Streams are reordered so that upcoming and live events appear first, with past events pushed to the bottom.

## Goals

- Automatically keep event channels sorted by relevance (live/upcoming first)
- Support multiple time formats from different providers
- Configurable per-channel with adjustable frequency
- Minimal API load with smart scheduling
- Web UI for easy configuration

## Changes

### Backend (`stream_checker_service.py`)

- **Multi-format time parsing** - `parse_event_time_multi_format()` supports:
  - `start:2025-11-22 18:55:00` (standard format with date/time)
  - `/ Nov 22 : 8PM UK` (date with timezone indicator)
  - `- 7PM Event Name` (time only, assumes today)

- **Channel-specific ordering** - `apply_event_time_ordering_for_channels()`:
  - Orders only configured channels
  - Separates streams into upcoming/live vs past
  - Preserves provider order for ties (Event 01 before Event 02 at same time)
  - Falls back to item number ordering when no times present

- **Scheduled execution** - `_check_event_ordering_schedule()`:
  - Runs at configurable frequency (default 5 minutes)
  - Reads config from `channel_regex_config.json`
  - Independent of global action schedule

### Backend (`web_api.py`)

New API endpoints:
- `GET /api/event-ordering` - Get current config
- `PUT /api/event-ordering` - Update config
- `POST /api/event-ordering/trigger` - Manual trigger

### Frontend

- **API client** (`api.js`) - `eventOrderingAPI` with get/update/trigger
- **Settings UI** (`AutomationSettings.js`):
  - Enable/disable toggle
  - Frequency setting (seconds)
  - Multi-select channel picker
  - "Trigger Now" button
  - Info about supported time formats

## Configuration

Stored in `channel_regex_config.json`:

```json
{
  "event_ordering": {
    "enabled": true,
    "frequency": 300,
    "channels": [1, 2, 3, 4]
  }
}
```

- **enabled** - Turn feature on/off
- **frequency** - Seconds between reordering (300 = 5 minutes)
- **channels** - List of channel IDs to order

## How It Works

1. Scheduler checks every 60 seconds if reordering is due
2. For each configured channel:
   - Fetch all streams
   - Parse event times from stream names
   - Separate into upcoming/live (within 2 hours) and past
   - Sort upcoming by time ASC, past by time DESC
   - Update channel with new stream order
3. Log results

## Testing

Manual trigger via API:
```bash
curl -X POST http://localhost:5000/api/event-ordering/trigger
```

Or use "Trigger Now" button in web UI under Configuration > Event Time Ordering.

## Future Improvements

- [ ] Add last run time display in UI
- [ ] Show next scheduled run
- [ ] Per-channel time format hints
- [ ] Support more time formats as discovered
- [ ] Option to disable past events entirely
