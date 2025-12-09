# API Documentation

All API endpoints are accessible at `http://localhost:3000/api/`

## Stream Checker Endpoints

### Get Status
```
GET /api/stream-checker/status
```
Returns service status, statistics, and queue information.

**Response:**
```json
{
  "running": true,
  "current_channel": "Channel Name",
  "queue_size": 5,
  "statistics": {
    "total_checked": 150,
    "total_failed": 3,
    "total_improved": 120
  }
}
```

### Start Service
```
POST /api/stream-checker/start
```
Starts the stream checking service.

### Stop Service
```
POST /api/stream-checker/stop
```
Stops the stream checking service.

### Get Queue
```
GET /api/stream-checker/queue
```
Returns current queue of channels pending check.

### Add to Queue
```
POST /api/stream-checker/queue/add
Content-Type: application/json

{
  "channel_ids": [1, 2, 3]
}
```
Adds specific channels to the checking queue.

### Clear Queue
```
POST /api/stream-checker/queue/clear
```
Removes all pending checks from the queue.

### Get Configuration
```
GET /api/stream-checker/config
```
Returns current stream checker configuration.

### Update Configuration
```
PUT /api/stream-checker/config
Content-Type: application/json

{
  "enabled": true,
  "global_check_schedule": {
    "enabled": true,
    "frequency": "daily",
    "hour": 3,
    "minute": 0
  },
  "queue": {
    "check_on_update": true,
    "max_channels_per_run": 50
  },
  "scoring": {
    "weights": {
      "bitrate": 0.30,
      "resolution": 0.25,
      "fps": 0.15,
      "codec": 0.10,
      "errors": 0.20
    }
  }
}
```
Updates stream checker configuration.

**Configuration Options:**
- `enabled` - Enable/disable the stream checker service
- `global_check_schedule.enabled` - Enable scheduled global checks of all channels
- `global_check_schedule.frequency` - Schedule frequency ('daily' or 'monthly')
- `global_check_schedule.hour` - Hour to run check (0-23)
- `global_check_schedule.minute` - Minute to run check (0-59)
- `queue.check_on_update` - Automatically queue channels for checking when M3U playlists are updated
- `queue.max_channels_per_run` - Maximum number of channels to check per run
- `scoring.weights` - Weights for different quality factors in stream scoring

### Get Progress
```
GET /api/stream-checker/progress
```
Returns real-time progress of current check operation.

### Check Channel
```
POST /api/stream-checker/check-channel
Content-Type: application/json

{
  "channel_id": 123
}
```
Queues a specific channel for checking with high priority.

**Response:**
```json
{
  "message": "Channel 123 queued for immediate checking"
}
```

### Check Single Channel (Synchronous)
```
POST /api/stream-checker/check-single-channel
Content-Type: application/json

{
  "channel_id": 123
}
```
Immediately checks a specific channel synchronously and returns detailed results.

**Response:**
```json
{
  "success": true,
  "channel_id": 123,
  "channel_name": "Example Channel",
  "stats": {
    "total_streams": 15,
    "dead_streams": 2,
    "avg_resolution": "1920x1080",
    "avg_bitrate": "5000 kbps",
    "stream_details": [
      {
        "stream_id": 1001,
        "stream_name": "Stream 1",
        "resolution": "1920x1080",
        "bitrate": "6000 kbps",
        "video_codec": "h264",
        "fps": "30.0"
      }
    ]
  }
}
```

**Note:** This endpoint performs the check immediately and waits for completion, while `/check-channel` queues the check and returns immediately.

### Mark Updated
```
POST /api/stream-checker/mark-updated
Content-Type: application/json

{
  "channel_ids": [1, 2, 3]
}
```
Marks channels as updated and needing check.

## Automation Endpoints

### Get Automation Status
```
GET /api/automation/status
```
Returns automation service status and configuration.

### Start Automation
```
POST /api/automation/start
```
Starts the automation service.

### Stop Automation
```
POST /api/automation/stop
```
Stops the automation service.

### Get Configuration
```
GET /api/automation/config
```
Returns automation configuration.

### Update Configuration
```
PUT /api/automation/config
Content-Type: application/json

{
  "playlist_update_interval_minutes": 5,
  "autostart_automation": false,
  "enabled_m3u_accounts": [],
  "enabled_features": {
    "auto_playlist_update": true,
    "auto_stream_discovery": true,
    "changelog_tracking": true
  }
}
```
Updates automation configuration.

**Configuration Options:**
- `playlist_update_interval_minutes` - How often to check for playlist updates
- `autostart_automation` - Whether to automatically start the automation service on server startup
- `enabled_m3u_accounts` - Array of M3U account IDs to enable (empty array means all accounts)
- `enabled_features.auto_playlist_update` - Enable automatic playlist updates
- `enabled_features.auto_stream_discovery` - Enable automatic stream discovery via regex
- `enabled_features.changelog_tracking` - Track changes in the changelog

### Discover Streams
```
POST /api/automation/discover-streams
```
Manually triggers stream discovery cycle.

## Channel Endpoints

### Get Channels
```
GET /api/channels
```
Returns list of all channels.

**Query Parameters:**
- `page` - Page number (default: 1)
- `per_page` - Results per page (default: 50)

### Get Channel Details
```
GET /api/channels/{channel_id}
```
Returns details for a specific channel.

### Get Channel Statistics
```
GET /api/channels/{channel_id}/stats
```
Returns comprehensive statistics for a specific channel.

**Response:**
```json
{
  "channel_id": "123",
  "channel_name": "Example Channel",
  "logo_id": 456,
  "total_streams": 25,
  "dead_streams": 2,
  "most_common_resolution": "1920x1080",
  "average_bitrate": 8500000,
  "resolutions": {
    "1920x1080": 18,
    "1280x720": 7
  }
}
```

**Statistics include:**
- `total_streams` - Count of all streams for the channel
- `dead_streams` - Count of non-functional streams
- `most_common_resolution` - Most frequently occurring resolution
- `average_bitrate` - Average bitrate across all streams (in bps)
- `resolutions` - Distribution of resolutions across streams

### Get Channel Streams
```
GET /api/channels/{channel_id}/streams
```
Returns all streams for a specific channel.

## Regex Pattern Endpoints

### Get Patterns
```
GET /api/regex-patterns
```
Returns all configured regex patterns.

### Add Pattern
```
POST /api/regex-patterns
Content-Type: application/json

{
  "pattern": "^HD.*Sports$",
  "channel_id": 123,
  "enabled": true
}
```
Adds a new regex pattern.

### Update Pattern
```
PUT /api/regex-patterns/{pattern_id}
Content-Type: application/json

{
  "pattern": "^HD.*Sports$",
  "channel_id": 123,
  "enabled": true
}
```
Updates an existing pattern.

### Delete Pattern
```
DELETE /api/regex-patterns/{pattern_id}
```
Deletes a regex pattern.

### Test Pattern
```
POST /api/regex-patterns/test
Content-Type: application/json

{
  "pattern": "^HD.*Sports$"
}
```
Tests a regex pattern against available streams.

## Changelog Endpoints

### Get Changelog
```
GET /api/changelog?days=7
```
Returns activity history with structured entries.

**Query Parameters:**
- `days` - Number of days to retrieve (default: 7, options: 1, 7, 30, 90)

**Response:**
```json
[
  {
    "timestamp": "2024-01-15T10:30:00.000Z",
    "action": "single_channel_check",
    "details": {
      "channel_id": 123,
      "channel_name": "Example Channel",
      "total_streams": 15,
      "dead_streams": 2,
      "avg_resolution": "1920x1080",
      "avg_bitrate": "5000 kbps"
    },
    "subentries": [
      {
        "group": "check",
        "items": [
          {
            "type": "check",
            "channel_id": 123,
            "channel_name": "Example Channel",
            "stats": {
              "total_streams": 15,
              "dead_streams": 2,
              "avg_resolution": "1920x1080",
              "avg_bitrate": "5000 kbps",
              "stream_details": [...]
            }
          }
        ]
      }
    ]
  }
]
```

**Action Types:**
- `playlist_update_match` - Playlist update with stream matching and channel checks
- `global_check` - Scheduled global check of all channels
- `single_channel_check` - Individual channel check
- `playlist_refresh` - M3U playlist refresh (legacy)
- `streams_assigned` - Stream assignment to channels (legacy)

**Subentry Groups:**
- `update_match` - Streams added to channels during playlist updates
- `check` - Channel check results with statistics

**Note:** The changelog endpoint merges entries from both the automation manager and stream checker, sorted by timestamp (newest first).

## Health Check

### Health Status
```
GET /api/health
```
Returns service health status.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "services": {
    "automation": "running",
    "stream_checker": "running"
  }
}
```

## Scheduling & EPG-Based Checks

### Get Scheduled Event Processor Status
```
GET /api/scheduling/processor/status
```
Get the status of the background thread that processes scheduled EPG events.

**Response:**
```json
{
  "running": true,
  "thread_alive": true
}
```

### Start Scheduled Event Processor
```
POST /api/scheduling/processor/start
```
Start the background thread for processing scheduled events. The processor runs every 60 seconds and automatically executes channel checks for due scheduled events.

**Response:**
```json
{
  "message": "Scheduled event processor started"
}
```

**Note:** The processor is automatically started when the Flask app starts (if wizard is complete).

### Stop Scheduled Event Processor
```
POST /api/scheduling/processor/stop
```
Stop the background thread for processing scheduled events.

**Response:**
```json
{
  "message": "Scheduled event processor stopped"
}
```

### Process Due Events (Manual)
```
POST /api/scheduling/process-due-events
```
Manually trigger processing of all scheduled events that are due. This is handled automatically by the background thread, but can be called manually if needed.

**Response:**
```json
{
  "message": "Processed 2 event(s), 2 successful",
  "processed": 2,
  "successful": 2,
  "results": [
    {
      "event_id": "abc123",
      "channel_name": "ESPN HD",
      "program_title": "Monday Night Football",
      "success": true
    }
  ]
}
```

**How It Works:**
1. The background thread checks for due scheduled events every 60 seconds
2. When an event is due (current time >= check_time), it executes a channel check
3. The channel check includes the program name in the changelog entry
4. After successful execution, the event is automatically deleted from the schedule
5. Any errors are logged but don't stop the processor

**Architecture:**
- Runs as a daemon thread in the Flask application
- Thread-safe using locks in SchedulingService
- Survives Flask reloads in development mode (daemon thread)
- Automatically starts with Flask app (when wizard is complete)
