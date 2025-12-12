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
Performs a complete channel refresh similar to a global check but scoped to a single channel. This operation:
- Refreshes M3U playlists for accounts associated with the channel
- Re-discovers and assigns matching streams (including previously dead ones)
- Force checks all streams, bypassing the 2-hour immunity period
- Revives streams that are now working
- Removes streams that are still dead

This is a synchronous operation that returns detailed results when complete.

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

### Auto-Create Rules

Auto-create rules automatically create scheduled events when EPG programs match regex patterns.

#### Get All Auto-Create Rules
```
GET /api/scheduling/auto-create-rules
```
Get all auto-create rules.

**Response:**
```json
[
  {
    "id": "rule-uuid",
    "name": "Breaking News",
    "channel_id": 123,
    "channel_name": "CNN HD",
    "channel_logo_url": "http://...",
    "tvg_id": "cnn.us",
    "regex_pattern": "^Breaking News",
    "minutes_before": 5,
    "created_at": "2024-01-01T12:00:00+00:00"
  }
]
```

#### Create Auto-Create Rule
```
POST /api/scheduling/auto-create-rules
Content-Type: application/json

{
  "name": "Breaking News Alert",
  "channel_id": 123,
  "regex_pattern": "^Breaking News",
  "minutes_before": 5
}
```
Create a new auto-create rule. The system will immediately match existing EPG programs to the new rule.

**Response:** Returns the created rule object (201 Created)

#### Update Auto-Create Rule
```
PUT /api/scheduling/auto-create-rules/{rule_id}
Content-Type: application/json

{
  "name": "Updated Rule Name",
  "channel_id": 123,
  "regex_pattern": "^Updated Pattern",
  "minutes_before": 10
}
```
Update an existing auto-create rule. All fields are optional - only provided fields will be updated. Existing scheduled events created by this rule will be deleted and new ones will be created based on the updated rule.

**Response:** Returns the updated rule object (200 OK)

#### Delete Auto-Create Rule
```
DELETE /api/scheduling/auto-create-rules/{rule_id}
```
Delete an auto-create rule and all scheduled events created by it.

**Response:**
```json
{
  "message": "Rule deleted"
}
```

#### Test Auto-Create Rule
```
POST /api/scheduling/auto-create-rules/test
Content-Type: application/json

{
  "channel_id": 123,
  "regex_pattern": "^Breaking News"
}
```
Test a regex pattern against current EPG programs for a channel without creating a rule.

**Response:**
```json
{
  "matches": [
    {
      "title": "Breaking News: Market Update",
      "start_time": "2024-01-01T14:00:00+00:00",
      "end_time": "2024-01-01T14:30:00+00:00"
    }
  ]
}
```

### Scheduled Events Processor

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
Start the background thread for processing scheduled events. The processor checks for due events every 30 seconds using an event-based approach for better responsiveness. When new events are created, the processor wakes up immediately to check them.

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
1. The background thread checks for due scheduled events every 30 seconds using an event-based approach
2. When an event is created, the thread is woken up immediately to check for new due events
3. When an event is due (current time >= check_time), it executes a channel check
4. The channel check includes the program name in the changelog entry
5. After successful execution, the event is automatically deleted from the schedule
6. Any errors are logged but don't stop the processor

**Architecture:**
- Runs as a daemon thread in the Flask application
- Uses threading.Event for responsive wake-up (inspired by Global Action scheduler)
- Thread-safe using locks in SchedulingService
- Survives Flask reloads in development mode (daemon thread)
- Automatically starts with Flask app (when wizard is complete)
- Check interval reduced from 60s to 30s for better responsiveness

### Get EPG Refresh Processor Status
```
GET /api/scheduling/epg-refresh/status
```
Get the status of the background thread that periodically refreshes EPG data and creates scheduled events from auto-create rules.

**Response:**
```json
{
  "running": true,
  "thread_alive": true
}
```

### Start EPG Refresh Processor
```
POST /api/scheduling/epg-refresh/start
```
Start the background thread for periodic EPG refresh. The processor fetches EPG data based on the configured refresh interval (default: 60 minutes, minimum: 5 minutes) and automatically matches programs to auto-create rules.

**Response:**
```json
{
  "message": "EPG refresh processor started"
}
```

**Note:** The processor is automatically started when the Flask app starts (if wizard is complete).

### Stop EPG Refresh Processor
```
POST /api/scheduling/epg-refresh/stop
```
Stop the background thread for EPG refresh.

**Response:**
```json
{
  "message": "EPG refresh processor stopped"
}
```

### Trigger EPG Refresh Manually
```
POST /api/scheduling/epg-refresh/trigger
```
Manually trigger an immediate EPG refresh, bypassing the configured interval. The processor will wake up and fetch EPG data immediately.

**Response:**
```json
{
  "message": "EPG refresh triggered"
}
```

**How It Works:**
1. The background thread starts 5 seconds after application startup
2. It fetches EPG data from Dispatcharr's `/api/epg/grid/` endpoint
3. The `match_programs_to_rules()` function is called automatically
4. Scheduled events are created for programs matching auto-create rules
5. The processor waits for the configured interval before the next refresh
6. On error, it waits 5 minutes before retrying

**Configuration:**
- Refresh interval configured via `epg_refresh_interval_minutes` in scheduling config
- Default: 60 minutes
- Minimum: 5 minutes
- Can be adjusted dynamically via configuration update

**Architecture:**
- Runs as a daemon thread in the Flask application
- Uses threading.Event for responsive wake-up
- Constants defined for timeouts and delays
- Critical error handling with graceful shutdown
- Automatically starts with Flask app (when wizard is complete)
