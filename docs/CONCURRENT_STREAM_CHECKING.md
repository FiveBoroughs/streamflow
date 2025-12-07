# Concurrent Stream Checking with Celery

## Overview

StreamFlow now supports concurrent stream checking using Celery and Redis. This allows multiple streams to be analyzed simultaneously, significantly reducing the time required for channel checks while respecting M3U account stream limits.

## Architecture

The concurrent stream checking system consists of several components:

### 1. Celery Task Queue
- **celery_app.py**: Celery application configuration
- **celery_tasks.py**: Stream checking task definitions
- Tasks are executed by Celery workers running in separate containers

### 2. Redis Backend
- Serves as the message broker for Celery tasks
- Stores UDI (Universal Data Index) data for distributed access
- Tracks concurrency state for M3U accounts and global limits

### 3. Concurrency Management
- **concurrency_manager.py**: Manages concurrent task limits
- Respects per-M3U account `max_streams` limits
- Enforces configurable global concurrent stream limit
- Automatic cleanup of stale tasks

### 4. Stream Checker Service
- **stream_checker_service.py**: Orchestrates stream checking
- Routes to concurrent or sequential checking based on configuration
- Dispatches tasks to Celery workers
- Aggregates results and reorders streams

## Configuration

### Via Web UI (Configuration Page)

The Configuration page (formerly Automation Settings) now provides UI controls for configuring concurrent stream checking:

1. Navigate to the **Configuration** page in the web interface
2. Scroll to the **Concurrent Stream Checking** section
3. Configure:
   - **Enable Concurrent Checking**: Toggle to enable/disable Celery-based concurrent checking
   - **Global Concurrent Limit**: Set the maximum number of concurrent stream checks (0-100, 0 = unlimited)
   - **Stagger Delay**: Set the delay in seconds between starting each worker (0-10 seconds, recommended: 0.5-2)

Changes are saved to `/app/data/stream_checker_config.json` when you click "Save Settings".

**Note**: Lowering the global limit helps reduce load on streaming providers and can prevent connection resets when multiple workers access the same provider simultaneously. The stagger delay prevents all workers from starting stream checks at the exact same time, which further reduces the risk of overwhelming streaming providers.

### Global Concurrent Limit

Configure the maximum number of concurrent stream checks globally:

```json
{
  "concurrent_streams": {
    "enabled": true,
    "global_limit": 10,
    "stagger_delay": 1.0
  }
}
```

- `enabled`: Set to `false` to use sequential checking instead
- `global_limit`: Maximum concurrent streams globally (0 = unlimited)
- `stagger_delay`: Delay in seconds between dispatching tasks (default: 1.0 second). This prevents all workers from connecting to streams simultaneously, reducing the load spike on streaming providers.

### M3U Account Limits

Each M3U account has a `max_streams` field that defines how many concurrent streams from that playlist can be checked simultaneously:

- **0 = Unlimited**: No concurrent stream limit for this account
- **N > 0**: Maximum N concurrent streams from this account

This information is fetched from the `/api/m3u/accounts/` endpoint.

**Examples:**
- Account with `max_streams: 0` → Can check any number of streams concurrently (unlimited)
- Account with `max_streams: 3` → Can check up to 3 streams concurrently
- Account with `max_streams: 10` → Can check up to 10 streams concurrently

### Configuration File

Settings are stored in `/app/data/stream_checker_config.json`:

```json
{
  "concurrent_streams": {
    "enabled": true,
    "global_limit": 10
  },
  "pipeline_mode": "pipeline_1_5",
  "stream_analysis": {
    "ffmpeg_duration": 30,
    "timeout": 30,
    "retries": 1,
    "retry_delay": 10,
    "user_agent": "VLC/3.0.14"
  }
}
```

## Deployment

### All-In-One Container (Default)

The All-In-One container includes all required services in a single container:

```yaml
services:
  stream-checker:
    image: ghcr.io/krinkuto11/streamflow:latest
    ports:
      - "5000:5000"
    volumes:
      - /srv/docker/databases/stream_checker:/app/data
    environment:
      - REDIS_HOST=localhost
      - REDIS_PORT=6379
      - REDIS_DB=0
      # Other environment variables...
```

The container automatically starts:
- Redis server (localhost:6379)
- Celery worker with 4 concurrent workers
- Flask API (port 5000)

All services are managed by Supervisor within the container.

### Environment Variables

Required environment variables (set via .env file or docker-compose.yml):

```bash
REDIS_HOST=localhost          # Redis hostname (localhost for All-In-One)
REDIS_PORT=6379               # Redis port
REDIS_DB=0                    # Redis database number
DISPATCHARR_BASE_URL=...      # Dispatcharr API URL
DISPATCHARR_USER=...          # Dispatcharr username
DISPATCHARR_PASS=...          # Dispatcharr password
```

> **Note**: In the All-In-One container, Redis runs on localhost within the container. No external Redis server is needed.

## How It Works

### Concurrent Stream Checking Flow

1. **Channel Queued**: A channel is added to the checking queue
2. **Fetch Streams**: All streams for the channel are fetched
3. **Group by Account**: Streams are grouped by their M3U account
4. **Dispatch Loop**: For each stream to check:
   - **Check Limits**: System checks if task can start based on:
     - Global concurrent limit
     - Per-account concurrent limits (from `max_streams`)
   - **Dispatch Task**: Only if limits allow, task is dispatched to Celery
   - **Register Task**: Task is registered immediately after dispatch
   - **Stagger**: Optional delay before dispatching next task (prevents simultaneous starts)
5. **Wait for Completion**: Main service waits for all tasks to complete
6. **Aggregate Results**: Stream analysis results are collected
7. **Calculate Scores**: Quality scores are calculated for each stream
8. **Reorder Streams**: Streams are sorted by score (highest first)
9. **Update Channel**: Channel is updated with reordered stream list

**Note**: The system checks limits **before** dispatching tasks, preventing unnecessary task creation and revocation. This approach eliminates the "Tasks flagged as revoked" messages that occurred in earlier versions.

### Concurrency Tracking

The `ConcurrencyManager` tracks:

```python
# Global counter
streamflow:concurrency:global = 5

# Per-account counters
streamflow:concurrency:account:123 = 2
streamflow:concurrency:account:456 = 3

# Task mappings (for cleanup)
streamflow:concurrency:task:task-uuid = {
    "m3u_account_id": 123,
    "stream_id": 789,
    "start_time": 1638360000
}
```

### Task Lifecycle

1. **Task Created**: Celery creates task with unique ID
2. **Start Registered**: Counters are incremented
   - Global counter: +1
   - Account counter: +1 (if applicable)
3. **Task Executes**: Stream is analyzed with ffmpeg
4. **Result Returned**: Analysis data sent back
5. **End Registered**: Counters are decremented
   - Global counter: -1
   - Account counter: -1 (if applicable)

### Error Handling

- **Task Failures**: Automatic retry up to 2 times with 5-second delay
- **Stale Tasks**: Cleanup after 1 hour
- **Counter Inconsistency**: Manual reset endpoint available
- **Redis Unavailable**: Graceful fallback to file-based UDI storage

## API Endpoints

### Get Concurrency Status

```http
GET /api/stream-checker/concurrency/status
```

Response:
```json
{
  "enabled": true,
  "global": {
    "limit": 10,
    "current_count": 5
  },
  "accounts": {
    "123": {
      "name": "Account 1",
      "max_streams": 3,
      "current_count": 2
    }
  }
}
```

### Get Concurrency Config

```http
GET /api/stream-checker/concurrency/config
```

Response:
```json
{
  "enabled": true,
  "global_limit": 10
}
```

### Update Concurrency Config

```http
PUT /api/stream-checker/concurrency/config
Content-Type: application/json

{
  "enabled": true,
  "global_limit": 15
}
```

### Reset Concurrency Counters

```http
POST /api/stream-checker/concurrency/reset
```

Use this if counters become inconsistent due to crashes or errors.

### Check Celery Health

```http
GET /api/stream-checker/celery/health
```

Response:
```json
{
  "healthy": true,
  "message": "Celery worker is responding",
  "result": {
    "status": "ok",
    "message": "Celery worker is healthy"
  }
}
```

## Performance

### Sequential vs Concurrent

**Sequential Mode** (legacy):
- Checks streams one at a time
- Channel with 10 streams @ 30s each = 5 minutes
- Predictable resource usage

**Concurrent Mode** (new):
- Checks streams in parallel
- Channel with 10 streams @ 30s each = ~30 seconds (with limit of 10)
- Higher resource usage, faster completion
- Respects M3U account limits

### Recommended Settings

| Scenario | Global Limit | Worker Concurrency |
|----------|--------------|-------------------|
| Low Power Server | 5 | 2 |
| Standard Server | 10 | 4 |
| High Performance | 20 | 8 |

## Monitoring

### Web UI

The Stream Checker page in the web UI provides comprehensive information about concurrent stream checking:

#### Status Cards
- **Status**: Shows if the service is running and the checking mode (concurrent/sequential)
- **Queue Size**: Number of channels pending check
- **Completed**: Total channels checked successfully
- **Failed**: Channels that failed during checking

#### Concurrent Checking Status Card (when enabled)
- **Global Concurrent Streams**: Current number vs limit with visual progress indicator
- **Per-Account Activity**: Shows which M3U accounts have active concurrent checks
- **Progress Bar**: Visual representation of concurrent capacity usage

#### Current Check Progress (when checking)
- **Channel Information**: Name and ID of channel being checked
- **Stream Progress**: Current stream number out of total
- **Overall Progress**: Percentage complete with progress bar
- **Current Step**: Shows detailed step information:
  - Fetching streams
  - Analyzing streams concurrently
  - Calculating scores
  - Reordering streams
  - Verifying update
- **Concurrent Mode Indicator**: Shows when concurrent mode is active with capacity limit

#### Pipeline Information
- Displays active pipeline mode
- Shows scheduled global action time (if configured)
- Last global action timestamp

All information auto-refreshes every 10 seconds to provide real-time status updates.

### Check Concurrency Status

Use the web UI or API to monitor:
- Current global concurrent count
- Per-account concurrent counts
- Task queue size

### Celery Worker Status

View all logs from the All-In-One container (all services including Celery):
```bash
docker compose logs -f stream-checker
# All logs from Redis, Celery, Flask API, and Supervisor are forwarded to stdout
```

View specific time ranges:
```bash
# Last 10 minutes
docker compose logs --since 10m stream-checker

# Last 100 lines
docker compose logs --tail 100 stream-checker
```

All services (Redis, Celery workers, Flask API) output to the container's stdout/stderr, making it easy to monitor all activity in one place.

### Redis Status

Check Redis health within the All-In-One container:
```bash
docker compose exec stream-checker redis-cli ping
# Should respond: PONG
```

### Supervisor Status

Check status of all services within the container:
```bash
docker compose exec stream-checker supervisorctl status
# Shows: redis, celery-worker, flask-api
```

## Troubleshooting

### Workers Not Processing Tasks

1. Check all services are running in the container:
   ```bash
   docker compose exec stream-checker supervisorctl status
   ```

2. Check all logs (includes Celery, Redis, Flask API):
   ```bash
   docker compose logs --tail 200 stream-checker
   ```

3. Verify Redis connection:
   ```http
   GET /api/stream-checker/celery/health
   ```

4. Restart the container if needed:
   ```bash
   docker compose restart stream-checker
   ```

Note: All logs are forwarded to Docker stdout, so you don't need to access individual log files inside the container.

### Counters Stuck

If concurrency counters become stuck:

1. Check current counts:
   ```http
   GET /api/stream-checker/concurrency/status
   ```

2. Reset if needed:
   ```http
   POST /api/stream-checker/concurrency/reset
   ```

### Tasks Timing Out

Increase task time limits in `celery_app.py`:
```python
task_soft_time_limit=300,  # 5 minutes soft limit
task_time_limit=360,  # 6 minutes hard limit
```

Or adjust stream analysis timeout in config:
```json
{
  "stream_analysis": {
    "timeout": 45
  }
}
```

## Limitations

1. **M3U Account Limits**: Requires accurate `max_streams` values in Dispatcharr
2. **Network Bandwidth**: Concurrent checks require more bandwidth
3. **CPU Usage**: Multiple ffmpeg processes run simultaneously within the container
4. **Redis Internal**: Redis runs within the container (not suitable for multi-container deployments)

## Migration from Multi-Container Setup

If you were using the old multi-container setup (separate Redis and Celery containers), migration to All-In-One is simple:

1. Stop and remove old containers:
   ```bash
   docker compose down
   ```

2. Update your docker-compose.yml to the new single-container format (see example above)

3. Update environment variables to use `REDIS_HOST=localhost`

4. Start the new All-In-One container:
   ```bash
   docker compose up -d
   ```

Your data will be preserved as it's stored in the mounted volume.

## Migration from Sequential

To switch from sequential to concurrent mode:

1. Ensure Redis is running
2. Update configuration:
   ```json
   {
     "concurrent_streams": {
       "enabled": true,
       "global_limit": 10
     }
   }
   ```
3. Restart StreamFlow services
4. Verify Celery health endpoint

To switch back to sequential:
```json
{
  "concurrent_streams": {
    "enabled": false
  }
}
```

## Best Practices

1. **Start Conservative**: Begin with low global limit (5-10)
2. **Monitor Resources**: Watch CPU, memory, and network usage
3. **Account Limits**: Ensure M3U accounts have accurate `max_streams` values
4. **Test First**: Test with a single channel before global checks
5. **Regular Cleanup**: The system auto-cleans stale tasks, but monitor for issues

## Future Enhancements

Potential improvements:
- Dynamic concurrency adjustment based on load
- Priority queuing for specific M3U accounts
- Better progress reporting for concurrent tasks
- Historical concurrency metrics
- Automatic limit discovery for M3U accounts
