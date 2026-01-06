# Debug Mode Guide

StreamFlow includes a comprehensive debug mode that provides detailed logging for troubleshooting and development.

## Enabling Debug Mode

Debug mode is controlled by the `DEBUG_MODE` environment variable. You can enable it in several ways:

### Docker Compose

Edit your `docker-compose.yml`:

```yaml
services:
  streamflow:
    environment:
      - DEBUG_MODE=true
```

### .env File

Add to your `.env` file:

```bash
DEBUG_MODE=true
```

### Command Line

When running locally:

```bash
DEBUG_MODE=true python3 backend/web_api.py
```

### Accepted Values

Debug mode recognizes these values as "enabled":
- `true`, `True`, `TRUE`
- `1`
- `yes`, `Yes`, `YES`
- `on`, `On`, `ON`

All other values (including `false`, `0`, `no`, `off`) disable debug mode.

## What Debug Mode Does

When debug mode is enabled, you'll get:

### 1. Enhanced Log Format

Logs include detailed context:
```
2025-11-13 09:39:52 - DEBUG - [module_name:function_name:line_number] - Message
```

### 2. Function Call Tracing

Entry and exit of critical functions:
```
→ function_name(param1=value1, param2=value2)
← function_name → result (1.234s)
```

### 3. API Request/Response Logging

Detailed HTTP request and response tracking:
```
→ API GET http://example.com/api/endpoint
← API GET http://example.com/api/endpoint → 200 (0.345s)
```

### 4. State Change Tracking

Service and component state transitions:
```
State change: stream_checker_service stopped → starting
State change: channel_123 queued → checking
```

### 5. Exception Details with Stack Traces

Full exception information including stack traces:
```
Exception in context_name: ValueError: error message
Traceback (most recent call last):
  ...
```

### 6. Configuration Loading

Details about configuration file loading:
```
Config file exists: /app/data/stream_checker_config.json
Loaded config with 15 top-level keys
Merged config: pipeline_mode=pipeline_1_5, enabled=True
```

### 7. Thread Operations

Thread creation and lifecycle:
```
Worker thread started (id: 140234567890)
Scheduler thread started (id: 140234567891)
```

### 8. Queue Operations

Channel queue management:
```
Added channel 42 to queue (priority: 0)
Worker waiting for next channel from queue...
Worker processing channel 42
```

## Performance Impact

Debug mode generates significantly more log output. Consider:

- **Disk space**: Debug logs can grow quickly
- **I/O overhead**: More log writes impact performance
- **Log file rotation**: Configure log rotation if running debug mode for extended periods

## Best Practices

### For Development

Enable debug mode to understand system behavior:

```bash
# Local development
DEBUG_MODE=true docker compose up
```

### For Troubleshooting

Enable temporarily when investigating issues:

1. Enable debug mode
2. Reproduce the issue
3. Collect logs
4. Disable debug mode
5. Analyze logs offline

### For Production

Keep debug mode disabled for production:

```bash
DEBUG_MODE=false  # or omit entirely
```

Enable only when actively troubleshooting specific issues.

## Log Levels

StreamFlow uses standard Python logging levels:

| Level | Description | Debug Mode | Production |
|-------|-------------|------------|------------|
| DEBUG | Detailed diagnostic information | ✓ Shown | ✗ Hidden |
| INFO | General informational messages | ✓ Shown | ✓ Shown |
| WARNING | Warning messages | ✓ Shown | ✓ Shown |
| ERROR | Error messages | ✓ Shown | ✓ Shown |

## Sensitive Data Protection

Debug mode automatically sanitizes sensitive information:

- **Authentication tokens**: Replaced with `<redacted>`
- **Passwords**: Never logged
- **API credentials**: Masked in request logs
- **Large payloads**: Summarized (e.g., `<dict with 5 keys>`)

## Viewing Logs

### Docker Container

View real-time logs:
```bash
docker compose logs -f streamflow
```

View with timestamp and specific service:
```bash
docker compose logs -f --timestamps streamflow
```

### Docker Container Logs File

Access logs directly:
```bash
docker compose exec streamflow cat /var/log/streamflow.log
```

### Filtering Debug Logs

Use grep to filter:
```bash
# Show only debug messages
docker compose logs streamflow | grep DEBUG

# Show function calls
docker compose logs streamflow | grep "→"

# Show API requests
docker compose logs streamflow | grep "API"

# Show state changes
docker compose logs streamflow | grep "State change"
```

## Common Debug Scenarios

### Stream Checker Not Working

```bash
# Enable debug mode
DEBUG_MODE=true docker compose up -d

# Watch logs for stream checker
docker compose logs -f streamflow | grep -E "stream_checker|StreamChecker"
```

Look for:
- Service initialization
- Queue operations
- Channel processing
- Configuration loading

### API Authentication Issues

```bash
docker compose logs streamflow | grep -E "login|token|auth"
```

Look for:
- Login attempts
- Token validation
- Token refresh operations

### Configuration Problems

```bash
docker compose logs streamflow | grep -E "config|Config"
```

Look for:
- Config file loading
- Default config creation
- Config validation
- Config updates

## Disabling Specific Log Types

HTTP request logs are filtered by default. To modify filtering, edit `backend/logging_config.py`:

```python
class HTTPLogFilter(logging.Filter):
    def filter(self, record):
        # Add or remove patterns to filter
        http_indicators = [
            'pattern1',
            'pattern2',
        ]
        return not any(indicator in message for indicator in http_indicators)
```

## Related Files

- `backend/logging_config.py` - Centralized logging configuration
- `backend/web_api.py` - Web API logging
- `backend/stream_checker_service.py` - Stream checker logging
- `backend/automated_stream_manager.py` - Automation manager logging
- `backend/api_utils.py` - API utilities logging

## Troubleshooting Debug Mode

### Debug Mode Not Working

1. **Verify environment variable**:
   ```bash
   docker compose exec streamflow env | grep DEBUG_MODE
   ```

2. **Check for restart**:
   After changing DEBUG_MODE, restart the container:
   ```bash
   docker compose restart streamflow
   ```

3. **Verify log format**:
   Debug logs should include function names and line numbers in square brackets.

### Too Many Logs

1. **Use log filtering** with grep
2. **Redirect to file**:
   ```bash
   docker compose logs streamflow > debug.log
   ```
3. **Disable debug mode** when not needed

## Support

If you're experiencing issues that debug mode doesn't help resolve:

1. Collect debug logs for the problematic operation
2. Sanitize any sensitive data
3. Open an issue on GitHub with:
   - Description of the issue
   - Relevant debug log excerpts
   - Configuration (sanitized)
   - Steps to reproduce
