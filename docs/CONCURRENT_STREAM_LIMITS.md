# Concurrent Stream Limits

## Overview

StreamFlow implements intelligent per-account concurrent stream limiting to respect M3U provider limits while maximizing overall parallelism. This ensures that when checking multiple streams in parallel, each account's maximum concurrent stream limit is respected.

## Problem Statement

Many M3U providers have stream limits (e.g., maximum 1 or 2 concurrent streams per account). When StreamFlow checks channel quality by analyzing multiple streams in parallel, it needs to respect these limits to avoid:
- Account suspensions
- Failed stream checks
- Provider throttling
- Wasted resources

## Solution

StreamFlow uses a **Smart Stream Scheduler** with **Per-Account Semaphore-Based Limiting** to intelligently schedule stream checks:

### Architecture

1. **AccountStreamLimiter**: Manages semaphores for each M3U account
   - Creates a semaphore for each account based on its `max_streams` setting
   - Enforces limits using thread-safe acquire/release operations
   - Supports unlimited accounts (max_streams=0)

2. **SmartStreamScheduler**: Intelligently schedules stream checks
   - Groups streams by account
   - Respects per-account limits while maximizing global concurrency
   - Ensures optimal resource utilization across different accounts

### Example Scenario

Given:
- **Account A**: max_streams = 1 (only 1 concurrent stream allowed)
- **Account B**: max_streams = 2 (up to 2 concurrent streams allowed)
- **Channel streams**: A1, A2, B1, B2, B3

**Without Limits** (❌ Problem):
```
All 5 streams start checking simultaneously
→ Account A: 2 concurrent (violates limit!)
→ Account B: 3 concurrent (violates limit!)
```

**With Smart Limiter** (✅ Solution):
```
Phase 1: A1, B1, B2 check concurrently (3 total)
  → Account A: 1 concurrent ✓
  → Account B: 2 concurrent ✓
  
Phase 2: When A1 completes, A2 starts
  → Account A: 1 concurrent ✓
  
Phase 3: When B1 or B2 completes, B3 starts
  → Account B: 2 concurrent ✓
```

## Configuration

### M3U Account Configuration

The `max_streams` field is configured per M3U account in Dispatcharr:

```json
{
  "id": 1,
  "name": "My IPTV Provider",
  "max_streams": 2,
  "server_url": "http://provider.com/playlist.m3u8",
  "is_active": true
}
```

- `max_streams = 0`: Unlimited concurrent streams
- `max_streams = 1`: Only 1 stream at a time
- `max_streams = 2`: Up to 2 streams concurrently
- `max_streams = N`: Up to N streams concurrently

### Global Concurrency Limit

In addition to per-account limits, StreamFlow respects a global concurrency limit:

```json
{
  "concurrent_streams": {
    "enabled": true,
    "global_limit": 10,
    "stagger_delay": 1.0
  }
}
```

- `global_limit`: Maximum total concurrent stream checks (default: 10)
- `stagger_delay`: Delay in seconds between starting tasks (default: 1.0)

**Effective Limit**: The actual concurrency is the minimum of:
1. Global limit (e.g., 10)
2. Sum of all account limits (e.g., Account A: 1 + Account B: 2 = 3)

## Implementation Details

### AccountStreamLimiter

```python
class AccountStreamLimiter:
    def set_account_limit(self, account_id: int, max_streams: int):
        """Set maximum concurrent streams for an account."""
        
    def acquire(self, account_id: int, timeout: float) -> bool:
        """Acquire permission to check a stream."""
        
    def release(self, account_id: int):
        """Release a stream slot."""
```

### SmartStreamScheduler

```python
class SmartStreamScheduler:
    def check_streams_with_limits(
        self,
        streams: List[Dict],
        check_function: Callable,
        **params
    ) -> List[Dict]:
        """Check streams with account-aware limits."""
```

### Integration

The smart scheduler is automatically used when `concurrent_streams.enabled = true`:

```python
# In stream_checker_service.py
from concurrent_stream_limiter import get_smart_scheduler, initialize_account_limits

# Initialize account limits from UDI
accounts = udi.get_m3u_accounts()
initialize_account_limits(accounts)

# Use smart scheduler
smart_scheduler = get_smart_scheduler(global_limit=10)
results = smart_scheduler.check_streams_with_limits(
    streams=streams_to_check,
    check_function=analyze_stream,
    **analysis_params
)
```

## Benefits

1. **Provider Compliance**: Respects account stream limits
2. **Maximum Parallelism**: Multiple accounts can check concurrently
3. **Optimal Resource Usage**: Minimizes total checking time
4. **Account Safety**: Prevents suspensions and throttling
5. **Transparent**: Works automatically with existing stream checking

## Testing

Comprehensive test suite included:

- `test_single_stream_limit`: Verifies max_streams=1 enforcement
- `test_multiple_stream_limit`: Verifies max_streams=2 enforcement
- `test_multiple_accounts_independent`: Verifies account independence
- `test_mixed_limits`: Verifies the example scenario (A:1, B:2)
- `test_concurrent_access`: Verifies thread-safety
- `test_unlimited_account`: Verifies max_streams=0 behavior

Run tests:
```bash
python -m unittest tests.test_concurrent_stream_limiter -v
```

## Monitoring

The smart scheduler logs detailed information:

```
INFO - Starting smart parallel check of 5 streams
INFO - Streams grouped by account: {1: 2, 2: 3}
INFO -   Account 1: 2 streams, limit=1
INFO -   Account 2: 3 streams, limit=2
INFO - Completed smart parallel check of 5/5 streams
```

## Future Enhancements

Potential improvements:
1. **Dynamic Limit Adjustment**: Auto-detect limits from provider responses
2. **UI Dashboard**: Real-time visualization of account usage
3. **Priority Scheduling**: Prioritize critical channels
4. **Load Balancing**: Distribute load across server groups
5. **Historical Analytics**: Track account usage patterns

## API Reference

### GET /api/m3u/accounts/
Returns list of M3U accounts with `max_streams` field.

### PATCH /api/m3u/accounts/{id}/
Update account settings including `max_streams`.

## Related Documentation

- [FEATURES.md](FEATURES.md) - General features overview
- [PIPELINE_SYSTEM.md](PIPELINE_SYSTEM.md) - Automation pipeline modes
- [API.md](API.md) - API reference

## Support

For issues or questions about concurrent stream limits:
1. Check account configuration in Dispatcharr
2. Review logs for "smart parallel check" messages
3. Verify `max_streams` values are correct
4. Run test suite to validate functionality
