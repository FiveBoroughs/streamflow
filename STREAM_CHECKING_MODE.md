# Stream Checking Mode Implementation

## Overview
This document describes the implementation of "Stream Checking Mode" - a safety feature that prevents UDI (Unified Data Interface) inconsistency by disabling certain operations while stream checks are in progress.

## Problem Statement
Stream checks are long-running operations that can take considerable time. During these operations, there's a risk that the UDI can become inconsistent if other processes (such as automation cycles, M3U refreshes) are performed simultaneously. This could lead to:
- Data corruption
- Race conditions
- Incorrect stream ordering
- Lost updates

## Solution
When stream checks are being performed, the system enters "Stream Checking Mode" where:
1. **Quick Actions are disabled** (Dashboard)
2. **Global Action button is disabled** (StreamChecker page)
3. **Visual indicators** show the user that the system is in this protected mode

## Backend Implementation

### Changes to `stream_checker_service.py`

Added `stream_checking_mode` computed property in `get_status()` method (lines 1678-1711):

```python
def get_status(self) -> Dict:
    """Get current service status."""
    queue_status = self.check_queue.get_status()
    progress = self.progress.get()
    
    # Stream checking mode is active when:
    # - A global action is in progress, OR
    # - An individual channel is being checked, OR
    # - There are channels in the queue waiting to be checked
    stream_checking_mode = (
        self.global_action_in_progress or 
        self.checking or 
        queue_status.get('queue_size', 0) > 0 or
        queue_status.get('in_progress', 0) > 0
    )
    
    return {
        'running': self.running,
        'checking': self.checking,
        'global_action_in_progress': self.global_action_in_progress,
        'stream_checking_mode': stream_checking_mode,  # NEW
        # ... rest of status
    }
```

### Stream Checking Mode Triggers

The mode is activated when ANY of the following conditions are true:

1. **`global_action_in_progress = True`**
   - Set during `_perform_global_action()` 
   - Covers: Update M3U → Match streams → Check all channels

2. **`checking = True`**
   - Set during `_check_channel()` for individual channel checks
   - Covers: Any single channel stream quality check

3. **`queue_size > 0`**
   - Channels waiting in queue to be checked
   - Covers: Pending stream checks

4. **`in_progress > 0`**
   - Channels currently being processed
   - Covers: Active stream checks

## Frontend Implementation

### Dashboard Component (`Dashboard.js`)

**Quick Actions Card** - Lines 264-316:
- Added check for `streamCheckerStatus?.stream_checking_mode`
- All three Quick Action buttons are disabled when mode is active:
  - Refresh M3U Playlist
  - Discover & Assign Streams
  - Run Automation Cycle
- Displays info alert: "Stream checking mode active - Quick Actions disabled"

```javascript
{streamCheckerStatus?.stream_checking_mode && (
  <Alert severity="info" sx={{ mb: 2 }}>
    Stream checking mode active - Quick Actions disabled
  </Alert>
)}
<Button
  disabled={actionLoading === 'playlist' || streamCheckerStatus?.stream_checking_mode}
  // ...
>
  Refresh M3U Playlist
</Button>
```

### StreamChecker Component (`StreamChecker.js`)

**Global Action Button** - Lines 302-314:
- Button disabled when `status?.stream_checking_mode` is true
- Button text changes to "Checking..." when mode is active
- Tooltip explains the operation

**Visual Indicator** - Added warning alert (lines ~150-165):
- Displays when `status?.stream_checking_mode` is true
- Message: "Stream Checking Mode Active: Stream checks are in progress. All Quick Actions and other processes are paused to prevent UDI inconsistency."

## Testing

### Backend Tests (`test_stream_checking_mode.py`)

Created comprehensive test suite with 6 tests:

1. `test_stream_checking_mode_with_global_action` - Verifies mode during global action
2. `test_stream_checking_mode_with_checking_flag` - Verifies mode during individual checks
3. `test_stream_checking_mode_with_queue` - Verifies mode when queue has channels
4. `test_stream_checking_mode_with_in_progress_channels` - Verifies mode during processing
5. `test_stream_checking_mode_false_when_idle` - Verifies mode is off when idle
6. `test_status_includes_stream_checking_mode` - Verifies API always includes the flag

All tests pass successfully.

### Verification Script (`verify_stream_checking_mode.py`)

Created manual verification script that tests all scenarios and provides clear output.

## API Changes

### GET `/api/stream-checker/status`

**Response format (NEW field added):**
```json
{
  "running": true,
  "checking": false,
  "global_action_in_progress": false,
  "stream_checking_mode": false,  // NEW
  "enabled": true,
  "queue": { /* ... */ },
  "progress": { /* ... */ },
  "last_global_check": "2025-12-04T10:30:00",
  "config": { /* ... */ }
}
```

## User Experience

### When Stream Checking Mode is Active:

1. **Dashboard**:
   - Info alert appears above Quick Actions
   - All Quick Action buttons are disabled (greyed out)
   - Users cannot trigger competing operations

2. **StreamChecker Page**:
   - Warning alert appears at top of page
   - Global Action button shows "Checking..." and is disabled
   - Progress indicators show current checking status
   - Queue status displays active operations

### When System is Idle:

- All buttons are enabled (subject to other conditions like service running)
- No alerts are displayed
- Users have full control over all actions

## Backward Compatibility

- **Fully backward compatible**: Existing API consumers will see the new `stream_checking_mode` field but are not required to use it
- **Graceful degradation**: Frontend will work even if backend doesn't provide the field (uses optional chaining)
- **No breaking changes**: All existing functionality remains unchanged

## Files Modified

1. `backend/stream_checker_service.py` - Added stream_checking_mode computation
2. `frontend/src/components/Dashboard.js` - Disabled Quick Actions during mode
3. `frontend/src/components/StreamChecker.js` - Disabled Global Action and added alerts
4. `backend/tests/test_stream_checking_mode.py` - Added comprehensive test suite (NEW)
5. `backend/verify_stream_checking_mode.py` - Added verification script (NEW)

## Future Considerations

- Any new UI actions that can modify streams or channels should check `stream_checking_mode` before executing
- Consider adding similar protection for other long-running operations if needed
- Monitor for any edge cases where the mode should be active but isn't

## Security & Safety

This implementation prevents:
- Race conditions during stream updates
- Data corruption from concurrent modifications
- UDI inconsistency
- Lost updates
- Unexpected behavior from competing operations
