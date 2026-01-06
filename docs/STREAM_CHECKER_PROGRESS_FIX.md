# Fix: UI Showing Incomplete Progress with Multiple M3U Accounts

## Problem

When checking streams from multiple M3U accounts using the "Check Single Channel" feature, the UI would display incomplete progress information. For example:
- Backend logs showed checking 15 streams
- UI showed only 3 streams being checked
- UI would show "completed" even though backend was still processing

This mismatch between backend and frontend caused confusion about the actual checking progress, particularly noticeable when multiple M3U accounts were involved in a single channel check.

## Root Cause

The issue was caused by duplicate parallel checks of the same channel due to overlapping automation triggers:

### Flow Before Fix

1. User triggers `check_single_channel(channel_id=16)` via API or UI
2. **Step 4** - Stream Matching: Calls `discover_and_assign_streams(force=True)`
   - This method found new streams and assigned them to channels
   - It automatically called `trigger_check_updated_channels()`
   - This queued the channel for checking → **Check Instance #1** (queued)
3. **Step 5** - Force Check: Explicitly calls `_check_channel(channel_id=16, force=True)`
   - This directly checked the channel → **Check Instance #2** (direct)

Both checks ran **in parallel**, both updating the same progress file (`stream_checker_progress.json`):
- Check #1 (queued): Regular check with only new/unchecked streams (e.g., 3 streams)
- Check #2 (direct): Force check with all streams (e.g., 15 streams)

### Race Condition

Since both checks wrote to the same progress file:
- The UI would see progress from whichever check finished first (usually #1 with fewer streams)
- Even though Check #2 was still running with all streams
- This caused the UI to show "3/3 completed" while 15 streams were actually being processed
- The backend logs showed both checks running simultaneously

## Solution

Added a new parameter `skip_check_trigger` to the `discover_and_assign_streams()` method to allow callers to control whether automatic queue triggering should occur.

### Implementation Details

#### Modified Method: `discover_and_assign_streams()`

```python
def discover_and_assign_streams(
    self, 
    force: bool = False, 
    skip_check_trigger: bool = False  # NEW PARAMETER
) -> Dict[str, int]:
    """Discover new streams and assign them to channels based on regex patterns.
    
    Args:
        force: If True, bypass the auto_stream_discovery feature flag check.
               Used for manual/quick action triggers from the UI.
        skip_check_trigger: If True, don't trigger immediate stream quality check.
               Used when the caller will handle the check itself (e.g., check_single_channel).
    """
```

When `skip_check_trigger=True`:
- The method still discovers and assigns streams
- It still marks channels as needing checks
- But it does NOT trigger the automatic queue check via `trigger_check_updated_channels()`
- The caller handles the check explicitly

#### Modified Triggering Logic

```python
# Trigger immediate check instead of waiting for scheduled interval
# Skip if caller will handle the check (e.g., check_single_channel)
if not skip_check_trigger:
    stream_checker.trigger_check_updated_channels()
else:
    logger.debug("Skipping automatic check trigger (will be handled by caller)")
```

#### Modified Call in `check_single_channel()`

```python
# Run full discovery (this will add new matching streams but skip dead ones)
# Skip automatic check trigger since we'll perform the check explicitly in Step 5
assignments = automation_manager.discover_and_assign_streams(
    force=True, 
    skip_check_trigger=True  # Prevent duplicate check
)
```

### Flow After Fix

1. User triggers `check_single_channel(channel_id=16)`
2. **Step 4**: Calls `discover_and_assign_streams(force=True, skip_check_trigger=True)`
   - Discovers and assigns streams
   - Marks channels for checking
   - Does NOT trigger queue → **No Check #1**
3. **Step 5**: Explicitly calls `_check_channel(channel_id=16, force=True)`
   - This directly checks the channel → **Only Check**

Now only ONE check runs, with all streams, and the UI correctly shows the full progress.

## Files Modified

1. **backend/automated_stream_manager.py**
   - Added `skip_check_trigger` parameter to `discover_and_assign_streams()`
   - Added conditional logic to skip trigger when parameter is True

2. **backend/stream_checker_service.py**
   - Updated `check_single_channel()` to pass `skip_check_trigger=True`
   - Added comment explaining the reason for the parameter

3. **backend/tests/test_single_channel_force_check.py**
   - Updated test to verify `skip_check_trigger=True` is passed

4. **backend/tests/test_single_channel_check_integration.py**
   - Updated integration test to verify the new parameter

## Test Coverage

### Unit Tests
- Verify `skip_check_trigger` parameter is passed correctly
- Verify automatic trigger is skipped when parameter is True
- Verify default behavior (False) still works

### Integration Tests
- End-to-end test of single channel check flow
- Verification of parameter propagation through the call chain

All tests passing ✅

## User Experience Impact

### Before Fix
1. User triggers single channel check
2. UI shows "Checking 3 streams" (only new streams)
3. UI shows "Completed" after 3 streams finish
4. Backend still checking remaining 12 streams
5. User confused about actual progress ❌

### After Fix
1. User triggers single channel check
2. UI shows "Checking 15 streams" (all streams)
3. UI updates progress correctly through all 15 streams
4. UI shows "Completed" only when all 15 streams finish
5. User sees accurate progress information ✅

## Backward Compatibility

The fix is fully backward compatible:
- Default value of `skip_check_trigger` is `False`
- All existing calls to `discover_and_assign_streams()` continue to work as before:
  - Automation cycle calls: Still trigger automatic checks (correct behavior)
  - Manual Quick Action calls: Still trigger automatic checks (correct behavior)
  - Global Action calls: Still trigger automatic checks (correct behavior)
- Only `check_single_channel()` uses the new `True` value to prevent duplicates

## Performance Impact

**Positive Impact:**
- Eliminates duplicate stream checks
- Reduces unnecessary work (checking same streams twice)
- Reduces network load on M3U providers
- More efficient resource usage

**No Negative Impact:**
- No additional overhead
- Same or better performance in all scenarios

## Benefits

1. **Fixes UI Progress Display**: UI now shows complete progress for all streams being checked
2. **Eliminates Race Conditions**: Only one check runs, no conflicting progress updates
3. **More Efficient**: Avoids duplicate work checking the same streams twice in parallel
4. **Clean Solution**: Minimal code changes with clear intent
5. **Maintains Existing Behavior**: All other code paths work exactly as before

## Verification

The fix can be verified by:
1. Setting up multiple M3U accounts with streams for a channel
2. Triggering a single channel check via UI or API
3. Observing the UI progress shows all streams being checked
4. Checking backend logs to confirm only one check instance runs
5. Verifying logs show: "Skipping automatic check trigger (will be handled by caller)"

## Related Issues

This fix addresses scenarios where:
- Multiple M3U accounts provide streams for the same channel
- Single channel check is triggered manually
- Progress tracking shows incomplete information
- Backend and frontend appear to be out of sync

## Future Enhancements

Consider:
- Adding UI indication when multiple checks are detected
- Implementing check deduplication at the queue level
- Adding metrics for duplicate check detection
- Monitoring progress file write conflicts
