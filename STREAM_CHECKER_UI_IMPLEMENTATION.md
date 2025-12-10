# Stream Checker UI Implementation

This document describes the implementation of the Stream Checker UI and the fix for dead stream handling behavior.

## Overview

This implementation delivers:
1. A comprehensive Stream Checker UI page with progress tracking and configuration
2. A fix to ensure Global Actions give dead streams a second chance
3. Updated documentation reflecting the correct behavior

## Frontend Changes

### Stream Checker Page (`frontend/src/pages/StreamChecker.jsx`)

A complete implementation of the Stream Checker UI with the following sections:

#### 1. Status Overview Cards
- **Status**: Shows if the checker is Active/Idle and displays current mode (parallel/sequential)
- **Queue Size**: Displays number of channels waiting to be checked
- **Completed**: Shows total channels checked in the current session
- **Failed**: Displays count of channels with errors

#### 2. Current Progress Display
Shows real-time progress when checking is active:
- Main progress bar with percentage
- Current step and step details
- Current stream being analyzed
- Stream counter (e.g., "Stream 5/20")
- Processing mode badges (Parallel with worker count)

#### 3. Stream Queue Information
- Displays queue size and provides context
- Clear Queue button for management
- Alert explaining priority-based processing

#### 4. Configuration Section
Comprehensive configuration panel with the following subsections:

**Stream Analysis Settings:**
- FFmpeg Duration (5-120 seconds)
- Timeout (10-300 seconds)
- Retry Attempts (0-5)
- Retry Delay (1-60 seconds)

**Concurrent Checking Settings:**
- Enable/Disable concurrent checking toggle
- Global Concurrent Limit (1-50 workers)
- Stagger Delay (0-10 seconds)

**Stream Scoring Weights:**
- Bitrate Weight (0-1)
- Resolution Weight (0-1)
- FPS Weight (0-1)
- Codec Weight (0-1)
- Prefer H.265/HEVC toggle

**Features:**
- Edit mode with validation
- Real-time updates (2-second polling)
- Disabled state indicators for dependent settings
- Toast notifications for success/error states
- Cancel/Save workflow

#### 5. Actions
- **Global Action**: Trigger manual global check
- **Clear Queue**: Remove all pending checks
- **Edit/Save Configuration**: Modify settings

### Component Dependencies

Added ShadCN component:
- `Separator`: For visual separation of configuration sections

## Backend Changes

### Dead Stream Tracker (`backend/dead_streams_tracker.py`)

#### New Method: `clear_all_dead_streams()`

```python
def clear_all_dead_streams(self) -> int:
    """Clear ALL dead streams from tracking.
    
    This is used during global actions to give all previously dead streams
    a second chance to be re-added and re-checked.
    
    Returns:
        int: Number of dead streams cleared
    """
```

**Implementation Details:**
- Thread-safe with lock acquisition
- Clears the in-memory `self.dead_streams` dictionary
- Calls `_save_dead_streams()` to persist the empty state to JSON file
- Returns count of cleared streams
- Comprehensive logging with emoji indicators

**JSON Persistence:**
The method ensures the cleared state is saved to `dead_streams.json` by calling `_save_dead_streams()`, which:
1. Creates the parent directory if needed
2. Writes the (now empty) dictionary to the JSON file with indentation
3. Handles errors gracefully with logging

### Stream Checker Service (`backend/stream_checker_service.py`)

#### Modified: `_perform_global_action()`

**Key Changes:**

1. **Added Step 2: Clear dead stream tracker**
   ```python
   # Step 2: Clear ALL dead streams from tracker to give them a second chance
   logger.info("Step 2/5: Clearing dead stream tracker...")
   dead_count = len(self.dead_streams_tracker.get_dead_streams())
   if dead_count > 0:
       self.dead_streams_tracker.clear_all_dead_streams()
       logger.info(f"✓ Cleared {dead_count} dead stream(s) - they will be given a second chance")
   ```

2. **Updated step numbering from 1/4, 2/4, 3/4, 4/4 to 1/5, 2/5, 3/5, 4/5, 5/5**

3. **Enhanced logging to indicate previously dead streams will be included in matching**

**Flow:**
1. Step 1/5: Refresh UDI cache
2. Step 2/5: Clear ALL dead streams from tracker (**NEW**)
3. Step 3/5: Update M3U playlists
4. Step 4/5: Match and assign streams (includes previously dead ones)
5. Step 5/5: Queue all channels for checking

#### Modified: `check_single_channel()`

**Key Changes:**

1. **Removed dead stream clearing logic**
   - Previously had Step 2 that removed dead streams for the channel
   - Now skips directly from identifying M3U accounts to refreshing playlists

2. **Updated step numbering from 1/5, 2/5, 3/5, 4/5, 5/5 to 1/4, 2/4, 3/4, 4/4**

3. **Updated docstring** to clarify it does NOT give dead streams a second chance

4. **Enhanced Step 3 comment** to explain dead streams remain excluded

**Flow:**
1. Step 1/4: Identify M3U accounts for the channel
2. Step 2/4: Refresh playlists for those accounts
3. Step 3/4: Re-match streams (excludes dead streams)
4. Step 4/4: Force check all streams

## Behavior Changes

### Before This Implementation

**Global Action:**
- Did NOT clear dead stream tracker
- Dead streams remained excluded from matching
- Dead streams never got a second chance

**Single Channel Check:**
- DID clear dead streams for the channel from tracker
- Dead streams could be re-added during matching
- Dead streams got a second chance

**Problem:** This was backwards from the intended behavior!

### After This Implementation

**Global Action (FIXED):**
- ✅ Clears ALL dead streams from tracker
- ✅ Dead streams are re-added during matching if they match patterns
- ✅ All streams get a fresh evaluation
- ✅ Comprehensive system-wide refresh

**Single Channel Check (FIXED):**
- ✅ Does NOT clear dead streams from tracker
- ✅ Respects dead stream tracker to avoid repeatedly checking known-bad streams
- ✅ Focused on checking current streams efficiently
- ✅ Dead streams only get second chance during Global Actions

**Why This Is Correct:**
- Global Action is the comprehensive operation that should give everything a fresh start
- Single Channel Check is a quick, targeted operation for specific channels
- This prevents excessive load from repeatedly checking known-bad streams
- Dead streams are still periodically re-evaluated during scheduled Global Actions

## Documentation Updates

### `docs/FEATURES.md`

Added comprehensive section **"Dead Stream Revival: Global Action vs Single Channel Check"** that explains:

1. **Global Action Behavior:**
   - 5-step process including dead stream tracker clearing
   - Result: ALL previously dead streams can be re-added and re-checked

2. **Single Channel Check Behavior:**
   - 4-step process that respects the dead stream tracker
   - Result: Dead streams remain excluded

3. **Rationale:**
   - Why the difference exists
   - When dead streams get re-evaluated
   - System design considerations

## API Integration

The frontend uses existing API endpoints from `streamCheckerAPI`:
- `getStatus()` - Service status and queue info
- `getProgress()` - Real-time progress updates
- `getConfig()` - Current configuration
- `updateConfig(config)` - Save configuration changes
- `triggerGlobalAction()` - Manual global check
- `clearQueue()` - Clear pending checks

All endpoints were already available; no backend API changes were needed.

## UI/UX Design

### Design Principles
- **Consistency**: Matches existing dashboard and configuration pages
- **Clarity**: Clear status indicators and progress information
- **Accessibility**: Proper labels, disabled states, and ARIA attributes
- **Responsiveness**: Grid layouts that adapt to screen sizes
- **Real-time Updates**: 2-second polling for active operations
- **User Feedback**: Toast notifications for all actions

### Component Usage
- ShadCN UI components for consistency
- Lucide React icons for visual cues
- Tailwind CSS for styling
- React hooks for state management

## Testing Recommendations

### Frontend Testing
1. **Status Display**: Verify all status cards update correctly
2. **Progress Tracking**: Test with actual stream checking operations
3. **Configuration**: 
   - Edit and save various settings
   - Verify validation (min/max values)
   - Test dependent field disabling
4. **Actions**:
   - Trigger Global Action and observe progress
   - Clear queue during operation
5. **Polling**: Verify updates occur every 2 seconds

### Backend Testing
1. **Dead Stream Clearing**:
   - Verify `clear_all_dead_streams()` empties the tracker
   - Confirm JSON file is updated (should be `{}`)
   - Test that cleared streams can be re-added during matching

2. **Global Action**:
   - Mark some streams as dead
   - Trigger Global Action
   - Verify dead streams are cleared before matching
   - Confirm dead streams are re-added if they match patterns

3. **Single Channel Check**:
   - Mark some streams as dead
   - Run Single Channel Check
   - Verify dead streams remain in tracker
   - Confirm dead streams are NOT re-added

### Integration Testing
1. End-to-end Global Action workflow
2. Configuration persistence across restarts
3. UI updates during concurrent checking
4. Error handling and recovery

## File Changes Summary

### Added
- `frontend/src/components/ui/separator.jsx` - ShadCN Separator component

### Modified
- `frontend/src/pages/StreamChecker.jsx` - Complete UI implementation (600+ lines)
- `backend/dead_streams_tracker.py` - Added `clear_all_dead_streams()` method
- `backend/stream_checker_service.py` - Fixed Global Action and Single Channel Check
- `docs/FEATURES.md` - Updated dead stream handling documentation

### Build Artifacts
- `frontend/package-lock.json` - Updated dependencies
- `frontend/package.json` - Updated dependencies

## Known Considerations

1. **Polling Frequency**: The UI polls every 2 seconds. This is acceptable for development but may need adjustment for production based on server load.

2. **Dead Stream Clearing**: Global Actions will clear ALL dead streams, which means streams that are truly dead will need to be re-checked. This is intentional to give them a second chance, but may add processing time.

3. **Configuration Validation**: The UI has basic min/max validation, but the backend should also validate configuration to prevent invalid values.

4. **Concurrent Checking Display**: The current implementation shows overall progress. Future enhancements could include per-playlist progress tracking as originally requested.

## Future Enhancements

1. **Per-Playlist Progress**: Display individual progress for each playlist being checked in concurrent mode
2. **Stream-Level Details**: Show list of streams currently being checked with individual progress
3. **Historical Statistics**: Add charts/graphs for stream checking history
4. **Advanced Filters**: Filter queue by channel group, priority, etc.
5. **Notifications**: Browser notifications for completed checks
6. **Export Configuration**: Allow exporting/importing configuration presets

## Migration Notes

No migration is required. The changes are additive and maintain backward compatibility:
- Existing dead stream tracker JSON files will work as-is
- API endpoints are unchanged
- Configuration format is unchanged
- The fix improves behavior without breaking existing functionality
