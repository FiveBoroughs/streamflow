# New Features: Dispatcharr Configuration, Channel Ordering, and M3U Account Discovery

## Summary

This PR implements three major features for StreamFlow as requested:
1. Dispatcharr configuration in the wizard and settings
2. Channel drag-and-drop reordering interface with sorting options
3. Automatic detection of new M3U accounts during playlist updates

## Features Implemented

### 1. Dispatcharr Configuration in Settings

**Frontend Implementation:**
- Added dedicated Dispatcharr Connection section to Automation Settings page
- Configuration fields:
  - Base URL (e.g., http://localhost:9191)
  - Username
  - Password (masked input, preserves existing password if left blank)
- **Test Connection** button with visual feedback:
  - Success: Green checkmark with success message
  - Failure: Red X with error details
- Integrated with existing `dispatcharrAPI` endpoints

**Backend Integration:**
- Uses existing `/api/dispatcharr/config` endpoints (GET/PUT)
- Uses existing `/api/dispatcharr/test-connection` endpoint
- Configuration managed by `dispatcharr_config.py`

### 2. Channel Drag-and-Drop Reordering Interface

**New Page Created:** `frontend/src/pages/ChannelOrdering.jsx`

**Features:**
- **Drag-and-Drop Interface**: 
  - Uses @dnd-kit library for smooth, accessible drag-and-drop
  - Visual feedback during drag (opacity change, shadow)
  - Touch-friendly for mobile devices
- **Multiple Sorting Options**:
  - Custom Order (manual drag-and-drop)
  - Sort by Channel Number (ascending)
  - Sort by Name (A-Z)
  - Sort by ID
- **Change Management**:
  - Visual indicator for unsaved changes (alert banner)
  - Reset button to discard changes
  - Save button to persist order
- **UI Features**:
  - Shows channel number, name, and ID for each channel
  - Displays total channel count
  - Accessible via new "Channel Ordering" menu item in sidebar

**Navigation:**
- Added to App.jsx routing: `/channel-ordering`
- Added to Sidebar.jsx with ArrowUpDown icon

**API Integration:**
- Uses `channelOrderAPI.setOrder()` to persist changes
- Backend endpoint: `PUT /api/channel-order`

### 3. Automatic M3U Account Detection

**Implementation:**

Modified playlist update logic to refresh M3U accounts list after each update:

**Backend Changes:**

1. **automated_stream_manager.py** (line ~563):
   ```python
   # Refresh UDI cache to get updated streams and channels after playlist update
   # Also refresh M3U accounts to detect any new accounts added in Dispatcharr
   logger.info("Refreshing UDI cache after playlist update...")
   udi = get_udi_manager()
   udi.refresh_m3u_accounts()  # Check for new M3U accounts
   udi.refresh_streams()
   udi.refresh_channels()
   ```

2. **stream_checker_service.py** (line ~2461):
   ```python
   # Refresh UDI cache to get updated streams
   # Also refresh M3U accounts to detect any new accounts
   udi.refresh_m3u_accounts()  # Check for new M3U accounts
   udi.refresh_streams()
   udi.refresh_channels()
   ```

**How It Works:**
- After every M3U playlist refresh operation, the UDI (Universal Data Index) manager refreshes its M3U accounts cache
- This ensures that any new accounts added in Dispatcharr are immediately available in StreamFlow
- No manual configuration needed - accounts are discovered automatically
- Works with both automated playlist updates and manual refresh operations

## Technical Details

### Dependencies Added
- `@dnd-kit/core`: ^6.3.1 - Core drag-and-drop functionality
- `@dnd-kit/sortable`: ^8.0.0 - Sortable lists
- `@dnd-kit/utilities`: ^3.2.2 - Utility functions

All dependencies checked for security vulnerabilities - none found.

### Code Quality
- **Security Scan**: Passed CodeQL analysis (0 alerts)
- **Code Review**: All issues addressed
- **Build**: Frontend builds successfully
- **Null Safety**: Added null check for drag-over parameter to prevent runtime errors

## User Experience Improvements

1. **Centralized Configuration**: Dispatcharr settings now accessible from Settings page (in addition to wizard)
2. **Intuitive Channel Management**: Drag-and-drop makes channel reordering much easier than manual number assignment
3. **Automatic Discovery**: New M3U accounts work immediately without requiring restart or manual refresh

## Documentation Updates

Updated `docs/FEATURES.md` to include:
- Configuration Management section
- Dispatcharr Connection Settings
- M3U Account Auto-Discovery
- Channel Ordering Interface

---

# Channel Settings and Scheduling Improvements

## Summary

This PR implements comprehensive enhancements to StreamFlow's channel configuration and scheduling features, addressing key user requirements for better control over stream matching and checking operations.

## Features Implemented

### 1. Channel-Specific Matching and Checking Exclusion

**Backend Implementation:**
- Created `ChannelSettingsManager` class to manage per-channel settings locally
- Settings stored in `channel_settings.json` (doesn't modify Dispatcharr's database)
- Two configurable modes per channel:
  - `matching_mode`: Controls whether the channel is included in stream discovery/matching operations
  - `checking_mode`: Controls whether the channel's streams are quality checked
- Both modes support `enabled` (default) and `disabled` states

**API Endpoints:**
- `GET /api/channel-settings` - Get all channel settings
- `GET /api/channel-settings/{channel_id}` - Get settings for a specific channel
- `PUT /api/channel-settings/{channel_id}` - Update settings for a specific channel

**Integration Points:**
- `automated_stream_manager.py`: Filters channels with matching disabled from stream assignment
- `stream_checker_service.py`: Filters channels with checking disabled from queue processing
- Both systems log the number of excluded channels for transparency

**Frontend Implementation:**
- Added dropdowns in Channel Configuration page (expandable card section)
- Two dropdowns per channel:
  - **Stream Matching**: Enable/Disable stream discovery for this channel
  - **Stream Checking**: Enable/Disable quality checking for this channel
- Helpful descriptions explain what each setting does
- Settings are saved immediately on change with toast notifications

### 2. Scheduling Table Improvements

**Changes:**
- Default events per page changed from 25 to 10
- Improved user experience for viewing scheduled EPG-based checks

## Technical Details

### Channel Settings Manager

The `ChannelSettingsManager` is a singleton service that:
- Loads settings from JSON file on startup
- Provides thread-safe access to settings
- Offers convenient helper methods:
  - `is_matching_enabled(channel_id)`: Check if matching is enabled
  - `is_checking_enabled(channel_id)`: Check if checking is enabled
  - `get_channel_settings(channel_id)`: Get full settings for a channel
  - `set_channel_settings(channel_id, ...)`: Update channel settings

### Data Flow

1. User changes setting in UI
2. Frontend calls API endpoint with new setting
3. Backend updates `channel_settings.json`
4. Next matching/checking operation respects the new setting
5. Excluded channels are logged for visibility

### Storage Format

```json
{
  "123": {
    "matching_mode": "disabled",
    "checking_mode": "enabled"
  },
  "456": {
    "matching_mode": "enabled", 
    "checking_mode": "disabled"
  }
}
```

## Benefits

1. **Fine-grained Control**: Users can now exclude specific channels from automation without deleting them
2. **Bandwidth Conservation**: Disable checking for channels that don't need quality monitoring
3. **Selective Matching**: Prevent certain channels from receiving new stream assignments
4. **Non-invasive**: Settings stored locally, doesn't modify Dispatcharr's database
5. **Transparent**: Operations log how many channels were excluded

## Testing

- ✅ Python syntax validation passed
- ✅ Frontend build successful
- ✅ Code review completed - all issues addressed
- ✅ CodeQL security scan passed - no vulnerabilities detected

## Compatibility

- Python 3.9+ (uses typing.List for compatibility)
- All existing tests pass (excluding pre-existing test infrastructure issues)
- No breaking changes to existing functionality
- Backward compatible - channels without settings default to "enabled"

## UI Screenshots

The Channel Configuration page now shows:
- Expandable channel cards with Edit Regex button
- When expanded, two new dropdowns appear above regex patterns:
  - Stream Matching (Enabled/Disabled)
  - Stream Checking (Enabled/Disabled)
- Settings saved immediately with success/error toasts

## Future Enhancements (Not in this PR)

The following features were identified but not implemented due to scope/time constraints:
- Multi-channel scheduling rules
- Channel filter dropdown in scheduled events table
- Edit functionality for auto-create rules
- Handling simultaneous check times
- Fix for single channel check UI popup timing
- Multiple concurrent streams display in Stream Checker

## Files Changed

**Backend:**
- `backend/channel_settings_manager.py` (new)
- `backend/web_api.py` (added API endpoints)
- `backend/automated_stream_manager.py` (integrated filtering)
- `backend/stream_checker_service.py` (integrated filtering)

**Frontend:**
- `frontend/src/services/api.js` (added channelSettingsAPI)
- `frontend/src/pages/ChannelConfiguration.jsx` (added UI controls)
- `frontend/src/pages/Scheduling.jsx` (changed default pagination)

**Documentation:**
- `README.md` (updated features list)
- `CHANGELOG_PR.md` (this file)
