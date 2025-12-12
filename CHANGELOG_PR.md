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
