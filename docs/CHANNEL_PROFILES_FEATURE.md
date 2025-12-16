# Dispatcharr Channel Profiles Feature Implementation

## Overview

This feature enables Streamflow to manage Dispatcharr channel profiles, allowing users to selectively disable empty channels (channels with no working streams) in specific profiles. This is particularly useful for maintaining clean channel lineups while preserving the ability to re-enable channels when streams become available again.

## Feature Capabilities

### 1. Profile Selection
- Users can select a specific channel profile for Streamflow operations
- Option to use the general/all profile (default) or a specific profile
- Profile selection persists across application restarts

### 2. Empty Channel Management
Two use cases are supported:

#### Use Case A: General List with Profile-Specific Disabling
- User wants to continue using the general channel list
- Empty channels are disabled in a specific target profile
- The general list remains unchanged

#### Use Case B: Profile with Snapshot
- User wants to use a specific profile
- Takes a "snapshot" of desired channels in the profile
- Empty channels are disabled
- Snapshot preserves which channels to re-enable when streams return

### 3. Profile Snapshots
- Create snapshots of channel lists in profiles
- Snapshots record: profile ID, name, channel IDs, timestamp, count
- Can re-snapshot to update after manual profile changes
- Delete snapshots when no longer needed

## Backend Implementation

### Architecture Components

#### 1. Profile Configuration Manager (`profile_config.py`)
**Purpose**: Central management of profile settings and snapshots

**Key Classes**:
- `ProfileConfig`: Manages all profile-related configuration

**Key Methods**:
```python
# Profile Selection
get_selected_profile() -> Optional[int]
set_selected_profile(profile_id, profile_name) -> bool
is_using_profile() -> bool

# Dead Stream Management
get_dead_stream_config() -> Dict
set_dead_stream_config(enabled, target_profile_id, target_profile_name, use_snapshot) -> bool
is_dead_stream_management_enabled() -> bool
get_target_profile_for_dead_streams() -> Optional[int]

# Snapshot Management
create_snapshot(profile_id, profile_name, channel_ids) -> bool
get_snapshot(profile_id) -> Optional[Dict]
has_snapshot(profile_id) -> bool
delete_snapshot(profile_id) -> bool
get_all_snapshots() -> Dict[str, Dict]
```

**Storage**: 
- File: `/app/data/profile_config.json`
- Thread-safe with locking
- Automatic persistence on changes

#### 2. UDI Integration (`udi/`)
**Purpose**: Fetch and cache channel profile data from Dispatcharr

**Modified Files**:
- `udi/fetcher.py`: Added `fetch_channel_profiles()` and `fetch_channel_profile_by_id()`
- `udi/manager.py`: Added profile caching, `get_channel_profiles()`, `get_channel_profile_by_id()`, `refresh_channel_profiles()`
- `udi/storage.py`: Added `load_channel_profiles()` and `save_channel_profiles()`

**Integration Points**:
- Profiles fetched from `/api/channels/profiles/` endpoint
- Cached in memory and persisted to disk
- Included in full UDI refresh cycles
- Background refresh supported

#### 3. Web API Endpoints (`web_api.py`)
**Purpose**: REST API for frontend integration

**Profile Configuration Endpoints**:
- `GET /api/profile-config` - Get current profile configuration
- `PUT /api/profile-config` - Update profile configuration

**Profile Data Endpoints**:
- `GET /api/profiles` - List all available profiles
- `GET /api/profiles/{id}/channels` - Get channels for a profile

**Snapshot Endpoints**:
- `POST /api/profiles/{id}/snapshot` - Create profile snapshot
- `GET /api/profiles/{id}/snapshot` - Get profile snapshot
- `DELETE /api/profiles/{id}/snapshot` - Delete profile snapshot
- `GET /api/profiles/snapshots` - Get all snapshots

**Empty Channel Management**:
- `POST /api/profiles/{id}/disable-empty-channels` - Disable empty channels in profile

### API Request/Response Examples

#### Get Profile Configuration
```http
GET /api/profile-config
```
Response:
```json
{
  "selected_profile_id": null,
  "selected_profile_name": null,
  "use_profile": false,
  "dead_streams": {
    "enabled": false,
    "target_profile_id": null,
    "target_profile_name": null,
    "use_snapshot": false
  },
  "snapshots": {}
}
```

#### Update Profile Configuration
```http
PUT /api/profile-config
Content-Type: application/json

{
  "selected_profile_id": 1,
  "selected_profile_name": "Family Profile",
  "dead_streams": {
    "enabled": true,
    "target_profile_id": 2,
    "target_profile_name": "Disabled Channels",
    "use_snapshot": true
  }
}
```

#### Get All Profiles
```http
GET /api/profiles
```
Response:
```json
[
  {
    "id": 1,
    "name": "Family Profile",
    "channels": "..."
  },
  {
    "id": 2,
    "name": "Disabled Channels",
    "channels": "..."
  }
]
```

#### Create Profile Snapshot
```http
POST /api/profiles/1/snapshot
```
Response:
```json
{
  "message": "Snapshot created successfully",
  "snapshot": {
    "profile_id": 1,
    "profile_name": "Family Profile",
    "channel_ids": [1, 2, 3, 4, 5],
    "created_at": "2025-12-16T12:00:00",
    "channel_count": 5
  }
}
```

#### Disable Empty Channels
```http
POST /api/profiles/2/disable-empty-channels
```
Response:
```json
{
  "message": "Disabled 3 empty channels",
  "disabled_count": 3,
  "total_checked": 50
}
```

### Integration with Dead Streams Tracker

The implementation leverages the existing `DeadStreamsTracker` class to identify empty channels:

1. Fetches all channels from UDI
2. For each channel, checks if all associated streams are marked as dead
3. Channels with no streams or all dead streams are considered "empty"
4. Uses Dispatcharr API to disable empty channels in the target profile

This ensures compatibility with the existing stream checking and validation systems.

## Frontend Implementation Guide

### Required Components

#### 1. Profile API Service (`frontend/src/services/api.js`)

Add the following API methods:

```javascript
// Profile Configuration
export const profileAPI = {
  getConfig: () => api.get('/profile-config'),
  updateConfig: (config) => api.put('/profile-config', config),
  
  // Profiles
  getProfiles: () => api.get('/profiles'),
  getProfileChannels: (profileId) => api.get(`/profiles/${profileId}/channels`),
  
  // Snapshots
  createSnapshot: (profileId) => api.post(`/profiles/${profileId}/snapshot`),
  getSnapshot: (profileId) => api.get(`/profiles/${profileId}/snapshot`),
  deleteSnapshot: (profileId) => api.delete(`/profiles/${profileId}/snapshot`),
  getAllSnapshots: () => api.get('/profiles/snapshots'),
  
  // Empty Channel Management
  disableEmptyChannels: (profileId) => api.post(`/profiles/${profileId}/disable-empty-channels`)
}
```

#### 2. Profile Management Section in AutomationSettings

Location: `frontend/src/pages/AutomationSettings.jsx`

Add a new tab or section for "Profile Management":

```jsx
<Card>
  <CardHeader>
    <CardTitle>Channel Profile Management</CardTitle>
    <CardDescription>
      Manage channel profiles and configure empty channel handling
    </CardDescription>
  </CardHeader>
  <CardContent>
    {/* Profile Selection */}
    <div className="space-y-4">
      <div>
        <Label>Active Profile</Label>
        <Select value={selectedProfileId} onValueChange={handleProfileChange}>
          <SelectTrigger>
            <SelectValue placeholder="Select a profile" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="general">General (All Channels)</SelectItem>
            {profiles.map(profile => (
              <SelectItem key={profile.id} value={profile.id}>
                {profile.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Dead Streams Configuration */}
      <div className="border-t pt-4">
        <h4 className="font-semibold mb-2">Empty Channel Management</h4>
        
        <div className="space-y-3">
          <div className="flex items-center space-x-2">
            <Switch 
              checked={deadStreamsEnabled} 
              onCheckedChange={setDeadStreamsEnabled}
            />
            <Label>Enable automatic empty channel disabling</Label>
          </div>
          
          {deadStreamsEnabled && (
            <>
              <div>
                <Label>Target Profile for Disabling</Label>
                <Select value={targetProfileId} onValueChange={setTargetProfileId}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select target profile" />
                  </SelectTrigger>
                  <SelectContent>
                    {profiles.map(profile => (
                      <SelectItem key={profile.id} value={profile.id}>
                        {profile.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              
              <div className="flex items-center space-x-2">
                <Switch 
                  checked={useSnapshot} 
                  onCheckedChange={setUseSnapshot}
                />
                <Label>Use snapshot for re-enabling</Label>
              </div>
              
              <Button onClick={handleDisableEmptyChannels}>
                Disable Empty Channels Now
              </Button>
            </>
          )}
        </div>
      </div>

      {/* Snapshot Management */}
      {selectedProfileId && selectedProfileId !== 'general' && (
        <div className="border-t pt-4">
          <h4 className="font-semibold mb-2">Profile Snapshot</h4>
          
          {snapshot ? (
            <div className="space-y-2">
              <p className="text-sm text-muted-foreground">
                Snapshot created: {new Date(snapshot.created_at).toLocaleString()}
              </p>
              <p className="text-sm text-muted-foreground">
                Channels: {snapshot.channel_count}
              </p>
              <div className="flex gap-2">
                <Button variant="outline" onClick={handleReSnapshot}>
                  Re-Snapshot
                </Button>
                <Button variant="destructive" onClick={handleDeleteSnapshot}>
                  Delete Snapshot
                </Button>
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              <p className="text-sm text-muted-foreground">
                No snapshot exists for this profile
              </p>
              <Button onClick={handleCreateSnapshot}>
                Create Snapshot
              </Button>
            </div>
          )}
        </div>
      )}
    </div>
  </CardContent>
</Card>
```

#### 3. Setup Wizard Integration

Location: `frontend/src/pages/SetupWizard.jsx`

Add a step for profile configuration (optional, can be skipped):

```jsx
// Step 4: Profile Configuration (Optional)
<div className="space-y-4">
  <h3 className="text-lg font-semibold">Profile Configuration</h3>
  <p className="text-sm text-muted-foreground">
    Configure which channel profile Streamflow should use (optional)
  </p>
  
  <div>
    <Label>Channel Profile</Label>
    <Select value={wizardConfig.profileId} onValueChange={(value) => 
      setWizardConfig(prev => ({ ...prev, profileId: value }))
    }>
      <SelectTrigger>
        <SelectValue placeholder="Use general profile (default)" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="general">General Profile</SelectItem>
        {profiles.map(profile => (
          <SelectItem key={profile.id} value={profile.id}>
            {profile.name}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  </div>
  
  <Alert>
    <AlertTitle>Info</AlertTitle>
    <AlertDescription>
      You can change this setting later in Automation Settings.
      The general profile includes all channels across all profiles.
    </AlertDescription>
  </Alert>
</div>
```

### State Management

```javascript
const [profileConfig, setProfileConfig] = useState(null)
const [profiles, setProfiles] = useState([])
const [selectedProfileId, setSelectedProfileId] = useState(null)
const [deadStreamsEnabled, setDeadStreamsEnabled] = useState(false)
const [targetProfileId, setTargetProfileId] = useState(null)
const [useSnapshot, setUseSnapshot] = useState(false)
const [snapshot, setSnapshot] = useState(null)
const [loading, setLoading] = useState(false)

// Load initial data
useEffect(() => {
  const loadData = async () => {
    try {
      const [configRes, profilesRes] = await Promise.all([
        profileAPI.getConfig(),
        profileAPI.getProfiles()
      ])
      
      setProfileConfig(configRes.data)
      setProfiles(profilesRes.data)
      setSelectedProfileId(configRes.data.selected_profile_id || 'general')
      setDeadStreamsEnabled(configRes.data.dead_streams.enabled)
      setTargetProfileId(configRes.data.dead_streams.target_profile_id)
      setUseSnapshot(configRes.data.dead_streams.use_snapshot)
      
      // Load snapshot if applicable
      if (configRes.data.selected_profile_id) {
        try {
          const snapshotRes = await profileAPI.getSnapshot(
            configRes.data.selected_profile_id
          )
          setSnapshot(snapshotRes.data)
        } catch (err) {
          // No snapshot exists
          setSnapshot(null)
        }
      }
    } catch (err) {
      console.error('Failed to load profile data:', err)
      toast({
        title: "Error",
        description: "Failed to load profile configuration",
        variant: "destructive"
      })
    }
  }
  
  loadData()
}, [])

// Save configuration
const handleSaveConfig = async () => {
  try {
    setLoading(true)
    
    await profileAPI.updateConfig({
      selected_profile_id: selectedProfileId === 'general' ? null : selectedProfileId,
      selected_profile_name: profiles.find(p => p.id === selectedProfileId)?.name,
      dead_streams: {
        enabled: deadStreamsEnabled,
        target_profile_id: targetProfileId,
        target_profile_name: profiles.find(p => p.id === targetProfileId)?.name,
        use_snapshot: useSnapshot
      }
    })
    
    toast({
      title: "Success",
      description: "Profile configuration saved"
    })
  } catch (err) {
    toast({
      title: "Error",
      description: "Failed to save configuration",
      variant: "destructive"
    })
  } finally {
    setLoading(false)
  }
}

// Handle empty channel disabling
const handleDisableEmptyChannels = async () => {
  if (!targetProfileId) return
  
  try {
    setLoading(true)
    const res = await profileAPI.disableEmptyChannels(targetProfileId)
    
    toast({
      title: "Success",
      description: res.data.message
    })
  } catch (err) {
    toast({
      title: "Error",
      description: "Failed to disable empty channels",
      variant: "destructive"
    })
  } finally {
    setLoading(false)
  }
}

// Snapshot handlers
const handleCreateSnapshot = async () => {
  try {
    setLoading(true)
    const res = await profileAPI.createSnapshot(selectedProfileId)
    setSnapshot(res.data.snapshot)
    
    toast({
      title: "Success",
      description: "Snapshot created successfully"
    })
  } catch (err) {
    toast({
      title: "Error",
      description: "Failed to create snapshot",
      variant: "destructive"
    })
  } finally {
    setLoading(false)
  }
}

const handleReSnapshot = async () => {
  await handleCreateSnapshot() // Reuse create logic
}

const handleDeleteSnapshot = async () => {
  try {
    setLoading(true)
    await profileAPI.deleteSnapshot(selectedProfileId)
    setSnapshot(null)
    
    toast({
      title: "Success",
      description: "Snapshot deleted successfully"
    })
  } catch (err) {
    toast({
      title: "Error",
      description: "Failed to delete snapshot",
      variant: "destructive"
    })
  } finally {
    setLoading(false)
  }
}
```

## Testing Strategy

### Backend Tests
✅ **Completed**: `backend/tests/test_profile_config.py`
- 9 comprehensive unit tests
- All passing
- Coverage: configuration management, snapshot lifecycle, persistence

### Frontend Tests (To Be Added)
- Component rendering tests
- API integration tests
- User interaction tests
- State management tests

### Integration Tests (Recommended)
1. **End-to-end snapshot workflow**
   - Create profile snapshot
   - Disable empty channels
   - Verify channels disabled in Dispatcharr
   - Re-enable from snapshot

2. **Profile selection persistence**
   - Set profile configuration
   - Restart application
   - Verify configuration loaded

3. **Empty channel detection**
   - Create channels with dead streams
   - Run disable empty channels
   - Verify correct channels disabled

## Deployment Considerations

### Configuration Files
The feature creates/uses these files in `/app/data`:
- `profile_config.json` - Profile configuration and snapshots
- `udi/channel_profiles.json` - Cached profile data

### Environment Variables
No new environment variables required. Uses existing:
- `CONFIG_DIR` - Configuration directory path (default: `/app/data`)

### Docker Volumes
Ensure `/app/data` is mounted as a volume for persistence

### Backwards Compatibility
- Feature is opt-in - doesn't affect existing functionality
- Works with existing dead streams tracking
- Compatible with existing stream checker cycles

## Future Enhancements

### Automatic Re-enabling
- Monitor previously disabled channels
- Automatically re-enable when streams become available
- Requires periodic background check

### Profile-Aware Stream Checking
- Respect selected profile during stream validation
- Only check channels in active profile
- Requires integration with `stream_checker_service.py`

### Automated Snapshot Updates
- Automatically update snapshots on manual profile changes
- Track profile modification timestamps
- Suggest re-snapshot when profile changed

### Profile-Specific Scheduling
- Schedule empty channel checks per profile
- Different check intervals for different profiles
- Integration with scheduling service

## API Documentation

Complete API documentation is embedded in the code docstrings. Key endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/profile-config` | GET | Get configuration |
| `/api/profile-config` | PUT | Update configuration |
| `/api/profiles` | GET | List profiles |
| `/api/profiles/{id}/channels` | GET | Get profile channels |
| `/api/profiles/{id}/snapshot` | POST | Create snapshot |
| `/api/profiles/{id}/snapshot` | GET | Get snapshot |
| `/api/profiles/{id}/snapshot` | DELETE | Delete snapshot |
| `/api/profiles/snapshots` | GET | Get all snapshots |
| `/api/profiles/{id}/disable-empty-channels` | POST | Disable empty channels |

## Troubleshooting

### Common Issues

**Issue**: "Profile not found"
- **Cause**: Profile doesn't exist in Dispatcharr
- **Solution**: Verify profile exists, refresh UDI data

**Issue**: "Failed to disable channels"
- **Cause**: Dispatcharr API authentication failure
- **Solution**: Check Dispatcharr credentials, test connection

**Issue**: "Snapshot not found"
- **Cause**: No snapshot created for profile
- **Solution**: Create snapshot before using snapshot features

**Issue**: "Permission denied" in tests
- **Cause**: CONFIG_DIR not writable
- **Solution**: Tests now use temporary directory

## Support

For issues or questions:
1. Check logs in `/app/data/logs/`
2. Verify Dispatcharr connectivity
3. Review API responses in browser dev tools
4. Check configuration files in `/app/data/`

## License

This feature is part of the Streamflow project and follows the same license terms.
