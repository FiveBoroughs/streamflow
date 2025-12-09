# Features

## Stream Management

### Pipeline-Based Automation
StreamFlow offers 5 pipeline modes to match different usage scenarios:
- **Pipeline 1**: Continuous updates with 2-hour immunity (moderate connection usage)
- **Pipeline 1.5**: Pipeline 1 + scheduled complete checks (balanced approach)
- **Pipeline 2**: Updates and matching only, no automatic checking (minimal connection usage)
- **Pipeline 2.5**: Pipeline 2 + scheduled complete checks (controlled automation)
- **Pipeline 3**: Only scheduled operations (maximum control)

See [PIPELINE_SYSTEM.md](PIPELINE_SYSTEM.md) for detailed pipeline documentation.

### Automated M3U Playlist Management
- Automatically refreshes playlists every 5 minutes (configurable)
- Detects playlist changes in real-time
- Updates channels immediately when M3U refreshes
- Tracks update history in changelog

### Intelligent Stream Quality Checking
Multi-factor analysis of stream quality using a single optimized ffmpeg call:
- **Bitrate**: Average kbps measurement
- **Resolution**: Width × height detection
- **Frame Rate**: FPS analysis
- **Video Codec**: H.265/H.264 identification with automatic sanitization
  - Filters out invalid codec names (e.g., "wrapped_avframe", "unknown")
  - Extracts actual codec from hardware-accelerated streams
- **Audio Codec**: Detection and validation
  - Parses **input stream codecs** only (e.g., "aac", "ac3", "mp3", "eac3")
  - Avoids decoded output formats (e.g., "pcm_s16le")
  - Supports multiple audio streams and language tracks
- **Error Detection**: Decode errors, discontinuities, timeouts
- **Optimized Performance**: Single ffmpeg call instead of separate ffprobe+ffmpeg (reduced overhead)
- **Parallel Checking**: Thread-based concurrent analysis with configurable worker pool
  - Proper pipeline: gather stats in parallel → when ALL checks finish → push info to Dispatcharr
  - Prevents race conditions during concurrent operations

### Automatic Stream Reordering
- Best quality streams automatically moved to top
- Quality score calculation (0.0-1.0 scale)
- Configurable scoring weights
- Preserves stream availability

### Stream Discovery
- Regex pattern matching for automatic assignment
- New stream detection on playlist refresh
- Automatic channel assignment based on patterns
- Pattern testing interface

## Quality Analysis

### Scoring Formula
**Total Score = (Bitrate × 0.30) + (Resolution × 0.25) + (FPS × 0.15) + (Codec × 0.10) + (Errors × 0.20)**

### Configurable Weights
```json
{
  "weights": {
    "bitrate": 0.30,      // Default: 30%
    "resolution": 0.25,   // Default: 25%
    "fps": 0.15,          // Default: 15%
    "codec": 0.10,        // Default: 10%
    "errors": 0.20        // Default: 20%
  }
}
```

### Codec Preferences
- H.265/HEVC preference: Higher score for modern codecs
- Interlaced penalty: Lower score for interlaced content
- Dropped frames penalty: Lower score for streams with frame drops

### Sequential and Parallel Checking
- **Parallel Mode** (default): Concurrent stream checking with configurable worker pool (default: 10)
  - Thread-based parallel execution
  - Configurable global concurrency limit
  - **Per-Account Stream Limits**: Respects maximum concurrent streams for each M3U account
    - Smart scheduler ensures account limits are never exceeded
    - Multiple accounts can check streams in parallel
    - Example: Account A (limit: 1), Account B (limit: 2) with streams A1, A2, B1, B2, B3
      - Concurrently checks: A1, B1, B2 (3 total, respecting limits)
      - When A1 completes, A2 starts; when B1/B2 completes, B3 starts
  - Stagger delay to prevent simultaneous starts
  - Robust pipeline: all stats gathered in parallel, then pushed to Dispatcharr after ALL checks complete
  - Prevents race conditions with dead stream removal
- **Sequential Mode**: One stream at a time for minimal provider load
- Queue-based processing
- Real-time progress tracking

### Dead Stream Detection and Management
Automatically identifies and manages non-functional streams:
- **Detection**: Streams with resolution=0 or bitrate=0 are marked as dead
- **Changelog Tracking**: Dead streams show status "dead" in changelog (not score:0)
- **Revival Tracking**: Revived streams show status "revived" in changelog
- **Removal**: Dead streams are removed from channels during regular checks
- **Revival Check**: During global actions, dead streams are re-checked for revival
- **Matching Exclusion**: Dead streams are not assigned to channels during stream matching
- **Pipeline-Aware**: Only operates in pipelines with stream checking enabled (1, 1.5, 2.5, 3)

## User Interface

### Theme Customization
- **Dark/Light Mode Toggle**: Switch between light, dark, and auto (system) themes
- **Deep Black Dark Mode**: True black background (#000) with white text and dark green accents
- **System Preference Detection**: Automatically follows system theme in auto mode
- **Persistent Settings**: Theme preference saved to local storage

### Dashboard
- System status overview
- Recent activity display
- Quick action buttons (start/stop automation)
- Real-time statistics

### Stream Checker Dashboard
- Service status monitoring
- Real-time statistics (queue size, completed, failed)
- Progress tracking with detailed stream information
- Pipeline information display
- Global Action trigger button
- Queue management (clear queue)

### Channel Configuration
- **Horizontal Channel Cards**: Modern card-based layout with expandable sections
  - Channel logo display (wider than taller for better visibility)
  - Channel name and metadata
  - Real-time statistics:
    - Total stream count
    - Dead stream count
    - Most common resolution
    - Average bitrate (Kbps)
  - Quick actions: Edit Regex, **Check Channel**
- **Single Channel Check**: Immediately check a specific channel's streams
  - Synchronous checking with detailed feedback
  - Shows results: total streams, dead streams, avg resolution, avg bitrate
  - Updates channel stats after completion
  - Creates changelog entry for tracking
- **Expandable Regex Editor**: Toggle pattern list within each card
  - View all configured patterns for the channel
  - Add new patterns inline
  - Delete patterns individually
- **Individual Channel Checking**: Queue single channels for immediate quality checking
- **Live Statistics**: Auto-loading channel stats from backend API
- **Search and Filtering**: Find channels by name, number, or ID
- **Pagination**: Efficient browsing of large channel lists
  - Configurable items per page (10, 20, 50, 100)
  - First/Previous/Next/Last navigation buttons
  - Visual page number selection (shows 5 pages at a time)
  - Current range indicator (e.g., "Showing 1-20 of 150 channels")
  - Automatic reset to first page when search query changes
- **Export/Import**: Backup and restore regex patterns as JSON

### Changelog Page
- **Activity History**: View all system events and stream operations
- **Structured Entries**: Multiple action types:
  - **Playlist Update & Match**: Shows streams added and channels checked
  - **Global Check**: Displays all channels checked during scheduled global actions
  - **Single Channel Check**: Individual channel check results
  - **Batch Stream Check**: Consolidated entries for checking batches
- **Global Statistics**: Summary stats for each action (total channels, successful/failed checks, streams analyzed, dead streams, etc.)
- **Collapsible Subentries**: Expandable dropdown blocks for detailed information
  - **Update & Match** group: Lists streams added to each channel
  - **Check** group: Shows per-channel statistics and scores
- **Time Filtering**: View entries from last 24 hours, 7 days, 30 days, or 90 days
- **Color-Coded Badges**: Visual indicators for different action types
- **Stream Details**: Top streams with resolution, bitrate, codec, and FPS information

### Scheduling Page
- **EPG-Based Event Scheduling**: Schedule channel checks before program events
- **Channel Selection**: Searchable dropdown with all available channels
- **Program Browser**: Scrollable list of upcoming programs for selected channel
  - Displays program title, start/end times, and descriptions
  - Programs loaded from Dispatcharr's EPG grid endpoint
- **Flexible Timing**: Input field for minutes before program start
- **Event Management Table**: View all scheduled events
  - Channel logo and name
  - Program title and time
  - Scheduled check time
  - Delete action button
- **Configuration Options**: Adjust EPG data refresh interval

### Configuration Page (unified)
- **Pipeline Selection**: Choose from 5 automation modes with visual cards and hints
  - Descriptive hints for each pipeline mode
- **Schedule Configuration**: Set timing for global actions (pipelines 1.5, 2.5, 3)
  - Daily or monthly frequency
  - Precise time selection (hour and minute)
  - Day of month for monthly schedules
- **Concurrent Stream Checking**: Configure maximum parallel workers (default: 10)
  - Controls load on streaming providers
  - Adjustable stagger delay between task dispatches
- **Context-Aware Settings**: Only relevant options shown based on selected pipeline
- **Update Intervals**: Configure M3U refresh frequency (for applicable pipelines)
- **Stream Analysis Parameters**: FFmpeg duration, timeouts, retries
- **Queue Settings**: Maximum queue size, channels per run

### Changelog
- Complete activity history
- Date range filtering
- Action categorization
- Detail expansion

### Setup Wizard
- Guided initial configuration
- Dispatcharr connection testing
- **JSON Pattern Import**: Import channel regex patterns from JSON file
- **Pipeline Hints**: Inline descriptions for each pipeline mode
- **Smart Navigation**: Save settings automatically when advancing
- **Autostart Default**: Automation enabled by default
- Configuration validation
- Quick start assistance

## Automation Features

### M3U Update Tracking
- Automatic detection of playlist updates
- Immediate channel queuing on update
- Update timestamp tracking
- Prevents duplicate checking

### Global Actions
- **Manual Trigger**: One-click complete update cycle (Update → Match → Check all)
- **Scheduled Execution**: Automatic runs at configured time (daily or monthly)
- **Force Check**: Bypasses 2-hour immunity to check all streams
- **Pipeline Integration**: Available in pipelines 1.5, 2.5, and 3
- **Configurable Timing**: Precise hour and minute selection for off-peak operation

### Queue Management
- Priority-based queue system
- Manual channel addition
- Queue clearing
- Duplicate prevention

### Real-Time Progress
- Current channel display
- Stream-by-stream progress
- Quality score updates
- Error reporting

## Data Management

### Changelog
- All automation actions logged
- Timestamps and details
- Persistent storage
- Filterable history
- **Batch Consolidation**: Stream checks batched into consolidated entries
  - Single entry per checking batch instead of per-channel entries
  - Aggregate statistics at batch level (total channels, streams analyzed, dead streams, etc.)
  - Individual channel results shown as expandable subentries
  - Cleaner, more organized changelog view

### Configuration Persistence
- All settings in JSON files
- Docker volume mounting
- Easy backup and restore
- Version-agnostic format

### Setup Wizard
- First-run configuration
- Connection validation
- Default settings
- Quick deployment

## EPG-Based Scheduling

### Scheduled Channel Checks Before Events
Schedule channel checks to run before EPG program events for optimal stream quality:
- **EPG Integration**: Fetches program data from Dispatcharr's EPG grid endpoint
- **Configurable Caching**: EPG data cached with configurable refresh interval (default: 60 minutes)
- **Program Search**: Browse upcoming programs by channel
- **Flexible Timing**: Configure minutes before program start to run the check
- **Playlist Updates**: Playlist refresh triggered before scheduled checks
- **Event Management**: Create, view, and delete scheduled events

### User Workflow
1. Navigate to Scheduling section in the UI
2. Click "Add Event Check" button
3. Select a channel from searchable dropdown
4. View and select from channel's upcoming programs
5. Specify minutes before program start for the check
6. Save the scheduled event
7. Monitor scheduled events in table with channel logos and program details

## API Integration

### REST API
- 30+ endpoints
- JSON request/response
- Authentication support
- Error handling

### Real-Time Updates
- Polling for status
- Progress tracking
- Queue monitoring
- Statistics updates

### Dispatcharr Integration
- Full API support
- Token-based authentication
- Channel management
- Stream operations

## Technical Features

### Docker Deployment
- Single container architecture
- Volume-based persistence
- Environment variable configuration
- Health checks

### Error Handling
- Automatic retry logic
- Error logging
- Graceful degradation
- User notifications

### Performance
- Parallel stream checking with configurable worker pool
- Optimized single ffmpeg call (instead of ffprobe + ffmpeg)
- Efficient queue processing
- Minimal API calls
- Resource optimization

### Logging
- Comprehensive activity logs
- Error tracking
- Debug mode support
- Persistent changelog
