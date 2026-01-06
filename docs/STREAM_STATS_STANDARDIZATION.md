# Stream Statistics Standardization

## Overview

This document describes the standardization of stream statistics handling across the StreamFlow application. Prior to this standardization, different parts of the codebase used inconsistent field names and formatting approaches, leading to:

- Dead stream counts differing between views
- Bitrate showing as "N/A" in some contexts but displaying correctly in others
- Inconsistent data formatting in API responses, changelog entries, and UI displays

## Standardized Field Names

All stream statistics throughout the application now use these standard field names:

### In `stream_stats` Object (from Dispatcharr/UDI)

| Field Name | Type | Description | Example |
|------------|------|-------------|---------|
| `ffmpeg_output_bitrate` | int/float | Bitrate in kbps | `5000` |
| `source_fps` | int/float | Frames per second | `30` or `29.97` |
| `resolution` | string | Resolution in WxH format | `"1920x1080"` |
| `video_codec` | string | Video codec name | `"h264"`, `"hevc"` |
| `audio_codec` | string | Audio codec name | `"aac"`, `"mp3"` |

### Legacy Field Names (Deprecated)

These field names are still supported for backward compatibility but should not be used in new code:

- `bitrate_kbps` → Use `ffmpeg_output_bitrate` instead
- `fps` → Use `source_fps` instead
- `bitrate` → Use `ffmpeg_output_bitrate` instead

## Centralized Utilities

All stream statistics handling is now centralized in `backend/stream_stats_utils.py`:

### Extraction Functions

- **`extract_stream_stats(stream_data)`** - Extracts and normalizes stats from any format
  - Handles `stream_stats` dict, JSON strings, or direct fields
  - Returns standardized dict with `resolution`, `fps`, `bitrate_kbps`, `video_codec`, `audio_codec`

### Parsing Functions

- **`parse_bitrate_value(bitrate_raw)`** - Parses bitrate from any format to kbps
  - Handles: `5000`, `"5000 kbps"`, `"5.0 Mbps"`, `5000.5`
  - Returns: `float` in kbps or `None`

- **`parse_fps_value(fps_raw)`** - Parses FPS from any format
  - Handles: `30`, `"30 fps"`, `"29.97"`
  - Returns: `float` or `None`

### Formatting Functions

- **`format_bitrate(bitrate_kbps)`** - Formats bitrate for display
  - Returns: `"5000 kbps"` or `"5.0 Mbps"` or `"N/A"`
  - Automatically converts to Mbps for values >= 1000 kbps

- **`format_fps(fps)`** - Formats FPS for display
  - Returns: `"30.0 fps"` or `"N/A"`

- **`format_stream_stats_for_display(stats)`** - Formats all stats for UI
  - Takes extracted stats dict
  - Returns dict with all values formatted as strings

### Analysis Functions

- **`is_stream_dead(stream_data)`** - Checks if a stream is dead
  - Dead if: `resolution == "0x0"` OR `bitrate == 0` OR width/height == 0
  - Uses extracted stats for consistency

- **`calculate_channel_averages(streams, dead_stream_ids)`** - Calculates channel averages
  - Excludes dead streams from calculations
  - Returns dict with `avg_resolution`, `avg_bitrate`, `avg_fps` (all formatted)

## Usage Examples

### Extracting Stats from Stream Data

```python
from stream_stats_utils import extract_stream_stats, format_stream_stats_for_display

# From Dispatcharr/UDI response
stream = {
    'id': 123,
    'name': 'My Stream',
    'stream_stats': {
        'resolution': '1920x1080',
        'source_fps': 30,
        'ffmpeg_output_bitrate': 5000
    }
}

# Extract normalized stats
stats = extract_stream_stats(stream)
# Returns: {'resolution': '1920x1080', 'fps': 30.0, 'bitrate_kbps': 5000.0, ...}

# Format for display
formatted = format_stream_stats_for_display(stats)
# Returns: {'resolution': '1920x1080', 'fps': '30.0 fps', 'bitrate': '5.0 Mbps', ...}
```

### Checking if Stream is Dead

```python
from stream_stats_utils import is_stream_dead

stream_data = {
    'stream_stats': {
        'resolution': '0x0',
        'ffmpeg_output_bitrate': 0
    }
}

if is_stream_dead(stream_data):
    print("Stream is dead!")
```

### Calculating Channel Averages

```python
from stream_stats_utils import calculate_channel_averages

streams = [...]  # List of stream dicts
dead_stream_ids = {2, 5, 7}  # IDs of dead streams to exclude

averages = calculate_channel_averages(streams, dead_stream_ids)
# Returns: {'avg_resolution': '1920x1080', 'avg_bitrate': '5.2 Mbps', 'avg_fps': '29.8 fps'}
```

## Migration Guide

### For Backend Code

**Before:**
```python
# Old: Manual extraction with inconsistent field names
stats = stream.get('stream_stats', {})
bitrate = stats.get('bitrate_kbps') or stats.get('ffmpeg_output_bitrate') or 0
fps = stats.get('fps') or stats.get('source_fps') or 0

# Old: Manual formatting
bitrate_display = f"{bitrate} kbps" if bitrate > 0 else "N/A"
```

**After:**
```python
# New: Use centralized utilities
from stream_stats_utils import extract_stream_stats, format_stream_stats_for_display

stats = extract_stream_stats(stream)
formatted = format_stream_stats_for_display(stats)
bitrate_display = formatted['bitrate']  # Automatically formatted
```

### For API Responses

All API endpoints that return stream statistics should use the standardized utilities:

```python
from stream_stats_utils import calculate_channel_averages

# Calculate averages for API response
channel_averages = calculate_channel_averages(streams, dead_stream_ids=set())

return jsonify({
    'avg_resolution': channel_averages['avg_resolution'],
    'avg_bitrate': channel_averages['avg_bitrate'],
    'avg_fps': channel_averages['avg_fps']
})
```

### For Frontend Code

The frontend should expect these standardized formats from API responses:

- Bitrate: `"5000 kbps"` or `"5.0 Mbps"` or `"N/A"`
- FPS: `"30.0 fps"` or `"N/A"`
- Resolution: `"1920x1080"` or `"N/A"`

## Benefits

1. **Consistency** - All parts of the application handle stream stats identically
2. **Maintainability** - Changes to formatting logic only need to be made in one place
3. **Correctness** - Dead stream detection works the same way everywhere
4. **Testability** - Centralized utilities are fully unit tested (31 tests, all passing)
5. **Clarity** - Standard field names make code easier to understand

## Testing

Comprehensive unit tests are available in `backend/tests/test_stream_stats_utils.py`:

```bash
cd backend
python -m unittest tests.test_stream_stats_utils -v
```

All 31 tests should pass, covering:
- Bitrate parsing and formatting
- FPS parsing and formatting
- Resolution normalization
- Stream stats extraction from various formats
- Dead stream detection
- Channel averages calculation

## See Also

- `backend/stream_stats_utils.py` - Implementation
- `backend/tests/test_stream_stats_utils.py` - Unit tests
- `backend/stream_checker_service.py` - Usage example
- `backend/web_api.py` - API integration example
