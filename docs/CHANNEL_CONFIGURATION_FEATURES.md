# Channel Configuration Features

## Overview
The Channel Configuration page provides comprehensive tools for managing channels, their regex patterns, ordering, and group-level settings through three main tabs.

## Tabs

### 1. Regex Configuration Tab
Manage stream matching patterns for individual channels.

### 2. Group Management Tab
Control stream matching and checking settings for entire channel groups.

### 3. Channel Order Tab
Organize and reorder channels using drag-and-drop functionality.

## Tab Features

### Regex Configuration Tab Features

### 1. Search/Filter Field
- **Location**: Top of the table, below the page description
- **Functionality**: Real-time filtering of channels
- **Search by**:
  - Channel Number (e.g., "101", "5")
  - Channel Name (e.g., "ESPN", "CNN")
- **Case insensitive**: Search works regardless of case

**Example Usage**:
- Type "ESPN" to see all ESPN channels
- Type "5" to see channels numbered with 5 (5, 50, 105, etc.)

### 2. Separate Columns
The channel information is now split into two separate columns for better readability:
- **Channel Number**: Shows the channel number (e.g., #101)
- **Channel Name**: Shows the channel name (e.g., ESPN)

**Before**: `#101 - ESPN` (single column)
**After**: `#101` | `ESPN` (two columns)

### 3. Sortable Columns
All main columns can be sorted by clicking the column header:

#### Channel Number
- **Default**: Ascending (1, 2, 3...)
- **Click once**: Descending (999, 998, 997...)
- **Click again**: Ascending (1, 2, 3...)

#### Channel Name
- **Alphabetically**: A-Z or Z-A
- **Case insensitive sorting**

#### Patterns
- **Sorts by**: Number of regex patterns configured
- **Useful for**: Finding channels with no patterns or most patterns

#### Status
- **Sorts by**: Enabled (1) vs Disabled (0)
- **Useful for**: Grouping enabled/disabled channels

### 4. Visual Indicators
- **Sort arrows**: Up/down arrows appear on active sorted column
- **Hover effect**: Column headers highlight on hover
- **Active indicator**: Sorted column is visually distinguished

## UI Components

### Table Layout
```
+----------------+---------------+------------------+----------+---------+
| Channel Number | Channel Name  | Patterns         | Status   | Actions |
+----------------+---------------+------------------+----------+---------+
| #5             | ABC News      | .*ABC.*         | Enabled  | ✏️ 🗑️   |
| #101           | ESPN          | .*ESPN.*        | Enabled  | ✏️ 🗑️   |
| #505           | CNN           | No patterns     | -        | ➕      |
+----------------+---------------+------------------+----------+---------+
```

### Search Field
```
┌────────────────────────────────────────────────┐
│ 🔍 Search by channel number or name...        │
└────────────────────────────────────────────────┘
```

## Technical Implementation

### State Management
```javascript
const [searchQuery, setSearchQuery] = useState('');
const [orderBy, setOrderBy] = useState('channel_number');
const [order, setOrder] = useState('asc');
```

### Sorting Logic
- **Channel Number**: Numeric comparison
- **Channel Name**: String comparison (case insensitive)
- **Patterns**: Count of regex patterns
- **Status**: Boolean (enabled = 1, disabled = 0)

### Filtering Logic
- Searches both channel number and name fields
- Case insensitive matching
- Partial match support (searches for substring)

## User Workflow Examples

### Example 1: Find a Specific Channel
1. Type channel name or number in search field
2. Table filters in real-time
3. Edit or add patterns as needed

### Example 2: Sort by Pattern Count
1. Click "Patterns" column header
2. Channels with most patterns appear first (descending)
3. Click again to see channels with fewest patterns (ascending)
4. Identify channels needing pattern configuration

### Example 3: Review All Enabled Channels
1. Click "Status" column header
2. All enabled channels grouped together
3. Review and manage active patterns

## Benefits

1. **Faster Navigation**: Quickly find channels without scrolling
2. **Better Organization**: Sort by any column to group related items
3. **Clear Separation**: Channel number and name are distinct
4. **Improved UX**: Intuitive sorting with visual feedback
5. **Scalability**: Works well with hundreds of channels

## Group Management Tab Features

### Overview
The Group Management tab allows you to control stream matching and checking behavior for entire channel groups at once, providing a more efficient way to manage large numbers of channels.

### Group Settings

Each channel group can be configured with two independent settings:

#### 1. Stream Matching
- **Enabled**: Channels in this group will be included in stream matching operations
- **Disabled**: Channels in this group will not participate in stream matching

#### 2. Stream Checking
- **Enabled**: Streams for channels in this group will be quality checked
- **Disabled**: Streams for channels in this group will skip quality checking

### Visibility Rules

**Important**: When BOTH settings (Stream Matching AND Stream Checking) are disabled for a group:
- All channels from that group will be **hidden** from the Regex Configuration tab
- All channels from that group will be **hidden** from the Channel Order tab
- This helps keep your interface clean by only showing channels that are actively being managed

### Group Card Display

Each group shows:
- **Group Name**: The name of the channel group
- **Channel Count**: Number of channels in the group
- **Group ID**: The unique identifier for the group
- **Settings Controls**: Dropdowns to enable/disable matching and checking
- **Warning Badge**: Displayed when both settings are disabled

### Use Cases

#### Example 1: Disable Sports Channels
1. Navigate to Group Management tab
2. Find the "Sports" group
3. Disable both Stream Matching and Stream Checking
4. Sports channels will no longer appear in other tabs

#### Example 2: Enable Quality Checking Only
1. Find a channel group
2. Set Stream Matching to "Disabled"
3. Set Stream Checking to "Enabled"
4. Channels will appear in tabs and be quality checked, but won't participate in stream matching

#### Example 3: Bulk Management
1. Quickly review all groups at once
2. Enable/disable settings for multiple groups
3. Changes apply immediately to all channels in each group

### Technical Details

- **Persistence**: Group settings are saved to disk and survive restarts
- **Real-time Updates**: Changes apply immediately via API
- **No Channel Modification**: Group settings don't modify individual channel configurations
- **Inheritance**: Individual channel settings take precedence over group settings

## Channel Order Tab Features

### Overview
Organize your channels using an intuitive drag-and-drop interface with multiple sorting options.

### Features

1. **Drag and Drop Reordering**: Click and drag channels to change their order
2. **Sort Options**:
   - Custom Order: Manual drag-and-drop arrangement
   - Channel Number: Sort by channel number (1, 2, 3...)
   - Name (A-Z): Alphabetical sorting
   - ID: Sort by internal channel ID
3. **Visible Channel Filtering**: Only shows channels from groups with enabled settings
4. **Unsaved Changes Detection**: Shows alert when you have pending changes
5. **Save/Reset Actions**: Easily save or discard your ordering changes

### Workflow

1. Switch to the Channel Order tab
2. Use the sort dropdown to organize channels by your preferred method
3. Optionally, drag and drop channels to fine-tune the order
4. Click "Save Order" to persist your changes
5. Or click "Reset" to discard changes

## Backwards Compatibility

All existing functionality is preserved:
- ✅ Add new patterns
- ✅ Edit existing patterns  
- ✅ Delete patterns
- ✅ Test patterns against live streams
- ✅ Enable/disable patterns
- ✅ Manage channel groups
- ✅ Reorder channels

## Future Enhancements

Potential improvements:
- Multi-column sorting in Regex Configuration
- Advanced filters (status, pattern count ranges)
- Bulk pattern operations across multiple channels
- Export/import group settings
- Per-group regex templates
- Channel group creation and editing from UI
