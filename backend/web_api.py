#!/usr/bin/env python3
"""
Web API Server for StreamFlow for Dispatcharr

Provides REST API endpoints for the React frontend to interact with
the automated stream management system.
"""

import json
import logging
import os
import requests
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from werkzeug.utils import secure_filename

from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS

from automated_stream_manager import AutomatedStreamManager, RegexChannelMatcher
from api_utils import _get_base_url
from stream_checker_service import get_stream_checker_service
from scheduling_service import get_scheduling_service

# Import UDI for direct data access
from udi import get_udi_manager

# Import centralized stream stats utilities
from stream_stats_utils import (
    extract_stream_stats,
    format_bitrate,
    parse_bitrate_value,
    calculate_channel_averages
)

# Import croniter for cron expression validation
try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False



# Setup centralized logging
from logging_config import setup_logging, log_function_call, log_function_return, log_exception

logger = setup_logging(__name__)

# Configuration constants
CONFIG_DIR = Path(os.environ.get('CONFIG_DIR', '/app/data'))
CONCURRENT_STREAMS_GLOBAL_LIMIT_KEY = 'concurrent_streams.global_limit'
CONCURRENT_STREAMS_ENABLED_KEY = 'concurrent_streams.enabled'

# Initialize Flask app with static file serving
# Note: static_folder set to None to disable Flask's built-in static route
# The catch-all route will handle serving all static files from the React build
static_folder = Path(__file__).parent / 'static'
app = Flask(__name__, static_folder=None)
CORS(app)  # Enable CORS for React frontend

# Global instances
automation_manager = None
regex_matcher = None
scheduled_event_processor_thread = None
scheduled_event_processor_running = False
scheduled_event_processor_wake = None  # threading.Event to wake up the processor early

def get_automation_manager():
    """Get or create automation manager instance."""
    global automation_manager
    if automation_manager is None:
        automation_manager = AutomatedStreamManager()
    return automation_manager

def get_regex_matcher():
    """Get or create regex matcher instance."""
    global regex_matcher
    if regex_matcher is None:
        regex_matcher = RegexChannelMatcher()
    return regex_matcher

def check_wizard_complete():
    """Check if the setup wizard has been completed."""
    try:
        config_file = CONFIG_DIR / 'automation_config.json'
        regex_file = CONFIG_DIR / 'channel_regex_config.json'
        
        # Check if configuration files exist
        if not config_file.exists() or not regex_file.exists():
            return False
        
        # Check if we have patterns configured
        if regex_file.exists():
            matcher = get_regex_matcher()
            patterns = matcher.get_patterns()
            if not patterns.get('patterns'):
                return False
        else:
            return False
        
        # Check if we can connect to Dispatcharr (optional - use cached result)
        # For startup, we'll accept the configuration exists as sufficient
        # The actual connection test will be done by the wizard
        
        return True
    except Exception as e:
        logger.warning(f"Error checking wizard completion status: {e}")
        return False


def scheduled_event_processor():
    """Background thread to process scheduled EPG events.
    
    This function runs in a separate thread and checks for due scheduled events
    periodically. When events are due, it executes the channel checks and
    automatically deletes the completed events.
    
    Uses an event-based approach similar to the global action scheduler for
    better responsiveness. Checks every 30 seconds but can be woken early.
    """
    global scheduled_event_processor_running, scheduled_event_processor_wake
    
    logger.info("Scheduled event processor thread started")
    
    # Check interval in seconds - more frequent than the old 60s for better responsiveness
    check_interval = 30
    
    while scheduled_event_processor_running:
        try:
            # Wait for wake event or timeout (similar to _scheduler_loop pattern)
            # This allows the processor to be woken up early if needed
            if scheduled_event_processor_wake is None:
                # This should not happen during normal operation
                logger.error("Wake event is None! This indicates a programming error. Using fallback sleep.")
                time.sleep(check_interval)
            else:
                scheduled_event_processor_wake.wait(timeout=check_interval)
                scheduled_event_processor_wake.clear()
            
            # Check for due events
            service = get_scheduling_service()
            stream_checker = get_stream_checker_service()
            
            # Get all due events
            due_events = service.get_due_events()
            
            if due_events:
                logger.info(f"Found {len(due_events)} scheduled event(s) due for execution")
                
                for event in due_events:
                    event_id = event.get('id')
                    channel_name = event.get('channel_name', 'Unknown')
                    program_title = event.get('program_title', 'Unknown')
                    
                    logger.info(f"Executing scheduled event {event_id} for {channel_name} (program: {program_title})")
                    
                    try:
                        success = service.execute_scheduled_check(event_id, stream_checker)
                        if success:
                            logger.info(f"✓ Successfully executed and removed scheduled event {event_id}")
                        else:
                            logger.warning(f"✗ Failed to execute scheduled event {event_id}")
                    except Exception as e:
                        logger.error(f"Error executing scheduled event {event_id}: {e}", exc_info=True)
            
        except Exception as e:
            logger.error(f"Error in scheduled event processor: {e}", exc_info=True)
    
    logger.info("Scheduled event processor thread stopped")


def start_scheduled_event_processor():
    """Start the background thread for processing scheduled events."""
    global scheduled_event_processor_thread, scheduled_event_processor_running, scheduled_event_processor_wake
    
    if scheduled_event_processor_thread is not None and scheduled_event_processor_thread.is_alive():
        logger.warning("Scheduled event processor is already running")
        return False
    
    # Initialize wake event for responsive control
    scheduled_event_processor_wake = threading.Event()
    
    scheduled_event_processor_running = True
    scheduled_event_processor_thread = threading.Thread(
        target=scheduled_event_processor,
        name="ScheduledEventProcessor",
        daemon=True  # Daemon thread will exit when main program exits
    )
    scheduled_event_processor_thread.start()
    logger.info("Scheduled event processor started")
    return True


def stop_scheduled_event_processor():
    """Stop the background thread for processing scheduled events."""
    global scheduled_event_processor_thread, scheduled_event_processor_running, scheduled_event_processor_wake
    
    if scheduled_event_processor_thread is None or not scheduled_event_processor_thread.is_alive():
        logger.warning("Scheduled event processor is not running")
        return False
    
    logger.info("Stopping scheduled event processor...")
    scheduled_event_processor_running = False
    
    # Wake the thread so it can exit promptly
    if scheduled_event_processor_wake:
        scheduled_event_processor_wake.set()
    
    # Wait for thread to finish (with timeout)
    scheduled_event_processor_thread.join(timeout=5)
    
    if scheduled_event_processor_thread.is_alive():
        logger.warning("Scheduled event processor thread did not stop gracefully")
        return False
    
    logger.info("Scheduled event processor stopped")
    return True



@app.route('/', methods=['GET'])
def root():
    """Serve React frontend."""
    try:
        return send_file(static_folder / 'index.html')
    except FileNotFoundError:
        # Fallback to API info if frontend not built
        return jsonify({
            "message": "StreamFlow for Dispatcharr API",
            "version": "1.0",
            "endpoints": {
                "health": "/api/health",
                "docs": "/api/health",
                "frontend": "React frontend not found. Build frontend and place in static/ directory."
            }
        })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/health', methods=['GET'])
def health_check_stripped():
    """Health check endpoint for nginx proxy (stripped /api prefix)."""
    return health_check()

@app.route('/api/automation/status', methods=['GET'])
def get_automation_status():
    """Get current automation status."""
    try:
        manager = get_automation_manager()
        status = manager.get_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting automation status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/automation/start', methods=['POST'])
def start_automation():
    """Start the automation system."""
    try:
        manager = get_automation_manager()
        manager.start_automation()
        return jsonify({"message": "Automation started successfully", "status": "running"})
    except Exception as e:
        logger.error(f"Error starting automation: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/automation/stop', methods=['POST'])
def stop_automation():
    """Stop the automation system."""
    try:
        manager = get_automation_manager()
        manager.stop_automation()
        return jsonify({"message": "Automation stopped successfully", "status": "stopped"})
    except Exception as e:
        logger.error(f"Error stopping automation: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/automation/cycle', methods=['POST'])
def run_automation_cycle():
    """Run one automation cycle manually."""
    try:
        manager = get_automation_manager()
        manager.run_automation_cycle()
        return jsonify({"message": "Automation cycle completed successfully"})
    except Exception as e:
        logger.error(f"Error running automation cycle: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/automation/config', methods=['GET'])
def get_automation_config():
    """Get automation configuration."""
    try:
        manager = get_automation_manager()
        return jsonify(manager.config)
    except Exception as e:
        logger.error(f"Error getting automation config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/automation/config', methods=['PUT'])
def update_automation_config():
    """Update automation configuration."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No configuration data provided"}), 400
        
        manager = get_automation_manager()
        manager.update_config(data)
        return jsonify({"message": "Configuration updated successfully", "config": manager.config})
    except Exception as e:
        logger.error(f"Error updating automation config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels', methods=['GET'])
def get_channels():
    """Get all channels from UDI."""
    try:
        udi = get_udi_manager()
        channels = udi.get_channels()
        
        if channels is None:
            return jsonify({"error": "Failed to fetch channels"}), 500
        
        return jsonify(channels)
    except Exception as e:
        logger.error(f"Error fetching channels: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/<channel_id>/stats', methods=['GET'])
def get_channel_stats(channel_id):
    """Get channel statistics including stream count, dead streams, resolution, and bitrate."""
    try:
        # Convert channel_id to int for comparison
        try:
            channel_id_int = int(channel_id)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid channel ID: must be a valid integer"}), 400
        
        udi = get_udi_manager()
        channels = udi.get_channels()
        
        if channels is None:
            return jsonify({"error": "Failed to fetch channels"}), 500
        
        # Find the specific channel - convert to dict for O(1) lookup
        # Filter out any invalid channel objects and build lookup dict
        channels_dict = {ch['id']: ch for ch in channels if isinstance(ch, dict) and 'id' in ch}
        channel = channels_dict.get(channel_id_int)
        
        if not channel:
            return jsonify({"error": "Channel not found"}), 404
        
        # Get streams for this channel
        # channel['streams'] is a list of stream IDs, need to fetch full stream objects
        stream_ids = channel.get('streams', [])
        total_streams = len(stream_ids)
        
        # Fetch full stream objects for each stream ID
        streams = []
        for stream_id in stream_ids:
            if isinstance(stream_id, int):
                stream = udi.get_stream_by_id(stream_id)
                if stream:
                    streams.append(stream)
        
        # Get dead streams count for this channel
        dead_count = 0
        URL_LOG_TRUNCATE_LENGTH = 50
        checker = get_stream_checker_service()
        if checker and checker.dead_streams_tracker:
            dead_streams = checker.dead_streams_tracker.get_dead_streams()
            logger.debug(f"Checking dead streams for channel {channel_id_int}: Total dead URLs in tracker: {len(dead_streams)}")
            
            # Count dead streams for this channel
            for stream in streams:
                stream_url = stream.get('url', '')
                if stream_url:
                    if stream_url in dead_streams:
                        dead_count += 1
                        logger.debug(f"Found dead stream in channel {channel_id_int}: {stream.get('name', 'Unknown')} - URL: {stream_url[:URL_LOG_TRUNCATE_LENGTH]}...")
            
            logger.debug(f"Channel {channel_id_int} has {dead_count} dead streams out of {len(streams)} total streams")
            
            # If we have dead streams in tracker but found none for this channel, log sample URLs for debugging
            if len(dead_streams) > 0 and dead_count == 0 and len(streams) > 0:
                sample_stream_url = streams[0].get('url', 'N/A')[:URL_LOG_TRUNCATE_LENGTH] if streams else 'N/A'
                sample_dead_url = list(dead_streams.keys())[0][:URL_LOG_TRUNCATE_LENGTH] if dead_streams else 'N/A'
                logger.debug(f"Channel {channel_id_int}: No matches found. Sample stream URL: {sample_stream_url}... | Sample dead URL: {sample_dead_url}...")
        else:
            logger.warning(f"Dead streams tracker not available for channel {channel_id_int}")
        
        # Calculate resolution and bitrate statistics using centralized utility
        # This ensures consistent handling across the application
        channel_averages = calculate_channel_averages(streams, dead_stream_ids=set())
        
        # Extract most common resolution
        most_common_resolution = channel_averages.get('avg_resolution', 'Unknown')
        
        # Extract and parse average bitrate
        # The calculate_channel_averages returns formatted string (e.g., "5000 kbps")
        # We need the numeric value for backward compatibility with existing UI
        # TODO: Consider removing this conversion in v2.0 and have UI handle formatted strings
        avg_bitrate_str = channel_averages.get('avg_bitrate', 'N/A')
        avg_bitrate = 0
        if avg_bitrate_str != 'N/A':
            parsed_bitrate = parse_bitrate_value(avg_bitrate_str)
            if parsed_bitrate:
                avg_bitrate = int(parsed_bitrate)
        
        # Build resolutions dict for detailed breakdown (if needed by UI)
        resolutions = {}
        for stream in streams:
            stats = extract_stream_stats(stream)
            resolution = stats.get('resolution', 'Unknown')
            if resolution not in ['Unknown', 'N/A']:
                resolutions[resolution] = resolutions.get(resolution, 0) + 1
        
        return jsonify({
            "channel_id": channel_id_int,
            "channel_name": channel.get('name', ''),
            "logo_id": channel.get('logo_id'),
            "total_streams": total_streams,
            "dead_streams": dead_count,
            "most_common_resolution": most_common_resolution,
            "average_bitrate": avg_bitrate,
            "resolutions": resolutions
        })
    except Exception as e:
        logger.error(f"Error fetching channel stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/groups', methods=['GET'])
def get_channel_groups():
    """Get all channel groups from UDI."""
    try:
        udi = get_udi_manager()
        groups = udi.get_channel_groups()
        
        if groups is None:
            return jsonify({"error": "Failed to fetch channel groups"}), 500
        
        return jsonify(groups)
    except Exception as e:
        logger.error(f"Error fetching channel groups: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/logos/<logo_id>', methods=['GET'])
def get_channel_logo(logo_id):
    """Get channel logo from UDI."""
    try:
        udi = get_udi_manager()
        logo = udi.get_logo_by_id(int(logo_id))
        
        if logo is None:
            return jsonify({"error": "Failed to fetch logo"}), 500
        
        return jsonify(logo)
    except Exception as e:
        logger.error(f"Error fetching logo: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/logos/<logo_id>/cache', methods=['GET'])
def get_channel_logo_cached(logo_id):
    """Download and cache channel logo locally, then serve it.
    
    This endpoint:
    1. Checks if logo is already cached locally
    2. If not, downloads it from Dispatcharr
    3. Saves it to local storage
    4. Serves the cached file
    """
    try:
        # Validate logo_id is a positive integer
        try:
            logo_id_int = int(logo_id)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid logo ID: must be a valid integer"}), 400
        
        if logo_id_int <= 0:
            return jsonify({"error": "Invalid logo ID: must be a positive integer"}), 400
        
        # Create logos cache directory if it doesn't exist
        logos_cache_dir = CONFIG_DIR / 'logos_cache'
        logos_cache_dir.mkdir(exist_ok=True)
        
        # Check if logo is already cached
        logo_filename = f"logo_{logo_id_int}"
        
        # Try common image extensions
        for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg']:
            cached_path = logos_cache_dir / f"{logo_filename}{ext}"
            if cached_path.exists():
                # Serve cached logo
                return send_file(cached_path, mimetype=f'image/{ext[1:]}')
        
        # Logo not cached, download it from Dispatcharr
        udi = get_udi_manager()
        logo = udi.get_logo_by_id(logo_id_int)
        
        if not logo:
            return jsonify({"error": "Logo not found"}), 404
        
        # Get the logo URL from Dispatcharr
        # Prefer cache_url if available, otherwise use url
        dispatcharr_base_url = os.getenv("DISPATCHARR_BASE_URL", "")
        if not dispatcharr_base_url:
            return jsonify({"error": "DISPATCHARR_BASE_URL not configured"}), 500
            
        logo_url = logo.get('cache_url') or logo.get('url')
        
        if not logo_url:
            return jsonify({"error": "Logo URL not available"}), 404
        
        # If cache_url is a relative path, make it absolute
        if logo_url.startswith('/'):
            logo_url = f"{dispatcharr_base_url}{logo_url}"
        
        # Validate URL scheme (must be http or https)
        if not logo_url.startswith(('http://', 'https://')):
            return jsonify({"error": "Invalid logo URL scheme"}), 400
        
        # Download the logo with SSL verification enabled
        logger.info(f"Downloading logo {logo_id_int} from {logo_url}")
        response = requests.get(logo_url, timeout=10, verify=True)
        response.raise_for_status()
        
        # Determine file extension from content-type or URL
        content_type = response.headers.get('content-type', '').lower()
        ext = '.png'  # default
        if 'jpeg' in content_type or 'jpg' in content_type:
            ext = '.jpg'
        elif 'png' in content_type:
            ext = '.png'
        elif 'gif' in content_type:
            ext = '.gif'
        elif 'webp' in content_type:
            ext = '.webp'
        elif 'svg' in content_type:
            ext = '.svg'
        else:
            # Try to extract from URL
            if logo_url.lower().endswith('.jpg') or logo_url.lower().endswith('.jpeg'):
                ext = '.jpg'
            elif logo_url.lower().endswith('.png'):
                ext = '.png'
            elif logo_url.lower().endswith('.gif'):
                ext = '.gif'
            elif logo_url.lower().endswith('.webp'):
                ext = '.webp'
            elif logo_url.lower().endswith('.svg'):
                ext = '.svg'
        
        # Save the logo to cache
        cached_path = logos_cache_dir / f"{logo_filename}{ext}"
        with open(cached_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Cached logo {logo_id} to {cached_path}")
        
        # Serve the cached logo
        mimetype = f'image/{ext[1:]}'
        if ext == '.svg':
            mimetype = 'image/svg+xml'
        return send_file(cached_path, mimetype=mimetype)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading logo {logo_id}: {e}")
        return jsonify({"error": f"Failed to download logo: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Error caching logo {logo_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/regex-patterns', methods=['GET'])
def get_regex_patterns():
    """Get all regex patterns for channel matching."""
    try:
        matcher = get_regex_matcher()
        patterns = matcher.get_patterns()
        return jsonify(patterns)
    except Exception as e:
        logger.error(f"Error getting regex patterns: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/regex-patterns', methods=['POST'])
def add_regex_pattern():
    """Add or update a regex pattern for a channel."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No pattern data provided"}), 400
        
        required_fields = ['channel_id', 'name', 'regex']
        if not all(field in data for field in required_fields):
            return jsonify({"error": f"Missing required fields: {required_fields}"}), 400
        
        matcher = get_regex_matcher()
        matcher.add_channel_pattern(
            data['channel_id'],
            data['name'],
            data['regex'],
            data.get('enabled', True)
        )
        
        return jsonify({"message": "Pattern added/updated successfully"})
    except ValueError as e:
        # Validation errors (e.g., invalid regex) should return 400
        logger.warning(f"Validation error adding regex pattern: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error adding regex pattern: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/regex-patterns/<channel_id>', methods=['DELETE'])
def delete_regex_pattern(channel_id):
    """Delete a regex pattern for a channel."""
    try:
        matcher = get_regex_matcher()
        patterns = matcher.get_patterns()
        
        if 'patterns' in patterns and str(channel_id) in patterns['patterns']:
            del patterns['patterns'][str(channel_id)]
            matcher._save_patterns(patterns)
            return jsonify({"message": "Pattern deleted successfully"})
        else:
            return jsonify({"error": "Pattern not found"}), 404
    except Exception as e:
        logger.error(f"Error deleting regex pattern: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/regex-patterns/import', methods=['POST'])
def import_regex_patterns():
    """Import regex patterns from JSON file."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Validate the JSON structure
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON format: must be an object"}), 400
        
        if 'patterns' not in data:
            return jsonify({"error": "Invalid JSON format: missing 'patterns' field"}), 400
        
        if not isinstance(data['patterns'], dict):
            return jsonify({"error": "Invalid JSON format: 'patterns' must be an object"}), 400
        
        # Validate each pattern
        matcher = get_regex_matcher()
        for channel_id, pattern_data in data['patterns'].items():
            if not isinstance(pattern_data, dict):
                return jsonify({"error": f"Invalid pattern format for channel {channel_id}"}), 400
            
            if 'regex' not in pattern_data:
                return jsonify({"error": f"Missing 'regex' field for channel {channel_id}"}), 400
            
            if not isinstance(pattern_data['regex'], list):
                return jsonify({"error": f"'regex' must be a list for channel {channel_id}"}), 400
            
            # Validate regex patterns
            is_valid, error_msg = matcher.validate_regex_patterns(pattern_data['regex'])
            if not is_valid:
                return jsonify({"error": f"Invalid regex pattern for channel {channel_id}: {error_msg}"}), 400
        
        # If validation passes, save the patterns
        matcher._save_patterns(data)
        
        # Reload patterns to ensure they're in sync
        matcher.reload_patterns()
        
        pattern_count = len(data['patterns'])
        logger.info(f"Imported {pattern_count} regex patterns successfully")
        
        return jsonify({
            "message": f"Successfully imported {pattern_count} patterns",
            "pattern_count": pattern_count
        })
    except Exception as e:
        logger.error(f"Error importing regex patterns: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/test-regex', methods=['POST'])
def test_regex_pattern():
    """Test a regex pattern against a stream name."""
    try:
        data = request.get_json()
        if not data or 'pattern' not in data or 'stream_name' not in data:
            return jsonify({"error": "Missing pattern or stream_name"}), 400
        
        pattern = data['pattern']
        stream_name = data['stream_name']
        case_sensitive = data.get('case_sensitive', False)
        
        import re
        
        search_pattern = pattern if case_sensitive else pattern.lower()
        search_name = stream_name if case_sensitive else stream_name.lower()
        
        # Convert literal spaces in pattern to flexible whitespace regex (\s+)
        # This allows matching streams with different whitespace characters
        search_pattern = re.sub(r' +', r'\\s+', search_pattern)
        
        try:
            match = re.search(search_pattern, search_name)
            return jsonify({
                "matches": bool(match),
                "match_details": {
                    "pattern": pattern,
                    "stream_name": stream_name,
                    "case_sensitive": case_sensitive,
                    "match_start": match.start() if match else None,
                    "match_end": match.end() if match else None,
                    "matched_text": match.group() if match else None
                }
            })
        except re.error as e:
            return jsonify({"error": f"Invalid regex pattern: {str(e)}"}), 400
    except Exception as e:
        logger.error(f"Error testing regex pattern: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/test-regex-live', methods=['POST'])
def test_regex_pattern_live():
    """Test regex patterns against all available streams to see what would be matched."""
    try:
        from api_utils import get_streams
        import re
        
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing request body"}), 400
        
        # Get patterns to test - can be a single pattern or multiple patterns per channel
        patterns = data.get('patterns', [])
        case_sensitive = data.get('case_sensitive', False)
        max_matches_per_pattern = data.get('max_matches', 100)  # Limit results
        
        if not patterns:
            return jsonify({"error": "No patterns provided"}), 400
        
        # Get all available streams
        all_streams = get_streams()
        if not all_streams:
            return jsonify({
                "matches": [],
                "total_streams": 0,
                "message": "No streams available"
            })
        
        results = []
        
        # Test each pattern against all streams
        for pattern_info in patterns:
            channel_id = pattern_info.get('channel_id', 'unknown')
            channel_name = pattern_info.get('channel_name', 'Unknown Channel')
            regex_patterns = pattern_info.get('regex', [])
            
            if not regex_patterns:
                continue
            
            matched_streams = []
            
            for stream in all_streams:
                if not isinstance(stream, dict):
                    continue
                
                stream_name = stream.get('name', '')
                stream_id = stream.get('id')
                
                if not stream_name:
                    continue
                
                search_name = stream_name if case_sensitive else stream_name.lower()
                
                # Test against all regex patterns for this channel
                matched = False
                matched_pattern = None
                
                for pattern in regex_patterns:
                    search_pattern = pattern if case_sensitive else pattern.lower()
                    
                    # Convert literal spaces in pattern to flexible whitespace regex (\s+)
                    # This allows matching streams with different whitespace characters
                    search_pattern = re.sub(r' +', r'\\s+', search_pattern)
                    
                    try:
                        if re.search(search_pattern, search_name):
                            matched = True
                            matched_pattern = pattern
                            break  # Only need one match
                    except re.error as e:
                        logger.warning(f"Invalid regex pattern '{pattern}': {e}")
                        continue
                
                if matched and len(matched_streams) < max_matches_per_pattern:
                    matched_streams.append({
                        "stream_id": stream_id,
                        "stream_name": stream_name,
                        "matched_pattern": matched_pattern
                    })
            
            results.append({
                "channel_id": channel_id,
                "channel_name": channel_name,
                "patterns": regex_patterns,
                "matched_streams": matched_streams,
                "match_count": len(matched_streams)
            })
        
        return jsonify({
            "results": results,
            "total_streams": len(all_streams),
            "case_sensitive": case_sensitive
        })
        
    except Exception as e:
        logger.error(f"Error testing regex patterns live: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/changelog', methods=['GET'])
def get_changelog():
    """Get recent changelog entries from both automation and stream checker."""
    try:
        days = request.args.get('days', 7, type=int)
        
        # Get automation changelog entries
        manager = get_automation_manager()
        automation_changelog = manager.changelog.get_recent_entries(days)
        
        # Get stream checker changelog entries
        stream_checker_changelog = []
        try:
            checker = get_stream_checker_service()
            if checker.changelog:
                stream_checker_changelog = checker.changelog.get_recent_entries(days)
        except Exception as e:
            logger.warning(f"Could not get stream checker changelog: {e}")
        
        # Merge and sort by timestamp (newest first)
        merged_changelog = automation_changelog + stream_checker_changelog
        merged_changelog.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        return jsonify(merged_changelog)
    except Exception as e:
        logger.error(f"Error getting changelog: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/dead-streams', methods=['GET'])
def get_dead_streams():
    """Get dead streams statistics and list."""
    try:
        checker = get_stream_checker_service()
        if not checker or not checker.dead_streams_tracker:
            return jsonify({"error": "Dead streams tracker not available"}), 503
        
        dead_streams = checker.dead_streams_tracker.get_dead_streams()
        
        # Transform to a more frontend-friendly format
        dead_streams_list = []
        for url, info in dead_streams.items():
            dead_streams_list.append({
                'url': url,
                'stream_id': info.get('stream_id'),
                'stream_name': info.get('stream_name'),
                'marked_dead_at': info.get('marked_dead_at')
            })
        
        # Sort by marked_dead_at (newest first)
        dead_streams_list.sort(key=lambda x: x.get('marked_dead_at', ''), reverse=True)
        
        return jsonify({
            "total_dead_streams": len(dead_streams_list),
            "dead_streams": dead_streams_list
        })
    except Exception as e:
        logger.error(f"Error getting dead streams: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/discover-streams', methods=['POST'])
def discover_streams():
    """Trigger stream discovery and assignment (manual Quick Action)."""
    try:
        manager = get_automation_manager()
        # Use force=True to bypass feature flags for manual Quick Actions
        assignments = manager.discover_and_assign_streams(force=True)
        return jsonify({
            "message": "Stream discovery completed",
            "assignments": assignments,
            "total_assigned": sum(assignments.values())
        })
    except Exception as e:
        logger.error(f"Error discovering streams: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/refresh-playlist', methods=['POST'])
def refresh_playlist():
    """Trigger M3U playlist refresh (manual Quick Action)."""
    try:
        data = request.get_json() or {}
        account_id = data.get('account_id')
        
        manager = get_automation_manager()
        # Use force=True to bypass feature flags for manual Quick Actions
        success = manager.refresh_playlists(force=True)
        
        if success:
            return jsonify({"message": "Playlist refresh completed successfully"})
        else:
            return jsonify({"error": "Playlist refresh failed"}), 500
    except Exception as e:
        logger.error(f"Error refreshing playlist: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/m3u-accounts', methods=['GET'])
def get_m3u_accounts_endpoint():
    """Get all M3U accounts from Dispatcharr, filtering out 'custom' account if no custom streams exist and non-active accounts."""
    try:
        from api_utils import get_m3u_accounts, has_custom_streams
        accounts = get_m3u_accounts()
        
        if accounts is None:
            return jsonify({"error": "Failed to fetch M3U accounts"}), 500
        
        # Filter out non-active accounts per Dispatcharr API spec
        accounts = [acc for acc in accounts if acc.get('is_active', True)]
        
        # Check if there are any custom streams using efficient method
        has_custom = has_custom_streams()
        
        # Filter out "custom" M3U account if there are no custom streams
        if not has_custom:
            # Filter accounts by checking name only
            # Only filter accounts named "custom" (case-insensitive)
            # Do not filter based on null URLs as legitimate disabled/file-based accounts may have these
            accounts = [
                acc for acc in accounts 
                if acc.get('name', '').lower() != 'custom'
            ]
        
        return jsonify(accounts)
    except Exception as e:
        logger.error(f"Error fetching M3U accounts: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/setup-wizard', methods=['GET'])
def get_setup_wizard_status():
    """Get setup wizard completion status."""
    try:
        # Check if basic configuration exists
        config_file = CONFIG_DIR / 'automation_config.json'
        regex_file = CONFIG_DIR / 'channel_regex_config.json'
        
        status = {
            "automation_config_exists": config_file.exists(),
            "regex_config_exists": regex_file.exists(),
            "has_patterns": False,
            "has_channels": False,
            "dispatcharr_connection": False
        }
        
        # Check if we have patterns configured
        if regex_file.exists():
            matcher = get_regex_matcher()
            patterns = matcher.get_patterns()
            status["has_patterns"] = bool(patterns.get('patterns'))
        
        # Check if we can connect to Dispatcharr
        # For testing purposes, simulate connection if running in test mode
        test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        
        if test_mode:
            # In test mode, simulate successful connection and channels
            status["dispatcharr_connection"] = True
            status["has_channels"] = True
        else:
            try:
                udi = get_udi_manager()
                channels = udi.get_channels()
                status["dispatcharr_connection"] = channels is not None
                status["has_channels"] = bool(channels)
            except:
                pass
        
        status["setup_complete"] = all([
            status["automation_config_exists"],
            status["regex_config_exists"],
            status["has_patterns"],
            status["has_channels"],
            status["dispatcharr_connection"]
        ])
        
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting setup wizard status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/setup-wizard/create-sample-patterns', methods=['POST'])
def create_sample_patterns():
    """Create sample regex patterns for testing setup completion."""
    try:
        matcher = get_regex_matcher()
        
        # Add some sample patterns
        patterns = {
            "patterns": {
                "1": {
                    "name": "News Channels",
                    "regex": [".*News.*", ".*CNN.*", ".*BBC.*"],
                    "enabled": True
                },
                "2": {
                    "name": "Sports Channels", 
                    "regex": [".*Sport.*", ".*ESPN.*", ".*Fox Sports.*"],
                    "enabled": True
                }
            },
            "global_settings": {
                "case_sensitive": False,
                "require_exact_match": False
            }
        }
        
        # Save the sample patterns
        with open(CONFIG_DIR / 'channel_regex_config.json', 'w') as f:
            json.dump(patterns, f, indent=2)
        
        return jsonify({"message": "Sample patterns created successfully"})
    except Exception as e:
        logger.error(f"Error creating sample patterns: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/dispatcharr/config', methods=['GET'])
def get_dispatcharr_config():
    """Get current Dispatcharr configuration (without exposing password)."""
    try:
        config = {
            "base_url": os.getenv("DISPATCHARR_BASE_URL", ""),
            "username": os.getenv("DISPATCHARR_USER", ""),
            # Never return the password for security reasons
            "has_password": bool(os.getenv("DISPATCHARR_PASS"))
        }
        return jsonify(config)
    except Exception as e:
        logger.error(f"Error getting Dispatcharr config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/dispatcharr/config', methods=['PUT'])
def update_dispatcharr_config():
    """Update Dispatcharr configuration."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No configuration data provided"}), 400
        
        from dotenv import set_key
        env_file = Path('.') / '.env'
        
        # Update environment variables
        if 'base_url' in data:
            base_url = data['base_url'].strip()
            if env_file.exists():
                set_key(env_file, "DISPATCHARR_BASE_URL", base_url)
            os.environ["DISPATCHARR_BASE_URL"] = base_url
        
        if 'username' in data:
            username = data['username'].strip()
            if env_file.exists():
                set_key(env_file, "DISPATCHARR_USER", username)
            os.environ["DISPATCHARR_USER"] = username
        
        if 'password' in data:
            password = data['password']
            if env_file.exists():
                set_key(env_file, "DISPATCHARR_PASS", password)
            os.environ["DISPATCHARR_PASS"] = password
        
        # Clear token when credentials change so we re-authenticate
        if env_file.exists():
            set_key(env_file, "DISPATCHARR_TOKEN", "")
        os.environ["DISPATCHARR_TOKEN"] = ""
        
        return jsonify({"message": "Dispatcharr configuration updated successfully"})
    except Exception as e:
        logger.error(f"Error updating Dispatcharr config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/dispatcharr/test-connection', methods=['POST'])
def test_dispatcharr_connection():
    """Test Dispatcharr connection with provided or existing credentials."""
    try:
        data = request.get_json() or {}
        
        # Temporarily use provided credentials if available, otherwise use existing
        test_base_url = data.get('base_url', os.getenv("DISPATCHARR_BASE_URL"))
        test_username = data.get('username', os.getenv("DISPATCHARR_USER"))
        test_password = data.get('password', os.getenv("DISPATCHARR_PASS"))
        
        if not all([test_base_url, test_username, test_password]):
            return jsonify({
                "success": False,
                "error": "Missing required credentials (base_url, username, password)"
            }), 400
        
        # Test login
        import requests
        login_url = f"{test_base_url}/api/accounts/token/"
        
        try:
            resp = requests.post(
                login_url,
                headers={"Content-Type": "application/json"},
                json={"username": test_username, "password": test_password},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("access") or data.get("token")
            
            if token:
                # Test if we can fetch channels
                channels_url = f"{test_base_url}/api/channels/channels/"
                channels_resp = requests.get(
                    channels_url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/json"
                    },
                    params={'page_size': 1},
                    timeout=10
                )
                
                if channels_resp.status_code == 200:
                    return jsonify({
                        "success": True,
                        "message": "Connection successful"
                    })
                else:
                    return jsonify({
                        "success": False,
                        "error": "Authentication successful but failed to fetch channels"
                    })
            else:
                return jsonify({
                    "success": False,
                    "error": "No token received from Dispatcharr"
                })
        except requests.exceptions.Timeout:
            return jsonify({
                "success": False,
                "error": "Connection timeout. Please check the URL and network connectivity."
            })
        except requests.exceptions.ConnectionError:
            return jsonify({
                "success": False,
                "error": "Could not connect to Dispatcharr. Please check the URL."
            })
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                return jsonify({
                    "success": False,
                    "error": "Invalid username or password"
                })
            else:
                return jsonify({
                    "success": False,
                    "error": f"HTTP error: {e.response.status_code}"
                })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": f"Connection failed: {str(e)}"
            })
            
    except Exception as e:
        logger.error(f"Error testing Dispatcharr connection: {e}")
        return jsonify({"error": str(e)}), 500

# ===== Stream Checker Endpoints =====

@app.route('/api/stream-checker/status', methods=['GET'])
def get_stream_checker_status():
    """Get current stream checker status."""
    try:
        service = get_stream_checker_service()
        status = service.get_status()
        
        # Add parallel checking information
        concurrent_enabled = service.config.get(CONCURRENT_STREAMS_ENABLED_KEY, True)
        global_limit = service.config.get(CONCURRENT_STREAMS_GLOBAL_LIMIT_KEY, 10)
        
        status['parallel'] = {
            'enabled': concurrent_enabled,
            'max_workers': global_limit,
            'mode': 'parallel' if concurrent_enabled else 'sequential'
        }
        
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting stream checker status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/start', methods=['POST'])
def start_stream_checker():
    """Start the stream checker service."""
    try:
        service = get_stream_checker_service()
        service.start()
        return jsonify({"message": "Stream checker started successfully", "status": "running"})
    except Exception as e:
        logger.error(f"Error starting stream checker: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/stop', methods=['POST'])
def stop_stream_checker():
    """Stop the stream checker service."""
    try:
        service = get_stream_checker_service()
        service.stop()
        return jsonify({"message": "Stream checker stopped successfully", "status": "stopped"})
    except Exception as e:
        logger.error(f"Error stopping stream checker: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/queue', methods=['GET'])
def get_stream_checker_queue():
    """Get current queue status."""
    try:
        service = get_stream_checker_service()
        status = service.get_status()
        return jsonify(status.get('queue', {}))
    except Exception as e:
        logger.error(f"Error getting stream checker queue: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/queue/add', methods=['POST'])
def add_to_stream_checker_queue():
    """Add channel(s) to the checking queue."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        service = get_stream_checker_service()
        
        # Handle single channel or multiple channels
        if 'channel_id' in data:
            channel_id = data['channel_id']
            priority = data.get('priority', 10)
            success = service.queue_channel(channel_id, priority)
            if success:
                return jsonify({"message": f"Channel {channel_id} queued successfully"})
            else:
                return jsonify({"error": "Failed to queue channel"}), 500
        
        elif 'channel_ids' in data:
            channel_ids = data['channel_ids']
            priority = data.get('priority', 10)
            added = service.queue_channels(channel_ids, priority)
            return jsonify({"message": f"Queued {added} channels successfully", "added": added})
        
        else:
            return jsonify({"error": "Must provide channel_id or channel_ids"}), 400
    
    except Exception as e:
        logger.error(f"Error adding to stream checker queue: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/queue/clear', methods=['POST'])
def clear_stream_checker_queue():
    """Clear the checking queue."""
    try:
        service = get_stream_checker_service()
        service.clear_queue()
        return jsonify({"message": "Queue cleared successfully"})
    except Exception as e:
        logger.error(f"Error clearing stream checker queue: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/config', methods=['GET'])
def get_stream_checker_config():
    """Get stream checker configuration."""
    try:
        service = get_stream_checker_service()
        return jsonify(service.config.config)
    except Exception as e:
        logger.error(f"Error getting stream checker config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/config', methods=['PUT'])
def update_stream_checker_config():
    """Update stream checker configuration."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No configuration data provided"}), 400
        
        # Validate cron expression if provided
        if 'global_check_schedule' in data and 'cron_expression' in data['global_check_schedule']:
            cron_expr = data['global_check_schedule']['cron_expression']
            if cron_expr:
                if CRONITER_AVAILABLE:
                    try:
                        if not croniter.is_valid(cron_expr):
                            return jsonify({"error": f"Invalid cron expression: {cron_expr}"}), 400
                    except Exception as e:
                        # Log the full error but only return a generic message to the user
                        logger.error(f"Cron expression validation error: {e}")
                        return jsonify({"error": "Invalid cron expression format"}), 400
                else:
                    logger.warning("croniter not available - cron expression validation skipped")
        
        service = get_stream_checker_service()
        service.update_config(data)
        
        # Auto-start or stop services based on pipeline mode when wizard is complete
        if 'pipeline_mode' in data and check_wizard_complete():
            pipeline_mode = data['pipeline_mode']
            manager = get_automation_manager()
            
            if pipeline_mode == 'disabled':
                # Stop services if pipeline is disabled
                if service.running:
                    service.stop()
                    logger.info("Stream checker service stopped (pipeline disabled)")
                if manager.running:
                    manager.stop_automation()
                    logger.info("Automation service stopped (pipeline disabled)")
            else:
                # Start services if pipeline is active and they're not already running
                if not service.running:
                    service.start()
                    logger.info(f"Stream checker service auto-started after config update (mode: {pipeline_mode})")
                if not manager.running:
                    manager.start_automation()
                    logger.info(f"Automation service auto-started after config update (mode: {pipeline_mode})")
        
        return jsonify({"message": "Configuration updated successfully", "config": service.config.config})
    except Exception as e:
        logger.error(f"Error updating stream checker config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/progress', methods=['GET'])
def get_stream_checker_progress():
    """Get current checking progress."""
    try:
        service = get_stream_checker_service()
        status = service.get_status()
        return jsonify(status.get('progress', {}))
    except Exception as e:
        logger.error(f"Error getting stream checker progress: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/check-channel', methods=['POST'])
def check_specific_channel():
    """Manually check a specific channel immediately (add to queue with high priority)."""
    try:
        data = request.get_json()
        if not data or 'channel_id' not in data:
            return jsonify({"error": "channel_id required"}), 400
        
        channel_id = data['channel_id']
        service = get_stream_checker_service()
        
        # Add with highest priority
        success = service.queue_channel(channel_id, priority=100)
        if success:
            return jsonify({"message": f"Channel {channel_id} queued for immediate checking"})
        else:
            return jsonify({"error": "Failed to queue channel"}), 500
    
    except Exception as e:
        logger.error(f"Error checking specific channel: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/check-single-channel', methods=['POST'])
def check_single_channel_now():
    """Immediately check a single channel synchronously and return results."""
    try:
        data = request.get_json()
        if not data or 'channel_id' not in data:
            return jsonify({"error": "channel_id required"}), 400
        
        channel_id = data['channel_id']
        service = get_stream_checker_service()
        
        # Perform synchronous check
        result = service.check_single_channel(channel_id)
        
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
    
    except Exception as e:
        logger.error(f"Error checking single channel: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/mark-updated', methods=['POST'])
def mark_channels_updated():
    """Mark channels as updated (triggered by M3U refresh)."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        service = get_stream_checker_service()
        
        if 'channel_id' in data:
            channel_id = data['channel_id']
            service.update_tracker.mark_channel_updated(channel_id)
            return jsonify({"message": f"Channel {channel_id} marked as updated"})
        
        elif 'channel_ids' in data:
            channel_ids = data['channel_ids']
            service.update_tracker.mark_channels_updated(channel_ids)
            return jsonify({"message": f"Marked {len(channel_ids)} channels as updated"})
        
        else:
            return jsonify({"error": "Must provide channel_id or channel_ids"}), 400
    
    except Exception as e:
        logger.error(f"Error marking channels updated: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/queue-all', methods=['POST'])
def queue_all_channels():
    """Queue all channels for checking (manual trigger for full check)."""
    try:
        service = get_stream_checker_service()
        
        # Fetch all channels from UDI
        udi = get_udi_manager()
        channels = udi.get_channels()
        
        if not channels:
            return jsonify({"error": "Could not fetch channels"}), 500
        
        channel_ids = [ch['id'] for ch in channels if isinstance(ch, dict) and 'id' in ch]
        
        if not channel_ids:
            return jsonify({"message": "No channels found to queue", "count": 0})
        
        # Mark all channels as updated and add to queue
        service.update_tracker.mark_channels_updated(channel_ids)
        added = service.check_queue.add_channels(channel_ids, priority=10)
        
        return jsonify({
            "message": f"Queued {added} channels for checking",
            "total_channels": len(channel_ids),
            "queued": added
        })
    
    except Exception as e:
        logger.error(f"Error queueing all channels: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stream-checker/global-action', methods=['POST'])
def trigger_global_action():
    """Trigger a manual global action (Update M3U, Match streams, Check all channels).
    
    This performs a complete global action that:
    1. Reloads enabled M3U accounts
    2. Matches new streams with regex patterns
    3. Checks every channel, bypassing 2-hour immunity
    """
    try:
        service = get_stream_checker_service()
        
        if not service.running:
            return jsonify({"error": "Stream checker service is not running"}), 400
        
        success = service.trigger_global_action()
        
        if success:
            return jsonify({
                "message": "Global action triggered successfully",
                "status": "in_progress",
                "description": "Update, Match, and Check all channels in progress"
            })
        else:
            return jsonify({"error": "Failed to trigger global action"}), 500
    
    except Exception as e:
        logger.error(f"Error triggering global action: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================================
# Scheduling API Endpoints
# ============================================================================

@app.route('/api/scheduling/config', methods=['GET'])
@log_function_call
def get_scheduling_config():
    """Get scheduling configuration including EPG refresh interval."""
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        config = service.get_config()
        return jsonify(config)
    except Exception as e:
        logger.error(f"Error getting scheduling config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scheduling/config', methods=['PUT'])
@log_function_call
def update_scheduling_config():
    """Update scheduling configuration.
    
    Expected JSON body:
    {
        "epg_refresh_interval_minutes": 60,
        "enabled": true
    }
    """
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        config = request.get_json()
        
        if not config:
            return jsonify({"error": "No configuration provided"}), 400
        
        success = service.update_config(config)
        
        if success:
            return jsonify({"message": "Configuration updated", "config": service.get_config()})
        else:
            return jsonify({"error": "Failed to save configuration"}), 500
    
    except Exception as e:
        logger.error(f"Error updating scheduling config: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scheduling/epg/grid', methods=['GET'])
@log_function_call
def get_epg_grid():
    """Get EPG grid data (all programs for next 24 hours).
    
    Query parameters:
    - force_refresh: If true, bypass cache and fetch fresh data
    """
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        
        programs = service.fetch_epg_grid(force_refresh=force_refresh)
        return jsonify(programs)
    
    except Exception as e:
        logger.error(f"Error fetching EPG grid: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scheduling/epg/channel/<int:channel_id>', methods=['GET'])
@log_function_call
def get_channel_programs(channel_id):
    """Get programs for a specific channel.
    
    Args:
        channel_id: Channel ID
    
    Returns:
        List of programs for the channel
    """
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        
        programs = service.get_programs_by_channel(channel_id)
        return jsonify(programs)
    
    except Exception as e:
        logger.error(f"Error fetching programs for channel {channel_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scheduling/events', methods=['GET'])
@log_function_call
def get_scheduled_events():
    """Get all scheduled events."""
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        events = service.get_scheduled_events()
        return jsonify(events)
    except Exception as e:
        logger.error(f"Error getting scheduled events: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scheduling/events', methods=['POST'])
@log_function_call
def create_scheduled_event():
    """Create a new scheduled event.
    
    Expected JSON body:
    {
        "channel_id": 123,
        "program_start_time": "2024-01-01T10:00:00Z",
        "program_end_time": "2024-01-01T11:00:00Z",
        "program_title": "Program Name",
        "minutes_before": 5
    }
    """
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        event_data = request.get_json()
        
        if not event_data:
            return jsonify({"error": "No event data provided"}), 400
        
        # Validate required fields
        required_fields = ['channel_id', 'program_start_time', 'program_end_time', 'program_title']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        event = service.create_scheduled_event(event_data)
        
        # Wake up the processor to check for new events immediately
        global scheduled_event_processor_wake
        if scheduled_event_processor_wake:
            scheduled_event_processor_wake.set()
        
        return jsonify(event), 201
    
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error creating scheduled event: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scheduling/events/<event_id>', methods=['DELETE'])
@log_function_call
def delete_scheduled_event(event_id):
    """Delete a scheduled event.
    
    Args:
        event_id: Event ID
    """
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        
        success = service.delete_scheduled_event(event_id)
        
        if success:
            return jsonify({"message": "Event deleted"}), 200
        else:
            return jsonify({"error": "Event not found"}), 404
    
    except Exception as e:
        logger.error(f"Error deleting scheduled event: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/scheduling/process-due-events', methods=['POST'])
@log_function_call
def process_due_scheduled_events():
    """Process all scheduled events that are due for execution.
    
    This endpoint should be called periodically (e.g., by a cron job or scheduler)
    to check for and execute any scheduled channel checks.
    
    Returns:
        JSON with execution results
    """
    try:
        from scheduling_service import get_scheduling_service
        service = get_scheduling_service()
        stream_checker = get_stream_checker_service()
        
        # Get all due events
        due_events = service.get_due_events()
        
        if not due_events:
            return jsonify({
                "message": "No events due for execution",
                "processed": 0
            }), 200
        
        results = []
        for event in due_events:
            event_id = event.get('id')
            channel_name = event.get('channel_name', 'Unknown')
            program_title = event.get('program_title', 'Unknown')
            
            logger.info(f"Processing due event {event_id} for {channel_name} (program: {program_title})")
            
            success = service.execute_scheduled_check(event_id, stream_checker)
            results.append({
                'event_id': event_id,
                'channel_name': channel_name,
                'program_title': program_title,
                'success': success
            })
        
        successful = sum(1 for r in results if r['success'])
        
        return jsonify({
            "message": f"Processed {len(results)} event(s), {successful} successful",
            "processed": len(results),
            "successful": successful,
            "results": results
        }), 200
    
    except Exception as e:
        logger.error(f"Error processing due scheduled events: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/scheduling/processor/status', methods=['GET'])
@log_function_call
def get_scheduled_event_processor_status():
    """Get the status of the scheduled event processor background thread.
    
    Returns:
        JSON with processor status
    """
    try:
        global scheduled_event_processor_thread, scheduled_event_processor_running
        
        thread_alive = scheduled_event_processor_thread is not None and scheduled_event_processor_thread.is_alive()
        is_running = thread_alive and scheduled_event_processor_running
        
        return jsonify({
            "running": is_running,
            "thread_alive": thread_alive
        }), 200
    
    except Exception as e:
        logger.error(f"Error getting scheduled event processor status: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/scheduling/processor/start', methods=['POST'])
@log_function_call
def start_scheduled_event_processor_api():
    """Start the scheduled event processor background thread.
    
    Returns:
        JSON with result
    """
    try:
        success = start_scheduled_event_processor()
        
        if success:
            return jsonify({"message": "Scheduled event processor started"}), 200
        else:
            return jsonify({"message": "Scheduled event processor is already running"}), 200
    
    except Exception as e:
        logger.error(f"Error starting scheduled event processor: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/scheduling/processor/stop', methods=['POST'])
@log_function_call
def stop_scheduled_event_processor_api():
    """Stop the scheduled event processor background thread.
    
    Returns:
        JSON with result
    """
    try:
        success = stop_scheduled_event_processor()
        
        if success:
            return jsonify({"message": "Scheduled event processor stopped"}), 200
        else:
            return jsonify({"message": "Scheduled event processor is not running"}), 200
    
    except Exception as e:
        logger.error(f"Error stopping scheduled event processor: {e}")
        return jsonify({"error": str(e)}), 500


# Serve React app for all frontend routes (catch-all - must be last!)
@app.route('/<path:path>')
def serve_frontend(path):
    """Serve React frontend files or return index.html for client-side routing."""
    file_path = static_folder / path
    if file_path.exists() and file_path.is_file():
        return send_from_directory(static_folder, path)
    else:
        # Return index.html for client-side routing (React Router)
        try:
            return send_file(static_folder / 'index.html')
        except FileNotFoundError:
            return jsonify({"error": "Frontend not found"}), 404

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='StreamFlow for Dispatcharr Web API')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    logger.info(f"Starting StreamFlow for Dispatcharr Web API on {args.host}:{args.port}")
    
    # Auto-start stream checker service if enabled and pipeline mode is not disabled AND wizard is complete
    try:
        # Check if wizard has been completed
        if not check_wizard_complete():
            logger.info("Stream checker service will not start - setup wizard has not been completed")
        else:
            service = get_stream_checker_service()
            pipeline_mode = service.config.get('pipeline_mode', 'pipeline_1_5')
            
            if pipeline_mode == 'disabled':
                logger.info("Stream checker service is disabled via pipeline mode")
            elif service.config.get('enabled', True):
                service.start()
                logger.info(f"Stream checker service auto-started (mode: {pipeline_mode})")
            else:
                logger.info("Stream checker service is disabled in configuration")
    except Exception as e:
        logger.error(f"Failed to auto-start stream checker service: {e}")
    
    # Auto-start automation service if pipeline mode is not disabled AND wizard is complete
    # When any pipeline other than disabled is selected, automation should auto-start
    try:
        # Check if wizard has been completed
        if not check_wizard_complete():
            logger.info("Automation service will not start - setup wizard has not been completed")
        else:
            manager = get_automation_manager()
            service = get_stream_checker_service()
            pipeline_mode = service.config.get('pipeline_mode', 'pipeline_1_5')
            
            if pipeline_mode == 'disabled':
                logger.info("Automation service is disabled via pipeline mode")
            else:
                # Auto-start automation for any active pipeline
                manager.start_automation()
                logger.info(f"Automation service auto-started (mode: {pipeline_mode})")
    except Exception as e:
        logger.error(f"Failed to auto-start automation service: {e}")
    
    # Auto-start scheduled event processor if wizard is complete
    try:
        if not check_wizard_complete():
            logger.info("Scheduled event processor will not start - setup wizard has not been completed")
        else:
            start_scheduled_event_processor()
            logger.info("Scheduled event processor auto-started")
    except Exception as e:
        logger.error(f"Failed to auto-start scheduled event processor: {e}")
    
    app.run(host=args.host, port=args.port, debug=args.debug)