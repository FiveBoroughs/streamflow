import datetime
import argparse
import configparser
import csv
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict
# Removed ThreadPoolExecutor - now using synchronous processing
# from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv

from api_utils import (
    _get_base_url,
    fetch_channel_streams,
    fetch_data_from_url,
    login,
    update_channel_streams,
    patch_request,
    refresh_m3u_playlists,
)

# --- Setup ---
# Setup centralized logging
from logging_config import setup_logging, log_function_call, log_function_return, log_exception

logger = setup_logging(__name__)

# --- Progress Tracking ---
class StreamCheckProgress:
    """Manages progress tracking for stream analysis operations."""
    
    def __init__(self, progress_file=None):
        if progress_file is None:
            # Use CONFIG_DIR if available (Docker environment), otherwise fall back to local csv directory
            config_dir = os.environ.get('CONFIG_DIR', str(Path(__file__).parent))
            progress_file = Path(config_dir) / 'csv' / 'stream_check_progress.json'
        self.progress_file = Path(progress_file)
        self.lock = threading.Lock()
        
    def update(self, current, total, current_stream_name=''):
        """Update progress information."""
        with self.lock:
            progress_data = {
                'current': current,
                'total': total,
                'percentage': round((current / total * 100) if total > 0 else 0, 1),
                'current_stream_name': current_stream_name,
                'timestamp': datetime.now().isoformat(),
                'in_progress': current < total
            }
            
            # Ensure directory exists
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write progress to file with explicit flush
            try:
                with open(self.progress_file, 'w') as f:
                    json.dump(progress_data, f)
                    f.flush()  # Ensure data is written to disk immediately
                    os.fsync(f.fileno())  # Force write to disk
            except Exception as e:
                logger.warning(f"Failed to write progress file: {e}")
    
    def clear(self):
        """Clear progress tracking (analysis complete)."""
        with self.lock:
            if self.progress_file.exists():
                try:
                    self.progress_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete progress file: {e}")

# --- Configuration ---
def load_config():
    """Loads the configuration from the config.ini file."""
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    if not config_path.exists():
        logger.error(f"Configuration file not found at: {config_path}")
        sys.exit(1)
    config.read(config_path)
    return config

# --- Main Functionality ---

def fetch_streams(config, output_file, channel_ids=None):
    """Fetches streams for channels based on group and/or range filters, or specific channel IDs.
    
    Args:
        config: Configuration object
        output_file: Path to output CSV file
        channel_ids: Optional list of specific channel IDs to fetch (overrides config filters)
    """
    logger.info("="*80)
    logger.info("STARTING FETCH STREAMS OPERATION")
    logger.info("="*80)
    
    settings = config['script_settings']
    try:
        group_ids_str = settings.get('channel_group_ids', 'ALL').strip()
        start_range = settings.getint('start_channel', 1)
        end_range = settings.getint('end_channel', 999)
        logger.info(f"Configuration loaded - Groups: {group_ids_str}, Channel range: {start_range}-{end_range}")
    except ValueError:
        logger.error("Invalid number format in config.ini for start/end channel. Please provide valid integers.")
        return

    # --- Fetch initial data ---
    logger.info("Fetching base URL from environment...")
    base_url = _get_base_url()
    if not base_url:
        logger.error("DISPATCHARR_BASE_URL not set in .env file.")
        return
    logger.info(f"Base URL: {base_url}")

    logger.info("Fetching channel groups from API...")
    groups = fetch_data_from_url(f"{base_url}/api/channels/groups/")
    if not groups:
        logger.error("Could not fetch groups. Aborting.")
        return
    logger.info(f"Successfully fetched {len(groups)} groups")
    
    with open("csv/00_channel_groups.csv", mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name"])
        for group in groups:
            writer.writerow([group.get("id", ""), group.get("name", "")])
    logger.info("Saved group list to csv/00_channel_groups.csv")

    logger.info("Fetching all channels from API...")
    all_channels = fetch_data_from_url(f"{base_url}/api/channels/channels/")
    if not all_channels:
        logger.error("Could not fetch channels. Aborting.")
        return
    logger.info(f"Successfully fetched {len(all_channels)} channels")

    # --- Filtering Logic ---
    logger.info("Applying channel filters...")
    target_channels = []
    
    # If specific channel IDs provided, use those (overrides config filters)
    if channel_ids:
        channel_ids_set = set(int(cid) for cid in channel_ids)
        target_channels = [ch for ch in all_channels if ch.get('id') in channel_ids_set]
        logger.info(f"Filtering for specific channel IDs: {channel_ids}")
        logger.info(f"Found {len(target_channels)} channels matching the provided IDs")
    else:
        # Use config-based filtering
        use_group_filter = group_ids_str.upper() != 'ALL'

        if use_group_filter:
            try:
                target_group_ids = {int(gid.strip()) for gid in group_ids_str.split(',')}
                logger.info(f"Filtering for channels in groups: {target_group_ids}")
                target_channels = [ch for ch in all_channels if ch.get('channel_group_id') in target_group_ids]
                logger.info(f"After group filter: {len(target_channels)} channels remain")
            except ValueError:
                logger.error(f"Invalid channel_group_ids in config.ini: '{group_ids_str}'. Please use a comma-separated list of numbers.")
                return
        else:
            logger.info("No specific groups selected (ALL). Using channel number range as primary filter.")
            target_channels = all_channels

        # Apply channel number range as a secondary filter (only if not using specific channel IDs)
        logger.info(f"Applying channel number range filter: {start_range}-{end_range}")
        target_channels = [
            ch for ch in target_channels
            if ch.get("channel_number") and start_range <= int(ch["channel_number"]) <= end_range
        ]
        logger.info(f"After range filter: {len(target_channels)} channels remain")

    final_filtered_channels = target_channels

    if not final_filtered_channels:
        if channel_ids:
            logger.warning(f"No channels found matching the provided channel IDs: {channel_ids}")
        else:
            logger.error("Conflict in filters: No channels were found that match BOTH the selected group(s) and the channel number range. Please check your config.ini. Aborting.")
        return

    logger.info(f"FINAL: {len(final_filtered_channels)} channels to process after applying all filters.")

    # --- Write metadata and streams for filtered channels ---
    logger.info("Writing channel metadata to csv/01_channels_metadata.csv...")
    with open("csv/01_channels_metadata.csv", mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        headers = ["id", "channel_number", "name", "channel_group_id", "tvg_id", "tvc_guide_stationid", "epg_data_id", "logo_id"]
        writer.writerow(headers)
        for ch in final_filtered_channels:
            writer.writerow([ch.get(h, "") for h in headers])
    logger.info("Successfully saved channel metadata")

    logger.info(f"Starting to fetch streams for {len(final_filtered_channels)} channels...")
    logger.info(f"Output file: {output_file}")
    
    total_streams_count = 0
    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Add channel_group_id to the header
        writer.writerow(["channel_number", "channel_id", "channel_group_id", "stream_id", "stream_name", "stream_url"])

        for idx, channel in enumerate(final_filtered_channels, 1):
            channel_id = channel.get("id")
            channel_number = channel.get("channel_number")
            channel_group_id = channel.get("channel_group_id") # Get group ID
            channel_name = channel.get("name", "")

            logger.info(f"[{idx}/{len(final_filtered_channels)}] Fetching streams for channel {channel_number} (Group: {channel_group_id}, ID: {channel_id}) - {channel_name}...")
            streams = fetch_channel_streams(channel_id)
            if not streams:
                logger.warning(f"  No streams found for channel {channel_number} ({channel_name})")
                continue

            for stream in streams:
                writer.writerow([
                    channel_number,
                    channel_id,
                    channel_group_id, # Write group ID to the CSV
                    stream.get("id", ""),
                    stream.get("name", ""),
                    stream.get("url", "")
                ])
                total_streams_count += 1
            logger.info(f"  ✓ Saved {len(streams)} streams for channel {channel_number} ({channel_name})")

    logger.info("="*80)
    logger.info(f"FETCH COMPLETE! Total streams fetched: {total_streams_count}")
    logger.info(f"Output saved to: {output_file}")
    logger.info("="*80)


# --- Stream Analysis ---

provider_semaphores = {}
semaphore_lock = threading.Lock()

def _check_ffmpeg_installed():
    """Checks if ffmpeg and ffprobe are installed and in PATH."""
    try:
        subprocess.run(['ffmpeg', '-h'], capture_output=True, check=True, text=True)
        subprocess.run(['ffprobe', '-h'], capture_output=True, check=True, text=True)
        return True
    except FileNotFoundError:
        logger.error("ffmpeg or ffprobe not found. Please install them and ensure they are in your system's PATH.")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Error checking ffmpeg/ffprobe installation: {e}")
        return False

def _get_stream_info(url, timeout, user_agent='VLC/3.0.14'):
    """Gets stream information using ffprobe."""
    logger.debug(f"Running ffprobe for URL: {url[:50]}...")
    command = [
        'ffprobe',
        '-user_agent', user_agent,
        '-v', 'error',
        '-show_entries', 'stream=codec_name,width,height,avg_frame_rate',
        '-of', 'json',
        url
    ]
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, text=True)
        if result.stdout:
            data = json.loads(result.stdout)
            streams = data.get('streams', [])
            logger.debug(f"ffprobe returned {len(streams)} streams")
            return streams
        logger.debug("ffprobe returned empty output")
        return []
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ({timeout}s) while fetching stream info for: {url[:50]}...")
        return []
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to decode JSON from ffprobe for {url[:50]}...: {e}")
        return []
    except Exception as e:
        logger.error(f"Stream info check failed for {url[:50]}...: {e}")
        return []

def _check_interlaced_status(url, stream_name, idet_frames, timeout, user_agent='VLC/3.0.14'):
    """
    Checks if a video stream is interlaced using ffmpeg's idet filter.
    Returns 'INTERLACED', 'PROGRESSIVE', or 'UNKNOWN' if detection fails.
    """
    logger.debug(f"Checking interlacing for '{stream_name}' using {idet_frames} frames...")
    idet_command = [
        'ffmpeg', '-user_agent', user_agent,
        '-analyzeduration', '5000000', '-probesize', '5000000',
        '-i', url, '-vf', 'idet', '-frames:v', str(idet_frames), '-an', '-f', 'null', 'NUL' if os.name == 'nt' else '/dev/null'
    ]

    try:
        idet_result = subprocess.run(idet_command, capture_output=True, text=True, timeout=timeout)
        idet_output = idet_result.stderr

        interlaced_frames = 0
        progressive_frames = 0

        for line in idet_output.splitlines():
            if "Single frame detection:" in line or "Multi frame detection:" in line:
                tff_match = re.search(r'TFF:\s*(\d+)', line)
                bff_match = re.search(r'BFF:\s*(\d+)', line)
                progressive_match = re.search(r'Progressive:\s*(\d+)', line)

                if tff_match: interlaced_frames += int(tff_match.group(1))
                if bff_match: interlaced_frames += int(bff_match.group(1))
                if progressive_match: progressive_frames += int(progressive_match.group(1))
        
        if interlaced_frames > progressive_frames:
            status = "INTERLACED"
            logger.debug(f"  → Interlaced detected: {interlaced_frames} interlaced vs {progressive_frames} progressive")
        elif progressive_frames > interlaced_frames:
            status = "PROGRESSIVE"
            logger.debug(f"  → Progressive detected: {progressive_frames} progressive vs {interlaced_frames} interlaced")
        else:
            status = "UNKNOWN"
            logger.debug(f"  → Unknown: {interlaced_frames} interlaced vs {progressive_frames} progressive")
            
        return status

    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ({timeout}s) checking interlacing for {stream_name}")
        return "UNKNOWN (Timeout)"
    except Exception as e:
        logger.error(f"Error checking interlacing for {stream_name}: {e}")
        return "UNKNOWN (Error)"

def _get_bitrate_and_frame_stats(url, ffmpeg_duration, timeout, user_agent='VLC/3.0.14'):
    """Gets bitrate and frame statistics using ffmpeg.
    
    Uses multiple methods to detect bitrate:
    1. Primary: Parse "Statistics:" line with "bytes read" (legacy method)
    2. Fallback 1: Parse progress output lines (e.g., "size=12345kB time=00:00:30.00 bitrate=3333.3kbits/s")
    3. Fallback 2: Calculate from total bytes transferred (any "bytes read" line)
    4. Fallback 3: Extract from any line containing "bitrate=" pattern
    """
    logger.debug(f"Analyzing bitrate and frame stats for {ffmpeg_duration}s...")
    command = [
        'ffmpeg', '-re', '-v', 'debug', '-user_agent', user_agent,
        '-i', url, '-t', str(ffmpeg_duration), '-f', 'null', '-'
    ]
    bitrate = "N/A"
    frames_decoded = "N/A"
    frames_dropped = "N/A"
    elapsed = 0
    status = "OK"

    # Add buffer to timeout to account for ffmpeg startup, network latency, and shutdown overhead
    # Since -re flag reads at real-time, ffmpeg takes at least ffmpeg_duration seconds
    actual_timeout = timeout + ffmpeg_duration + 10

    try:
        start = time.time()
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=actual_timeout, text=True)
        elapsed = time.time() - start
        output = result.stderr
        total_bytes = 0
        progress_bitrate = None  # Track last progress bitrate separately
        
        for line in output.splitlines():
            # Method 1: Primary method - Statistics line with bytes read
            if "Statistics:" in line and "bytes read" in line:
                try:
                    parts = line.split("bytes read")
                    size_str = parts[0].strip().split()[-1]
                    total_bytes = int(size_str)
                    if total_bytes > 0 and ffmpeg_duration > 0:
                        bitrate = (total_bytes * 8) / 1000 / ffmpeg_duration
                        logger.debug(f"  → Calculated bitrate (method 1): {bitrate:.2f} kbps from {total_bytes} bytes")
                except ValueError:
                    pass
            
            # Method 2: Parse progress output (e.g., "size=12345kB time=00:00:30.00 bitrate=3333.3kbits/s")
            # Track latest progress bitrate as fallback, will use last one found
            if "bitrate=" in line and "kbits/s" in line:
                try:
                    bitrate_match = re.search(r'bitrate=\s*(\d+\.?\d*)\s*kbits/s', line)
                    if bitrate_match:
                        # Store progress bitrate, will keep updating with later values
                        progress_bitrate = float(bitrate_match.group(1))
                        logger.debug(f"  → Found progress bitrate (method 2): {progress_bitrate:.2f} kbps")
                except (ValueError, AttributeError):
                    pass
            
            # Method 3: Alternative bytes read pattern (not requiring Statistics:)
            if bitrate == "N/A" and "bytes read" in line and not "Statistics:" in line:
                try:
                    # Look for pattern like "12345 bytes read"
                    bytes_match = re.search(r'(\d+)\s+bytes read', line)
                    if bytes_match:
                        total_bytes = int(bytes_match.group(1))
                        if total_bytes > 0 and ffmpeg_duration > 0:
                            calculated_bitrate = (total_bytes * 8) / 1000 / ffmpeg_duration
                            logger.debug(f"  → Calculated bitrate (method 3): {calculated_bitrate:.2f} kbps from {total_bytes} bytes")
                            bitrate = calculated_bitrate
                except (ValueError, AttributeError):
                    pass
            
            # Parse frame statistics
            if "Input stream #" in line and "frames decoded;" in line:
                decoded_match = re.search(r'(\d+)\s*frames decoded', line)
                errors_match = re.search(r'(\d+)\s*decode errors', line)
                if decoded_match: 
                    frames_decoded = int(decoded_match.group(1))
                    logger.debug(f"  → Frames decoded: {frames_decoded}")
                if errors_match: 
                    frames_dropped = int(errors_match.group(1))
                    logger.debug(f"  → Decode errors: {frames_dropped}")
        
        # Use progress bitrate as final fallback if primary methods didn't find anything
        if bitrate == "N/A" and progress_bitrate is not None:
            bitrate = progress_bitrate
            logger.debug(f"  → Using last progress bitrate as fallback: {bitrate:.2f} kbps")
        
        # Log if bitrate detection failed
        if bitrate == "N/A":
            logger.warning(f"  ⚠ Failed to detect bitrate from ffmpeg output (analyzed for {ffmpeg_duration}s)")
            logger.debug(f"  → Searched {len(output.splitlines())} lines of output")
        
        logger.debug(f"  → Analysis completed in {elapsed:.2f}s")
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ({actual_timeout}s) while fetching bitrate/frames")
        status = "Timeout"
        elapsed = actual_timeout
    except Exception as e:
        logger.error(f"Bitrate/frames check failed: {e}")
        status = "Error"

    return bitrate, frames_decoded, frames_dropped, status, elapsed

def _get_provider_from_url(url):
    """Extracts the hostname and port as a provider identifier."""
    try:
        return urlparse(url).netloc
    except Exception:
        return "unknown_provider"

def _check_stream_for_critical_errors(url, stream_name, timeout, config):
    """
    Runs a specific ffmpeg command to check for critical, provider-side errors.
    Returns a dictionary of identified critical errors.
    """
    logger.debug(f"Checking for critical errors in stream (timeout: {timeout}s)...")
    settings = config['script_settings']
    hwaccel_mode = settings.get('ffmpeg_hwaccel_mode', 'none').lower()

    # Base command arguments
    ffmpeg_command = [
        'ffmpeg',
        '-probesize', '500000', '-analyzeduration', '1000000',
        '-fflags', '+genpts+discardcorrupt', '-flags', 'low_delay',
        '-flush_packets', '1', '-avoid_negative_ts', 'make_zero',
        '-timeout', '5000000', '-rw_timeout', '5000000',
    ]

    # Hardware acceleration specific arguments
    if hwaccel_mode == 'qsv':
        ffmpeg_command.extend([
            '-hwaccel', 'qsv', '-hwaccel_output_format', 'qsv',
        ])
        logger.debug(f"  Using QSV hardware acceleration")

    # Input and common arguments
    ffmpeg_command.extend([
        '-i', url,
        '-t', '20', # 20 second duration for the check
        '-map', '0:v:0', '-map', '0:a:0?', '-map', '0:s?',
    ])

    # Codec and output arguments
    if hwaccel_mode == 'qsv':
        ffmpeg_command.extend([
            '-c:v', 'hevc_qsv',
        ])
    else: # Default to software encoding
        ffmpeg_command.extend([
            '-c:v', 'libx265',
        ])

    ffmpeg_command.extend([
        '-preset', 'veryfast', '-profile:v', 'main', '-g', '50', '-bf', '1',
        '-b:v', '12000k', '-maxrate', '15000k', '-bufsize', '25000k',
        '-c:a', 'libfdk_aac', '-vbr', '4', '-b:a', '128k', '-ac', '2',
        '-af', 'aresample=async=0', '-fps_mode', 'passthrough',
        '-f', 'null', '-'
    ])

    errors = {
        'err_decode': False,
        'err_discontinuity': False,
        'err_timeout': False,
    }

    try:
        start_time = time.time()
        result = subprocess.run(
            ffmpeg_command,
            capture_output=True, # Captures both stdout and stderr
            text=True,
            timeout=timeout
        )
        elapsed = time.time() - start_time
        stderr_output = result.stderr

        if "decode_slice_header error" in stderr_output:
            errors['err_decode'] = True
            logger.debug(f"  ✗ Decode error detected")
        if "timestamp discontinuity" in stderr_output:
            errors['err_discontinuity'] = True
            logger.debug(f"  ✗ Timestamp discontinuity detected")
        if "Connection timed out" in stderr_output:
            errors['err_timeout'] = True
            logger.debug(f"  ✗ Connection timeout detected")
        
        if not any(errors.values()):
            logger.debug(f"  ✓ No critical errors detected (elapsed: {elapsed:.2f}s)")
        else:
            logger.debug(f"  Critical errors found (elapsed: {elapsed:.2f}s): {errors}")

    except subprocess.TimeoutExpired:
        logger.warning(f"  ✗ Timeout ({timeout}s) during critical error check for {stream_name}")
        errors['err_timeout'] = True
    except Exception as e:
        logger.error(f"  ✗ Exception during critical error check for {stream_name}: {e}")
        errors['err_timeout'] = True

    return errors

def _analyze_stream_task(row, ffmpeg_duration, idet_frames, timeout, retries, retry_delay, config, user_agent='VLC/3.0.14'):
    url = row.get('stream_url')
    stream_name = row.get('stream_name', 'Unknown')
    stream_id = row.get('stream_id', 'Unknown')
    if not url:
        logger.warning(f"No URL for stream {stream_name} (ID: {stream_id})")
        return row

    provider = _get_provider_from_url(url)
    with semaphore_lock:
        if provider not in provider_semaphores:
            provider_semaphores[provider] = threading.Semaphore(1)
        provider_semaphore = provider_semaphores[provider]

    with provider_semaphore:
        logger.info(f"▶ Processing stream: {stream_name} (ID: {stream_id}, Provider: {provider})")

        for attempt in range(retries + 1):
            if attempt > 0:
                logger.info(f"  Retry attempt {attempt}/{retries} for {stream_name}")
                
            # Initialize fields for each attempt
            row['timestamp'] = datetime.now().isoformat()
            row['video_codec'] = 'N/A'
            row['audio_codec'] = 'N/A'
            row['resolution'] = '0x0'
            row['fps'] = 0
            row['interlaced_status'] = 'N/A'
            row['bitrate_kbps'] = 0
            row['frames_decoded'] = 'N/A'
            row['frames_dropped'] = 'N/A'
            row['status'] = 'N/A'

            # 1. Get Codec, Resolution, FPS from ffprobe
            logger.info(f"  [1/4] Fetching codec/resolution/FPS info...")
            streams_info = _get_stream_info(url, timeout, user_agent)
            video_info = next((s for s in streams_info if 'width' in s), None)
            audio_info = next((s for s in streams_info if 'codec_name' in s and 'width' not in s), None)

            if video_info:
                row['video_codec'] = video_info.get('codec_name')
                row['resolution'] = f"{video_info.get('width')}x{video_info.get('height')}"
                fps_str = video_info.get('avg_frame_rate', '0/1')
                try:
                    num, den = map(int, fps_str.split('/'))
                    row['fps'] = round(num / den, 2) if den != 0 else 0
                except (ValueError, ZeroDivisionError):
                    row['fps'] = 0
                logger.info(f"    ✓ Video: {row['video_codec']}, {row['resolution']}, {row['fps']} FPS")
            else:
                logger.warning(f"    ✗ No video info found")
            
            if audio_info:
                row['audio_codec'] = audio_info.get('codec_name')
                logger.info(f"    ✓ Audio: {row['audio_codec']}")
            else:
                logger.warning(f"    ✗ No audio info found")

            # 2. Get Bitrate and Frame Drop stats from ffmpeg
            logger.info(f"  [2/4] Analyzing bitrate and frame stats...")
            bitrate, frames_decoded, frames_dropped, status, elapsed = _get_bitrate_and_frame_stats(url, ffmpeg_duration, timeout, user_agent)
            row['bitrate_kbps'] = bitrate
            row['frames_decoded'] = frames_decoded
            row['frames_dropped'] = frames_dropped
            row['status'] = status
            
            if status == "OK":
                logger.info(f"    ✓ Bitrate: {bitrate} kbps, Frames: {frames_decoded} decoded, {frames_dropped} dropped (elapsed: {elapsed:.2f}s)")
            else:
                logger.warning(f"    ✗ Status: {status} (elapsed: {elapsed:.2f}s)")

            # 3. Check for interlacing if stream is OK so far
            if status == "OK":
                logger.info(f"  [3/4] Checking interlaced status...")
                row['interlaced_status'] = _check_interlaced_status(url, stream_name, idet_frames, timeout, user_agent)
                logger.info(f"    ✓ Interlaced status: {row['interlaced_status']}")
            else:
                logger.info(f"  [3/4] Skipping interlace check due to previous errors")
                row['interlaced_status'] = "N/A"

            # 4. Perform critical error check
            logger.info(f"  [4/4] Checking for critical errors...")
            critical_errors = _check_stream_for_critical_errors(url, stream_name, timeout, config)
            row.update(critical_errors)
            error_count = sum(critical_errors.values())
            if error_count > 0:
                logger.warning(f"    ✗ Found {error_count} critical error(s): {critical_errors}")
            else:
                logger.info(f"    ✓ No critical errors detected")

            # If the main status is OK, break the retry loop
            if status == "OK":
                logger.info(f"  ✓ Stream analysis complete for {stream_name}")
                break

            # If not the last attempt, wait before retrying
            if attempt < retries:
                logger.warning(f"  Stream '{stream_name}' failed with status '{status}'. Retrying in {retry_delay} seconds... ({attempt + 1}/{retries})")
                time.sleep(retry_delay)

        # Respect ffmpeg duration to avoid hammering provider
        if isinstance(elapsed, (int, float)) and elapsed < ffmpeg_duration:
            wait_time = ffmpeg_duration - elapsed
            logger.debug(f"  Waiting additional {wait_time:.2f} seconds before next stream from {provider}")
            time.sleep(wait_time)

    return row

def analyze_streams(config, input_csv, output_csv, fails_csv, ffmpeg_duration, idet_frames, timeout, max_workers, retries, retry_delay, user_agent='VLC/3.0.14'):
    """Analyzes streams from a CSV file for various metrics and saves results incrementally."""
    logger.info("="*80)
    logger.info("STARTING STREAM ANALYSIS OPERATION")
    logger.info("="*80)
    
    analysis_start_time = datetime.now()
    
    if not _check_ffmpeg_installed():
        logger.error("ffmpeg/ffprobe not installed. Cannot proceed.")
        sys.exit(1)
    logger.info("✓ ffmpeg and ffprobe are installed")

    settings = config['script_settings']

    # --- Load and Filter Data ---
    logger.info(f"Loading input CSV: {input_csv}")
    try:
        df = pd.read_csv(input_csv)
        logger.info(f"✓ Loaded {len(df)} streams from CSV")
    except FileNotFoundError:
        logger.error(f"Input CSV not found: {input_csv}")
        return

    try:
        start_range = settings.getint('start_channel', 1)
        end_range = settings.getint('end_channel', 999)
        group_ids_str = settings.get('channel_group_ids', 'ALL').strip()
        logger.info(f"Filter settings - Groups: {group_ids_str}, Channel range: {start_range}-{end_range}")
    except ValueError:
        logger.error("Invalid start_channel or end_channel in config.ini. Aborting analyze.")
        return

    if group_ids_str.upper() != 'ALL':
        try:
            target_group_ids = {int(gid.strip()) for gid in group_ids_str.split(',')}
            df['channel_group_id'] = pd.to_numeric(df['channel_group_id'], errors='coerce')
            before_count = len(df)
            df = df[df['channel_group_id'].isin(target_group_ids)]
            logger.info(f"Group filter applied: {before_count} → {len(df)} streams")
        except ValueError:
            logger.error(f"Invalid channel_group_ids in config.ini: '{group_ids_str}'. Aborting analyze.")
            return

    df['channel_number'] = pd.to_numeric(df['channel_number'], errors='coerce')
    df.dropna(subset=['channel_number'], inplace=True)
    before_count = len(df)
    df = df[df['channel_number'].between(start_range, end_range)]
    logger.info(f"Channel range filter applied: {before_count} → {len(df)} streams")

    if df.empty:
        logger.warning(f"No streams found in {input_csv} for the specified filters. Nothing to analyze.")
        return

    # --- Prune Recently Analyzed Streams ---
    try:
        days_to_keep = settings.getint('stream_last_measured_days', 7)
        logger.info(f"Pruning streams analyzed within last {days_to_keep} days...")
    except (ValueError, TypeError):
        days_to_keep = 7
        logger.warning("Invalid or missing stream_last_measured_days in config.ini, defaulting to 7 days.")

    if days_to_keep > 0 and os.path.exists(output_csv):
        try:
            df_processed = pd.read_csv(output_csv)
            df_processed['timestamp'] = pd.to_datetime(df_processed['timestamp'], errors='coerce')
            last_measured_date = datetime.now() - timedelta(days=days_to_keep)
            recent_urls = df_processed[df_processed['timestamp'] > last_measured_date]['stream_url'].unique()
            before_count = len(df)
            df = df[~df['stream_url'].isin(recent_urls)]
            logger.info(f"Pruned recently analyzed streams: {before_count} → {len(df)} streams to analyze")
        except Exception as e:
            logger.warning(f"Could not read or parse existing measurements file '{output_csv}'. Re-analyzing all streams. Error: {e}")

    # --- Duplicate Stream Handling (API removal part) ---
    logger.info("Checking for duplicate stream URLs...")
    duplicates = df[df.duplicated(subset=['stream_url'], keep='first')]
    if not duplicates.empty:
        logger.info(f"Found {len(duplicates)} duplicate streams to remove from Dispatcharr")
        channels_with_duplicates = duplicates.groupby('channel_id')['stream_id'].apply(list).to_dict()
        for channel_id, stream_ids_to_remove in channels_with_duplicates.items():
            try:
                current_streams_data = fetch_channel_streams(channel_id)
                if current_streams_data:
                    current_stream_ids = [s['id'] for s in current_streams_data]
                    updated_stream_ids = [sid for sid in current_stream_ids if sid not in stream_ids_to_remove]
                    if len(updated_stream_ids) < len(current_stream_ids):
                        logger.info(f"Updating channel {channel_id} to remove {len(current_stream_ids) - len(updated_stream_ids)} duplicate streams.")
                        update_channel_streams(channel_id, updated_stream_ids)
            except Exception as e:
                logger.error(f"Error removing duplicate streams for channel {channel_id}: {e}")
    else:
        logger.info("✓ No duplicate streams found")

    # --- Prepare Final List for Analysis ---
    df.drop_duplicates(subset=['stream_url'], keep='first', inplace=True)

    if df.empty:
        logger.info("All filtered streams have been analyzed recently. Nothing to do.")
        return

    streams_to_analyze = df.to_dict('records')
    logger.info(f"FINAL: {len(streams_to_analyze)} streams to analyze")

    # Calculate estimated time
    estimated_time_per_stream = ffmpeg_duration + 30  # ffmpeg duration + overhead
    estimated_total_seconds = estimated_time_per_stream * len(streams_to_analyze)
    estimated_hours = estimated_total_seconds / 3600
    logger.info(f"ESTIMATED TIME: ~{estimated_hours:.1f} hours ({estimated_time_per_stream}s per stream)")

    # --- Execute Analysis and Write Incrementally ---
    final_columns = [
        'channel_number', 'channel_id', 'stream_id', 'stream_name', 'stream_url',
        'channel_group_id', 'timestamp', 'video_codec', 'audio_codec', 'interlaced_status',
        'status', 'bitrate_kbps', 'fps', 'resolution', 'frames_decoded', 'frames_dropped',
        'err_decode', 'err_discontinuity', 'err_timeout'
    ]
    
    # Ensure the output directory exists
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(fails_csv).parent.mkdir(parents=True, exist_ok=True)

    # Check if files exist to determine if we need to write headers
    output_exists = os.path.exists(output_csv)
    fails_exists = os.path.exists(fails_csv)
    
    # Initialize progress tracker
    progress_tracker = StreamCheckProgress()
    total_streams = len(streams_to_analyze)
    completed_streams = 0

    logger.info("="*80)
    logger.info(f"ANALYSIS PARAMETERS:")
    logger.info(f"  - Duration: {ffmpeg_duration}s per stream")
    logger.info(f"  - Timeout: {timeout}s per operation")
    logger.info(f"  - idet frames: {idet_frames}")
    logger.info(f"  - Retries: {retries}")
    logger.info(f"  - Retry delay: {retry_delay}s")
    logger.info(f"  - Output: {output_csv}")
    logger.info(f"  - Fails output: {fails_csv}")
    logger.info("="*80)

    try:
        with open(output_csv, 'a', newline='', encoding='utf-8') as f_out, \
             open(fails_csv, 'a', newline='', encoding='utf-8') as f_fails:

            writer_out = csv.DictWriter(f_out, fieldnames=final_columns, extrasaction='ignore', lineterminator='\n')
            writer_fails = csv.DictWriter(f_fails, fieldnames=final_columns, extrasaction='ignore', lineterminator='\n')

            if not output_exists or os.path.getsize(output_csv) == 0:
                writer_out.writeheader()
                logger.info("✓ Created output CSV with headers")
            if not fails_exists or os.path.getsize(fails_csv) == 0:
                writer_fails.writeheader()
                logger.info("✓ Created fails CSV with headers")

            # Initialize progress
            progress_tracker.update(0, total_streams, 'Starting...')
            logger.info(f"Starting synchronous analysis of {total_streams} streams...")
            logger.info("="*80)

            # Process streams synchronously (one at a time)
            for idx, row in enumerate(streams_to_analyze, 1):
                stream_start_time = datetime.now()
                
                try:
                    stream_name = row.get('stream_name', 'Unknown')
                    logger.info(f"\n[{idx}/{total_streams}] ═══ Starting analysis of: {stream_name} ═══")
                    
                    result_row = _analyze_stream_task(row, ffmpeg_duration, idet_frames, timeout, retries, retry_delay, config, user_agent)
                    completed_streams += 1
                    
                    stream_elapsed = (datetime.now() - stream_start_time).total_seconds()
                    
                    # Update progress
                    progress_tracker.update(completed_streams, total_streams, stream_name)
                    
                    # Log progress in terminal with time estimates
                    percentage = round((completed_streams / total_streams * 100), 1)
                    status = result_row.get('status', 'Unknown')
                    
                    # Calculate ETA
                    elapsed_total = (datetime.now() - analysis_start_time).total_seconds()
                    avg_time_per_stream = elapsed_total / completed_streams
                    remaining_streams = total_streams - completed_streams
                    eta_seconds = avg_time_per_stream * remaining_streams
                    eta_hours = eta_seconds / 3600
                    
                    logger.info(f"[{idx}/{total_streams}] Progress: {percentage}% - {stream_name} → Status: {status}")
                    logger.info(f"  Time: {stream_elapsed:.1f}s this stream, ETA: {eta_hours:.1f}h remaining")
                    
                    # Write to the main measurements file
                    writer_out.writerow(result_row)
                    f_out.flush()  # Flush buffer to disk
                    
                    # If the stream failed, write to the fails file
                    if result_row.get('status') != 'OK':
                        writer_fails.writerow(result_row)
                        f_fails.flush() # Flush buffer to disk
                        logger.warning(f"  ⚠ Stream failed and saved to fails CSV")
                    
                    logger.info(f"[{idx}/{total_streams}] ═══ Completed: {stream_name} ═══\n")
                        
                except KeyboardInterrupt:
                    logger.warning("\n\n⚠️  INTERRUPTED BY USER - Saving progress...")
                    progress_tracker.clear()
                    logger.warning(f"Analysis interrupted at {completed_streams}/{total_streams} streams")
                    logger.warning("Partial results have been saved. You can resume by running the command again.")
                    return
                    
                except Exception as exc:
                    completed_streams += 1
                    stream_name = row.get('stream_name', 'Unknown')
                    
                    stream_elapsed = (datetime.now() - stream_start_time).total_seconds()
                    
                    # Update progress
                    progress_tracker.update(completed_streams, total_streams, stream_name)
                    
                    # Log progress in terminal
                    percentage = round((completed_streams / total_streams * 100), 1)
                    logger.error(f'[{idx}/{total_streams}] Progress: {percentage}% - Stream {stream_name} generated an exception: {exc}')
                    logger.error(f'  Exception occurred after {stream_elapsed:.1f}s')
                    
                    # Update row with error info and write to both files
                    row.update({'timestamp': datetime.now().isoformat(), 'status': "Exception"})
                    default_errors = {'err_decode': False, 'err_discontinuity': False, 'err_timeout': True}
                    row.update(default_errors)
                    
                    writer_out.writerow(row)
                    writer_fails.writerow(row)
                    f_out.flush()
                    f_fails.flush()
            
            # Clear progress when complete
            progress_tracker.clear()

        total_elapsed = (datetime.now() - analysis_start_time).total_seconds()
        total_hours = total_elapsed / 3600
        
        logger.info("="*80)
        logger.info(f"✓ Incremental analysis complete. Results saved to {output_csv} and {fails_csv}")
        logger.info(f"  Total time: {total_hours:.2f} hours ({total_elapsed:.0f} seconds)")
        logger.info(f"  Average time per stream: {total_elapsed/completed_streams:.1f}s")
        logger.info("="*80)

        # --- Final Cleanup: Deduplicate the results file ---
        logger.info(f"Deduplicating final results in {output_csv}...")
        df_final = pd.read_csv(output_csv)
        
        # Ensure consistent data types before dropping duplicates
        df_final['stream_id'] = pd.to_numeric(df_final['stream_id'], errors='coerce')
        df_final.dropna(subset=['stream_id'], inplace=True)
        df_final['stream_id'] = df_final['stream_id'].astype(int)
        
        # Keep the latest entry for each stream_id
        df_final.sort_values(by='timestamp', ascending=True, inplace=True)
        before_dedup = len(df_final)
        df_final.drop_duplicates(subset=['stream_id'], keep='last', inplace=True)
        logger.info(f"Deduplication: {before_dedup} → {len(df_final)} entries")
        
        # Reorder columns to the desired final order
        df_final = df_final.reindex(columns=final_columns)

        df_final.to_csv(output_csv, index=False, na_rep='N/A')
        logger.info(f"✓ Successfully deduplicated and saved final results to {output_csv}")
        
        logger.info("="*80)
        logger.info("STREAM ANALYSIS COMPLETE")
        logger.info("="*80)

    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  INTERRUPTED BY USER")
        progress_tracker.clear()
        logger.warning("Analysis interrupted. Partial results have been saved.")
        raise
    except Exception as e:
        logger.error(f"An error occurred during incremental writing or final deduplication: {e}")
        import traceback
        logger.error(traceback.format_exc())
        progress_tracker.clear()

# --- Scoring and Sorting ---

def score_streams(config, input_csv, output_csv, update_stats=False):
    """Calculates averages, scores, and sorts streams based on config."""
    logger.info("="*80)
    logger.info("STARTING SCORE STREAMS OPERATION")
    logger.info("="*80)
    
    settings = config['script_settings']

    # Use a DataFrame for easier manipulation
    logger.info(f"Loading input CSV: {input_csv}")
    try:
        df = pd.read_csv(input_csv)
        logger.info(f"✓ Loaded {len(df)} stream measurements")
    except FileNotFoundError:
        logger.error(f"Input CSV not found: {input_csv}")
        return
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return

    # --- Filtering based on config.ini ---
    try:
        start_range = settings.getint('start_channel', 1)
        end_range = settings.getint('end_channel', 999)
        group_ids_str = settings.get('channel_group_ids', 'ALL').strip()
        logger.info(f"Filter settings - Groups: {group_ids_str}, Channel range: {start_range}-{end_range}")
    except ValueError:
        logger.error("Invalid start_channel or end_channel in config.ini. Aborting score.")
        return

    # Filter by group first if specified
    if group_ids_str.upper() != 'ALL':
        try:
            target_group_ids = {int(gid.strip()) for gid in group_ids_str.split(',')}
            df['channel_group_id'] = pd.to_numeric(df['channel_group_id'], errors='coerce')
            before_count = len(df)
            df = df[df['channel_group_id'].isin(target_group_ids)]
            logger.info(f"Group filter applied: {before_count} → {len(df)} streams")
        except ValueError:
            logger.error(f"Invalid channel_group_ids in config.ini: '{group_ids_str}'. Aborting score.")
            return

    # Then filter by channel number range
    df['channel_number'] = pd.to_numeric(df['channel_number'], errors='coerce')
    df.dropna(subset=['channel_number'], inplace=True)
    before_count = len(df)
    df = df[df['channel_number'].between(start_range, end_range)]
    logger.info(f"Channel range filter applied: {before_count} → {len(df)} streams")

    if df.empty:
        logger.warning(f"No streams found in {input_csv} for the specified filters. Nothing to score.")
        return
    # --- End Filtering ---

    # Convert types, handling potential errors
    logger.info("Converting data types for scoring...")
    df['bitrate_kbps'] = pd.to_numeric(df['bitrate_kbps'], errors='coerce')
    df['frames_decoded'] = pd.to_numeric(df['frames_decoded'], errors='coerce')
    df['frames_dropped'] = pd.to_numeric(df['frames_dropped'], errors='coerce')

    # Group by stream_id and calculate averages
    logger.info("Calculating averages per stream...")
    summary = df.groupby('stream_id').agg(
        avg_bitrate_kbps=('bitrate_kbps', 'mean'),
        avg_frames_decoded=('frames_decoded', 'mean'),
        avg_frames_dropped=('frames_dropped', 'mean')
    ).reset_index()
    logger.info(f"✓ Calculated averages for {len(summary)} unique streams")

    # Merge with the latest metadata for each stream
    logger.info("Merging with latest metadata...")
    latest_meta = df.drop_duplicates(subset='stream_id', keep='last')
    summary = pd.merge(summary, latest_meta.drop(columns=['bitrate_kbps', 'frames_decoded', 'frames_dropped']), on='stream_id')

    # Calculate dropped frame percentage
    logger.info("Calculating dropped frame percentages...")
    summary['dropped_frame_percentage'] = (summary['avg_frames_dropped'] / summary['avg_frames_decoded'] * 100).fillna(0)

    # Score and Sort
    logger.info("Calculating scores based on resolution, FPS, bitrate, and errors...")
    RESOLUTION_SCORES = {
        '3840x2160': 100, '1920x1080': 80, '1280x720': 50,
        '960x540': 20, 'Unknown': 0, '': 0
    }
    summary['resolution_score'] = summary['resolution'].astype(str).str.strip().map(RESOLUTION_SCORES).fillna(0)
    logger.info(f"  Resolution scoring applied")
    
    fps_bonus_points = settings.getint("fps_bonus_points", 55)
    summary['fps_bonus'] = 0
    summary.loc[pd.to_numeric(summary['fps'], errors='coerce').fillna(0) >= 50, 'fps_bonus'] = fps_bonus_points
    high_fps_count = (summary['fps_bonus'] == fps_bonus_points).sum()
    logger.info(f"  FPS bonus ({fps_bonus_points} pts) applied to {high_fps_count} streams with FPS >= 50")
    
    summary['max_bitrate_for_channel'] = summary.groupby('channel_id')['avg_bitrate_kbps'].transform('max')
    summary['bitrate_score'] = (summary['avg_bitrate_kbps'] / (summary['max_bitrate_for_channel'] * 0.01)).fillna(0)
    logger.info(f"  Bitrate scoring applied (relative to channel max)")
    
    summary['dropped_frames_penalty'] = summary['dropped_frame_percentage'] * 1
    logger.info(f"  Dropped frames penalty calculated")

    # Calculate penalty for critical errors
    error_columns = ['err_decode', 'err_discontinuity', 'err_timeout']
    for col in error_columns:
        summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)
    summary['error_penalty'] = summary[error_columns].sum(axis=1) * 25
    streams_with_errors = (summary['error_penalty'] > 0).sum()
    logger.info(f"  Error penalties applied (25 pts each, {streams_with_errors} streams affected)")

    summary['score'] = (
        summary['bitrate_score'] +
        summary['resolution_score'] +
        summary['fps_bonus'] -
        summary['dropped_frames_penalty'] -
        summary['error_penalty']
    )
    summary.loc[summary['avg_bitrate_kbps'].isna(), 'score'] = -1
    
    logger.info("Sorting streams by channel and score...")
    df_sorted = summary.sort_values(by=['channel_number', 'score'], ascending=[True, False])
    
    # Ensure all columns are present for the final CSV
    final_columns = [
        'stream_id', 'channel_number', 'channel_id', 'channel_group_id', 'stream_name', 'stream_url',
        'avg_bitrate_kbps', 'avg_frames_decoded', 'avg_frames_dropped', 'dropped_frame_percentage',
        'fps', 'resolution', 'video_codec', 'audio_codec', 'interlaced_status', 'status', 'score', 'error_penalty'
    ]
    for col in final_columns:
        if col not in df_sorted.columns:
            df_sorted[col] = 'N/A' # Add missing columns with default value

    df_sorted = df_sorted[final_columns] # Ensure correct order
    df_sorted.to_csv(output_csv, index=False, na_rep='N/A')
    
    logger.info("="*80)
    logger.info(f"✓ Scored and sorted CSV saved as {output_csv}")
    logger.info(f"  Total streams scored: {len(df_sorted)}")
    logger.info(f"  Channels affected: {df_sorted['channel_number'].nunique()}")
    
    if update_stats:
        logger.info("Updating stream stats on server...")
        update_stream_stats(output_csv)
    
    logger.info("SCORE STREAMS COMPLETE")
    logger.info("="*80)


def update_stream_stats(csv_path):
    """Updates stream stats on the server from a CSV file."""
    base_url = _get_base_url()
    if not base_url:
        logger.error("DISPATCHARR_BASE_URL not set in .env file.")
        return

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        logger.error(f"CSV file not found at: {csv_path}")
        return

    for _, row in df.iterrows():
        stream_id = row.get("stream_id")
        if not stream_id:
            continue

        # Construct the stream stats payload from the CSV row
        stream_stats_payload = {
            "resolution": row.get("resolution"),
            "source_fps": row.get("fps"),
            "video_codec": row.get("video_codec"),
            "audio_codec": row.get("audio_codec"),
            "ffmpeg_output_bitrate": int(row.get("avg_bitrate_kbps")) if pd.notna(row.get("avg_bitrate_kbps")) else None,
        }

        # Clean up the payload, removing any None values
        stream_stats_payload = {k: v for k, v in stream_stats_payload.items() if pd.notna(v)}

        if not stream_stats_payload:
            logger.info(f"No data to update for stream {stream_id}. Skipping.")
            continue

        # Construct the URL for the specific stream
        stream_url = f"{base_url}/api/channels/streams/{int(stream_id)}/"

        try:
            # Fetch the existing stream data to get the current stream_stats
            existing_stream_data = fetch_data_from_url(stream_url)
            if not existing_stream_data:
                logger.warning(
                    f"Could not fetch existing data for stream {stream_id}. Skipping."
                )
                continue

            # Get the existing stream_stats or an empty dict
            existing_stats = existing_stream_data.get("stream_stats") or {}
            if isinstance(existing_stats, str):
                try:
                    existing_stats = json.loads(existing_stats)
                except json.JSONDecodeError:
                    existing_stats = {}

            # Merge the existing stats with the new payload
            updated_stats = {**existing_stats, **stream_stats_payload}

            # Send the PATCH request with the updated stream_stats
            patch_payload = {"stream_stats": updated_stats}
            logger.info(f"Updating stream {stream_id} with: {patch_payload}")
            patch_request(stream_url, patch_payload)

        except Exception as e:
            logger.error(f"An error occurred while updating stream {stream_id}: {e}")


# --- Reordering Streams ---

def reorder_streams(config, input_csv):
    """Reorders streams in Dispatcharr based on the scored and sorted CSV."""
    logger.info("="*80)
    logger.info("STARTING REORDER STREAMS OPERATION")
    logger.info(f"Input CSV: {input_csv}")
    logger.info("="*80)
    
    settings = config['script_settings']
    try:
        start_range = settings.getint('start_channel', 1)
        end_range = settings.getint('end_channel', 999)
        group_ids_str = settings.get('channel_group_ids', 'ALL').strip()
        logger.info(f"Filter settings - Groups: {group_ids_str}, Channel range: {start_range}-{end_range}")
    except ValueError:
        logger.error("Invalid start_channel or end_channel in config.ini. Aborting reorder.")
        return

    logger.info(f"Loading scored CSV: {input_csv}")
    try:
        df = pd.read_csv(input_csv)
        logger.info(f"✓ Loaded {len(df)} scored streams")
    except FileNotFoundError:
        logger.error(f"Error: {input_csv} not found. Please run the 'score' command first.")
        return

    # Filter by group first if specified
    if group_ids_str.upper() != 'ALL':
        try:
            target_group_ids = {int(gid.strip()) for gid in group_ids_str.split(',')}
            df['channel_group_id'] = pd.to_numeric(df['channel_group_id'], errors='coerce')
            before_count = len(df)
            df = df[df['channel_group_id'].isin(target_group_ids)]
            logger.info(f"Group filter applied: {before_count} → {len(df)} streams")
        except ValueError:
            logger.error(f"Invalid channel_group_ids in config.ini: '{group_ids_str}'. Aborting reorder.")
            return

    # Then filter by channel number range
    df['channel_number'] = pd.to_numeric(df['channel_number'], errors='coerce')
    df.dropna(subset=['channel_number'], inplace=True)
    before_count = len(df)
    df = df[df['channel_number'].between(start_range, end_range)]
    logger.info(f"Channel range filter applied: {before_count} → {len(df)} streams")

    if df.empty:
        logger.warning(f"No streams found in {input_csv} for the specified filters. Nothing to reorder.")
        return

    df['stream_id'] = pd.to_numeric(df['stream_id'], errors='coerce')
    df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
    df.dropna(subset=['stream_id', 'channel_id'], inplace=True)
    df['stream_id'] = df['stream_id'].astype(int)
    df['channel_id'] = df['channel_id'].astype(int)

    grouped = df.groupby("channel_id")
    logger.info(f"Reordering streams for {len(grouped)} channels...")
    logger.info("="*80)

    success_count = 0
    skip_count = 0
    error_count = 0

    for idx, (channel_id, group) in enumerate(grouped, 1):
        sorted_stream_ids_from_csv = group["stream_id"].tolist()
        channel_number = group["channel_number"].iloc[0]
        
        logger.info(f"[{idx}/{len(grouped)}] Processing channel {channel_number} (ID: {channel_id})...")
        logger.info(f"  CSV has {len(sorted_stream_ids_from_csv)} sorted streams")
        
        current_streams_from_api = fetch_channel_streams(channel_id)
        if current_streams_from_api is None:
            logger.warning(f"  ✗ Could not fetch current streams for channel ID {channel_id}. Skipping reorder.")
            skip_count += 1
            continue

        logger.info(f"  API has {len(current_streams_from_api)} current streams")
        
        current_stream_ids_set = {s['id'] for s in current_streams_from_api}
        validated_sorted_ids = [sid for sid in sorted_stream_ids_from_csv if sid in current_stream_ids_set]
        csv_ids_set = set(sorted_stream_ids_from_csv)
        new_unscored_ids = [sid for sid in current_stream_ids_set if sid not in csv_ids_set]
        final_stream_id_list = validated_sorted_ids + new_unscored_ids
        
        if not final_stream_id_list:
            logger.warning(f"  ✗ No valid streams to reorder for channel ID {channel_id}. Skipping.")
            skip_count += 1
            continue
        
        logger.info(f"  Final order: {len(validated_sorted_ids)} scored + {len(new_unscored_ids)} unscored = {len(final_stream_id_list)} total")
        
        try:
            update_channel_streams(channel_id, final_stream_id_list)
            logger.info(f"  ✓ Successfully reordered streams for channel {channel_number} (ID: {channel_id})")
            success_count += 1
        except Exception as e:
            logger.error(f"  ✗ Exception while reordering streams for channel ID {channel_id}: {e}")
            error_count += 1

    logger.info("="*80)
    logger.info(f"REORDER COMPLETE")
    logger.info(f"  Success: {success_count} channels")
    logger.info(f"  Skipped: {skip_count} channels")
    logger.info(f"  Errors: {error_count} channels")
    logger.info("="*80)

def retry_failed_streams(config, input_csv, fails_csv, ffmpeg_duration, idet_frames, timeout, max_workers, user_agent='VLC/3.0.14'):
    """Retries analysis for streams that previously failed."""
    if not os.path.exists(input_csv):
        logger.error(f"Input file not found: {input_csv}. Cannot retry failed streams.")
        return

    if not _check_ffmpeg_installed():
        sys.exit(1)

    all_rows = []
    fieldnames = []
    with open(input_csv, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames or []
        all_rows = list(reader)

    required_cols = ['video_codec', 'audio_codec', 'interlaced_status', 'status']
    for col in required_cols:
        if col not in fieldnames:
            fieldnames.append(col)

    failed_streams = [row for row in all_rows if row.get('status') != 'OK']

    if not failed_streams:
        logger.info("No failed streams to retry.")
        return

    logger.info(f"Retrying analysis for {len(failed_streams)} failed streams...")

    updated_rows = {row['stream_id']: row for row in all_rows}
    
    # Initialize progress tracker
    progress_tracker = StreamCheckProgress()
    total_streams = len(failed_streams)
    completed_streams = 0
    progress_tracker.update(0, total_streams, 'Starting retry...')

    # Process streams synchronously (one at a time)
    for row in failed_streams:
        try:
            result_row = _analyze_stream_task(row, ffmpeg_duration, idet_frames, timeout, 0, 0, config, user_agent)
            completed_streams += 1
            stream_id = result_row.get('stream_id')
            stream_name = result_row.get('stream_name', 'Unknown')
            
            # Update progress
            progress_tracker.update(completed_streams, total_streams, stream_name)
            percentage = round((completed_streams / total_streams * 100), 1)
            logger.info(f"Retry Progress: {completed_streams}/{total_streams} ({percentage}%) - {stream_name}")
            
            if stream_id:
                updated_rows[stream_id] = result_row
        except Exception as exc:
            completed_streams += 1
            stream_name = row.get('stream_name', 'Unknown')
            
            # Update progress
            progress_tracker.update(completed_streams, total_streams, stream_name)
            percentage = round((completed_streams / total_streams * 100), 1)
            logger.error(f'Retry Progress: {completed_streams}/{total_streams} ({percentage}%) - Stream {stream_name} generated an exception during retry: {exc}')
            
            row.update({'timestamp': datetime.now().isoformat(), 'status': "Retry Exception"})
            updated_rows[row['stream_id']] = row
    
    # Clear progress when complete
    progress_tracker.clear()

    with open(input_csv, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows.values())

    new_fails = [row for row in updated_rows.values() if row.get('status') != 'OK']
    with open(fails_csv, 'w', newline='', encoding='utf-8') as fails_outfile:
        writer = csv.DictWriter(fails_outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(new_fails)

    logger.info(f"Retry complete. Updated {input_csv} and {fails_csv}.")

def main():
    """Main function to parse arguments and call the appropriate function."""
    load_dotenv()
    config = load_config()

    parser = argparse.ArgumentParser(
        description="A tool for managing and analyzing Dispatcharr IPTV streams.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    subparsers.add_parser('login', help='Authenticate with Dispatcharr and save the token.')
    
    fetch_parser = subparsers.add_parser('fetch', help='Fetch channel and stream information.')
    fetch_parser.add_argument('--output', type=str, default='csv/02_grouped_channel_streams.csv')
    fetch_parser.add_argument('--channel-ids', type=str, help='Comma-separated list of channel IDs to fetch (overrides config filters)')

    analyze_parser = subparsers.add_parser('analyze', help='Analyze IPTV streams.')
    analyze_parser.add_argument('--input', type=str, default='csv/02_grouped_channel_streams.csv')
    analyze_parser.add_argument('--output', type=str, default='csv/03_iptv_stream_measurements.csv')
    analyze_parser.add_argument('--fails_output', type=str, default='csv/04_fails.csv')
    analyze_parser.add_argument('--duration', type=int, default=10, help='Duration in seconds for ffmpeg to analyze stream.')
    analyze_parser.add_argument('--idet-frames', type=int, default=500)
    analyze_parser.add_argument('--timeout', type=int, default=30)
    analyze_parser.add_argument('--workers', type=int, default=8)
    analyze_parser.add_argument('--retries', type=int, default=1)
    analyze_parser.add_argument('--retry-delay', type=int, default=10)

    score_parser = subparsers.add_parser('score', help='Score and sort streams.')
    score_parser.add_argument('--input', type=str, default='csv/03_iptv_stream_measurements.csv')
    score_parser.add_argument('--output', type=str, default='csv/05_iptv_streams_scored_sorted.csv')
    score_parser.add_argument('--update-stats', action='store_true', help='Update stream stats on the server after scoring.')

    reorder_parser = subparsers.add_parser('reorder', help='Reorder streams in Dispatcharr.')
    reorder_parser.add_argument('--input', type=str, default='csv/05_iptv_streams_scored_sorted.csv')

    retry_parser = subparsers.add_parser('retry', help='Retry analysis for failed streams.')
    retry_parser.add_argument('--input', type=str, default='csv/03_iptv_stream_measurements.csv')
    retry_parser.add_argument('--fails-output', type=str, default='csv/04_fails.csv')
    retry_parser.add_argument('--duration', type=int, default=20)
    retry_parser.add_argument('--idet-frames', type=int, default=500)
    retry_parser.add_argument('--timeout', type=int, default=30)
    retry_parser.add_argument('--workers', type=int, default=8)

    # Automation commands
    automation_parser = subparsers.add_parser('automation', help='Automated stream management commands.')
    automation_subparsers = automation_parser.add_subparsers(dest='automation_command', help='Automation commands')
    
    automation_subparsers.add_parser('start', help='Start continuous automated stream management.')
    automation_subparsers.add_parser('stop', help='Stop automated stream management.')
    automation_subparsers.add_parser('status', help='Show automation status and recent activity.')
    automation_subparsers.add_parser('cycle', help='Run one automation cycle manually.')
    
    refresh_parser = subparsers.add_parser('refresh-playlist', help='Manually refresh M3U playlists.')
    refresh_parser.add_argument('--account-id', type=int, help='Refresh specific M3U account (if not provided, refreshes all)')
    
    discover_parser = subparsers.add_parser('discover-streams', help='Discover and assign new streams to channels based on regex patterns.')

    args = parser.parse_args()

    if args.command == 'login':
        login()
    elif args.command == 'fetch':
        channel_ids = None
        if args.channel_ids:
            channel_ids = [int(cid.strip()) for cid in args.channel_ids.split(',')]
        fetch_streams(config, args.output, channel_ids)
    elif args.command == 'analyze':
        analyze_streams(config, args.input, args.output, args.fails_output, args.duration, args.idet_frames, args.timeout, args.workers, args.retries, args.retry_delay)
    elif args.command == 'score':
        score_streams(config, args.input, args.output, args.update_stats)
    elif args.command == 'reorder':
        reorder_streams(config, args.input)
    elif args.command == 'retry':
        retry_failed_streams(config, args.input, args.fails_output, args.duration, args.idet_frames, args.timeout, args.workers)
    elif args.command == 'refresh-playlist':
        try:
            refresh_m3u_playlists(args.account_id)
            logger.info("M3U playlist refresh completed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh playlists: {e}")
    elif args.command == 'discover-streams':
        from automated_stream_manager import AutomatedStreamManager
        manager = AutomatedStreamManager()
        assignments = manager.discover_and_assign_streams()
        if assignments:
            logger.info(f"Stream discovery completed. Assignments: {assignments}")
        else:
            logger.info("No new streams were assigned")
    elif args.command == 'automation':
        from automated_stream_manager import AutomatedStreamManager
        manager = AutomatedStreamManager()
        
        if args.automation_command == 'start':
            manager.start_automation()
            logger.info("Automation started. Press Ctrl+C to stop.")
            try:
                while manager.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                manager.stop_automation()
                logger.info("Automation stopped by user")
        elif args.automation_command == 'stop':
            manager.stop_automation()
        elif args.automation_command == 'status':
            status = manager.get_status()
            print(json.dumps(status, indent=2, default=str))
        elif args.automation_command == 'cycle':
            manager.run_automation_cycle()
        else:
            logger.error("Unknown automation command")
    else:
        logger.warning("=" * 80)
        logger.warning("⚠️  DEPRECATION WARNING: Manual pipeline execution is deprecated!")
        logger.warning("⚠️  The old manual approach (running without specific commands) is no longer recommended.")
        logger.warning("⚠️  Please migrate to the new automated stream management system:")
        logger.warning("⚠️  ")
        logger.warning("⚠️  🔧 For automated management:")
        logger.warning("⚠️      python3 web_api.py")
        logger.warning("⚠️      Access web interface at http://localhost:5000")
        logger.warning("⚠️  ")
        logger.warning("⚠️  📚 For manual operations use specific commands:")
        logger.warning("⚠️      python3 dispatcharr-stream-sorter.py automation start")
        logger.warning("⚠️      python3 dispatcharr-stream-sorter.py refresh-playlist")
        logger.warning("⚠️      python3 dispatcharr-stream-sorter.py discover-streams")
        logger.warning("⚠️  ")
        logger.warning("⚠️  🐳 For Docker deployment:")
        logger.warning("⚠️      docker-compose up -d")
        logger.warning("⚠️  ")
        logger.warning("=" * 80)
        logger.info("Running legacy default pipeline (this will be removed in future versions)")
        logger.info("Pipeline: fetch -> analyze -> score -> reorder")
        
        fetch_streams(config, 'csv/02_grouped_channel_streams.csv')
        analyze_streams(config, 'csv/02_grouped_channel_streams.csv', 'csv/03_iptv_stream_measurements.csv', 'csv/04_fails.csv', 20, 500, 30, 8, 1, 10)
        score_streams(config, 'csv/03_iptv_stream_measurements.csv', 'csv/05_iptv_streams_scored_sorted.csv', update_stats=True)
        reorder_streams(config, 'csv/05_iptv_streams_scored_sorted.csv')
        
        logger.warning("=" * 80)
        logger.warning("⚠️  Legacy pipeline completed. Please migrate to the automated system!")
        logger.warning("=" * 80)

if __name__ == "__main__":
    print(f"Running script at {datetime.now()}")
    main()
