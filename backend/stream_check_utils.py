#!/usr/bin/env python3
"""
Stream Quality Checking Utility for StreamFlow.

This module provides focused stream checking functionality using ffmpeg/ffprobe
to analyze IPTV streams. It extracts essential quality metrics:
- Resolution (width x height)
- Bitrate (kbps)
- FPS (frames per second)
- Audio codec
- Video codec

The module is designed to work with the UDI (Universal Data Index) storage
system and provides a clean, maintainable API for stream quality analysis.
"""

import json
import re
import subprocess
import time
from datetime import datetime
from typing import Dict, Optional, Tuple, Any

from logging_config import setup_logging

logger = setup_logging(__name__)

# Constants for error detection and logging
EARLY_EXIT_THRESHOLD = 0.8  # Consider ffmpeg exited early if elapsed < 80% of expected duration
MAX_ERROR_LINES_TO_LOG = 5  # Maximum number of error lines to log from ffmpeg output
MAX_DEBUG_LINES_TO_LOG = 10  # Maximum number of debug lines to log from ffmpeg output


def _sanitize_codec_name(codec: str) -> str:
    """
    Sanitize codec name to filter out invalid or placeholder codec names.
    
    ffmpeg sometimes outputs codec names like 'wrapped_avframe' when it cannot
    properly identify the codec (e.g., for hardware-accelerated streams). This
    function filters out such values and returns 'N/A' for invalid codecs.
    
    Args:
        codec: The raw codec name extracted from ffmpeg output
        
    Returns:
        Sanitized codec name, or 'N/A' if invalid
    """
    if not codec:
        return 'N/A'
    
    # List of invalid/placeholder codec names to filter out
    invalid_codecs = {
        'wrapped_avframe',  # Hardware acceleration placeholder
        'none',             # No codec
        'unknown',          # Unknown codec
        'null',             # Null codec
    }
    
    codec_lower = codec.lower()
    if codec_lower in invalid_codecs:
        logger.debug(f"  → Filtered out invalid codec: {codec}")
        return 'N/A'
    
    return codec


def check_ffmpeg_installed() -> bool:
    """
    Check if ffmpeg and ffprobe are installed and available.

    Returns:
        bool: True if both tools are available, False otherwise
    """
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


def get_stream_info(url: str, timeout: int = 30, user_agent: str = 'VLC/3.0.14') -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    DEPRECATED: Use get_stream_info_and_bitrate() instead for better performance.
    
    Get stream information using ffprobe to extract codec, resolution, and FPS.

    Args:
        url: Stream URL to analyze
        timeout: Timeout in seconds for the ffprobe operation
        user_agent: User agent string to use for HTTP requests

    Returns:
        Tuple of (video_info, audio_info) dictionaries, or (None, None) on error
        video_info contains: codec_name, width, height, avg_frame_rate
        audio_info contains: codec_name
    """
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
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            text=True
        )

        if result.stdout:
            data = json.loads(result.stdout)
            streams = data.get('streams', [])
            logger.debug(f"ffprobe returned {len(streams)} streams")

            # Extract video and audio stream info
            video_info = next((s for s in streams if 'width' in s), None)
            audio_info = next((s for s in streams if 'codec_name' in s and 'width' not in s), None)

            return video_info, audio_info

        logger.debug("ffprobe returned empty output")
        return None, None

    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ({timeout}s) while fetching stream info for: {url[:50]}...")
        return None, None
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to decode JSON from ffprobe for {url[:50]}...: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Stream info check failed for {url[:50]}...: {e}")
        return None, None


def get_stream_info_and_bitrate(url: str, duration: int = 30, timeout: int = 30, user_agent: str = 'VLC/3.0.14') -> Dict[str, Any]:
    """
    Get complete stream information using ffmpeg in a single call.
    
    This function replaces the previous two-step process (ffprobe + ffmpeg) with a 
    single ffmpeg call that extracts all needed information: codec, resolution, FPS, 
    and bitrate. This reduces network overhead and processing time.

    Args:
        url: Stream URL to analyze (will be validated and sanitized)
        duration: Duration in seconds to analyze the stream
        timeout: Base timeout in seconds (actual timeout includes duration + overhead)
        user_agent: User agent string to use for HTTP requests

    Returns:
        Dictionary containing:
        - video_codec: Video codec name (e.g., 'h264', 'hevc')
        - audio_codec: Audio codec name (e.g., 'aac', 'mp3')
        - resolution: Resolution string (e.g., '1920x1080')
        - fps: Frames per second (float)
        - bitrate_kbps: Bitrate in kbps (float or None)
        - status: "OK", "Timeout", or "Error"
        - elapsed_time: Time taken for the operation
    """
    # Validate and sanitize URL to prevent command injection
    if not url or not isinstance(url, str):
        logger.error("Invalid URL: must be a non-empty string")
        return {
            'video_codec': 'N/A',
            'audio_codec': 'N/A',
            'resolution': '0x0',
            'fps': 0,
            'bitrate_kbps': None,
            'status': 'Error',
            'elapsed_time': 0
        }
    
    # Basic URL validation - must start with http://, https://, or rtmp://
    url_lower = url.lower()
    if not (url_lower.startswith('http://') or url_lower.startswith('https://') or 
            url_lower.startswith('rtmp://') or url_lower.startswith('rtmps://')):
        logger.error(f"Invalid URL protocol: {url[:50]}... (must be http://, https://, rtmp://, or rtmps://)")
        return {
            'video_codec': 'N/A',
            'audio_codec': 'N/A',
            'resolution': '0x0',
            'fps': 0,
            'bitrate_kbps': None,
            'status': 'Error',
            'elapsed_time': 0
        }
    
    logger.debug(f"Analyzing stream with ffmpeg for {duration}s: {url[:50]}...")
    # Use list arguments to pass URL safely to subprocess without shell interpretation
    command = [
        'ffmpeg', '-re', '-v', 'debug', '-user_agent', user_agent,
        '-i', url, '-t', str(duration), '-f', 'null', '-'
    ]

    result_data = {
        'video_codec': 'N/A',
        'audio_codec': 'N/A',
        'resolution': '0x0',
        'fps': 0,
        'bitrate_kbps': None,
        'status': 'OK',
        'elapsed_time': 0
    }

    # Add buffer to timeout to account for ffmpeg startup, network latency, and shutdown overhead
    actual_timeout = timeout + duration + 10

    try:
        start = time.time()
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=actual_timeout,
            text=True
        )
        elapsed = time.time() - start
        result_data['elapsed_time'] = elapsed
        
        output = result.stderr
        total_bytes = 0
        progress_bitrate = None
        
        # Parse ffmpeg output to extract all information
        for line in output.splitlines():
            # Extract video codec, resolution, and FPS from Stream mapping line
            # Example: "Stream #0:0: Video: h264, yuv420p, 1920x1080, 25 fps"
            # Example with wrapped codec: "Stream #0:0(und): Video: wrapped_avframe (avc1 / 0x31637661), yuv420p, 1920x1080, 25 fps"
            if 'Stream #' in line and 'Video:' in line:
                try:
                    # Extract codec - try to get actual codec from parentheses first
                    # Pattern: wrapped_avframe (avc1 / 0x...) -> extract "avc1"
                    parenthesis_codec_match = re.search(r'Video:\s*\w+\s*\(([a-zA-Z0-9]+)', line)
                    if parenthesis_codec_match:
                        codec = parenthesis_codec_match.group(1)
                        codec = _sanitize_codec_name(codec)
                        if codec != 'N/A':
                            result_data['video_codec'] = codec
                            logger.debug(f"  → Detected video codec from parentheses: {result_data['video_codec']}")
                    
                    # Fallback: extract first word after "Video:"
                    if result_data['video_codec'] == 'N/A':
                        codec_match = re.search(r'Video:\s*(\w+)', line)
                        if codec_match:
                            codec = codec_match.group(1)
                            codec = _sanitize_codec_name(codec)
                            result_data['video_codec'] = codec
                            logger.debug(f"  → Detected video codec: {result_data['video_codec']}")
                    
                    # Extract resolution
                    res_match = re.search(r'(\d{2,5})x(\d{2,5})', line)
                    if res_match:
                        width, height = res_match.groups()
                        result_data['resolution'] = f"{width}x{height}"
                        logger.debug(f"  → Detected resolution: {result_data['resolution']}")
                    
                    # Extract FPS
                    fps_match = re.search(r'(\d+\.?\d*)\s*fps', line)
                    if fps_match:
                        result_data['fps'] = round(float(fps_match.group(1)), 2)
                        logger.debug(f"  → Detected FPS: {result_data['fps']}")
                except (ValueError, AttributeError) as e:
                    logger.debug(f"  → Error parsing video stream line: {e}")
            
            # Extract audio codec
            # Example: "Stream #0:1: Audio: aac, 48000 Hz, stereo"
            if 'Stream #' in line and 'Audio:' in line:
                try:
                    codec_match = re.search(r'Audio:\s*(\w+)', line)
                    if codec_match:
                        codec = codec_match.group(1)
                        codec = _sanitize_codec_name(codec)
                        result_data['audio_codec'] = codec
                        logger.debug(f"  → Detected audio codec: {result_data['audio_codec']}")
                except (ValueError, AttributeError) as e:
                    logger.debug(f"  → Error parsing audio stream line: {e}")
            
            # Extract bitrate using multiple methods (same as get_stream_bitrate)
            # Method 1: Statistics line with bytes read
            if "Statistics:" in line and "bytes read" in line:
                try:
                    parts = line.split("bytes read")
                    size_str = parts[0].strip().split()[-1]
                    total_bytes = int(size_str)
                    if total_bytes > 0 and duration > 0:
                        result_data['bitrate_kbps'] = (total_bytes * 8) / 1000 / duration
                        logger.debug(f"  → Calculated bitrate (method 1): {result_data['bitrate_kbps']:.2f} kbps from {total_bytes} bytes")
                except ValueError:
                    pass

            # Method 2: Parse progress output
            if "bitrate=" in line and "kbits/s" in line:
                try:
                    bitrate_match = re.search(r'bitrate=\s*(\d+\.?\d*)\s*kbits/s', line)
                    if bitrate_match:
                        progress_bitrate = float(bitrate_match.group(1))
                        logger.debug(f"  → Found progress bitrate (method 2): {progress_bitrate:.2f} kbps")
                except (ValueError, AttributeError):
                    pass

            # Method 3: Alternative bytes read pattern
            if result_data['bitrate_kbps'] is None and "bytes read" in line and "Statistics:" not in line:
                try:
                    bytes_match = re.search(r'(\d+)\s+bytes read', line)
                    if bytes_match:
                        total_bytes = int(bytes_match.group(1))
                        if total_bytes > 0 and duration > 0:
                            calculated_bitrate = (total_bytes * 8) / 1000 / duration
                            logger.debug(f"  → Calculated bitrate (method 3): {calculated_bitrate:.2f} kbps from {total_bytes} bytes")
                            result_data['bitrate_kbps'] = calculated_bitrate
                except (ValueError, AttributeError):
                    pass

        # Use progress bitrate as final fallback
        if result_data['bitrate_kbps'] is None and progress_bitrate is not None:
            result_data['bitrate_kbps'] = progress_bitrate
            logger.debug(f"  → Using last progress bitrate as fallback: {result_data['bitrate_kbps']:.2f} kbps")

        # Check if ffmpeg exited early with errors
        expected_min_time = duration * EARLY_EXIT_THRESHOLD
        exited_early = elapsed < expected_min_time
        
        # Log warnings if detection failed
        if result_data['bitrate_kbps'] is None:
            logger.warning(f"  ⚠ Failed to detect bitrate from ffmpeg output (analyzed for {elapsed:.2f}s, expected ~{duration}s)")
            
            if exited_early or result.returncode != 0:
                if result.returncode != 0:
                    logger.warning(f"  ⚠ ffmpeg exited with code {result.returncode}")
                else:
                    logger.warning(f"  ⚠ ffmpeg completed in {elapsed:.2f}s (expected ~{duration}s)")
                
                # Look for and log error messages
                error_patterns = [
                    "Connection refused", "Connection timed out", "Invalid data found",
                    "Server returned", "404 Not Found", "403 Forbidden", "401 Unauthorized",
                    "No route to host", "could not find codec", "Protocol not found",
                    "Error opening input", "Operation timed out", "I/O error",
                    "HTTP error", "SSL", "TLS", "Certificate"
                ]
                
                error_lines = []
                for line in output.splitlines():
                    line_lower = line.lower()
                    if any(pattern.lower() in line_lower for pattern in error_patterns):
                        error_lines.append(line.strip())
                
                if error_lines:
                    logger.warning(f"  ⚠ ffmpeg error details:")
                    for error_line in error_lines[:MAX_ERROR_LINES_TO_LOG]:
                        logger.warning(f"     {error_line}")

        logger.debug(f"  → Analysis completed in {elapsed:.2f}s")
        
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ({actual_timeout}s) while analyzing stream")
        result_data['status'] = "Timeout"
        result_data['elapsed_time'] = actual_timeout
    except Exception as e:
        logger.error(f"Stream analysis failed: {e}")
        result_data['status'] = "Error"
        result_data['elapsed_time'] = 0

    return result_data


def get_stream_bitrate(url: str, duration: int = 30, timeout: int = 30, user_agent: str = 'VLC/3.0.14') -> Tuple[Optional[float], str, float]:
    """
    Get stream bitrate using ffmpeg to analyze actual stream data.

    Uses multiple methods to detect bitrate:
    1. Primary: Parse "Statistics:" line with "bytes read"
    2. Fallback 1: Parse progress output lines (e.g., "bitrate=3333.3kbits/s")
    3. Fallback 2: Calculate from total bytes transferred

    Args:
        url: Stream URL to analyze
        duration: Duration in seconds to analyze the stream
        timeout: Base timeout in seconds (actual timeout includes duration + overhead)
        user_agent: User agent string to use for HTTP requests

    Returns:
        Tuple of (bitrate_kbps, status, elapsed_time)
        bitrate_kbps: Bitrate in kilobits per second, or None if detection failed
        status: "OK", "Timeout", or "Error"
        elapsed_time: Time taken for the operation
    """
    logger.debug(f"Analyzing bitrate for {duration}s...")
    command = [
        'ffmpeg', '-re', '-v', 'debug', '-user_agent', user_agent,
        '-i', url, '-t', str(duration), '-f', 'null', '-'
    ]

    bitrate = None
    status = "OK"

    # Add buffer to timeout to account for ffmpeg startup, network latency, and shutdown overhead
    # Since -re flag reads at real-time, ffmpeg takes at least duration seconds
    actual_timeout = timeout + duration + 10

    try:
        start = time.time()
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=actual_timeout,
            text=True
        )
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
                    if total_bytes > 0 and duration > 0:
                        bitrate = (total_bytes * 8) / 1000 / duration
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
            if bitrate is None and "bytes read" in line and "Statistics:" not in line:
                try:
                    # Look for pattern like "12345 bytes read"
                    bytes_match = re.search(r'(\d+)\s+bytes read', line)
                    if bytes_match:
                        total_bytes = int(bytes_match.group(1))
                        if total_bytes > 0 and duration > 0:
                            calculated_bitrate = (total_bytes * 8) / 1000 / duration
                            logger.debug(f"  → Calculated bitrate (method 3): {calculated_bitrate:.2f} kbps from {total_bytes} bytes")
                            bitrate = calculated_bitrate
                except (ValueError, AttributeError):
                    pass

        # Use progress bitrate as final fallback if primary methods didn't find anything
        if bitrate is None and progress_bitrate is not None:
            bitrate = progress_bitrate
            logger.debug(f"  → Using last progress bitrate as fallback: {bitrate:.2f} kbps")

        # Check if ffmpeg exited early with errors
        # If elapsed time is much less than duration, ffmpeg likely encountered an error
        expected_min_time = duration * EARLY_EXIT_THRESHOLD
        exited_early = elapsed < expected_min_time
        
        # Log if bitrate detection failed
        if bitrate is None:
            logger.warning(f"  ⚠ Failed to detect bitrate from ffmpeg output (analyzed for {elapsed:.2f}s, expected ~{duration}s)")
            logger.debug(f"  → Searched {len(output.splitlines())} lines of output")
            
            # If ffmpeg exited early or returned non-zero, provide more details
            if exited_early or result.returncode != 0:
                if result.returncode != 0:
                    logger.warning(f"  ⚠ ffmpeg exited with code {result.returncode}")
                else:
                    logger.warning(f"  ⚠ ffmpeg completed in {elapsed:.2f}s (expected ~{duration}s) - stream may have ended early or encountered an error")
                
                # Look for and log specific error messages from ffmpeg output
                error_patterns = [
                    "Connection refused", "Connection timed out", "Invalid data found",
                    "Server returned", "404 Not Found", "403 Forbidden", "401 Unauthorized",
                    "No route to host", "could not find codec", "Protocol not found",
                    "Error opening input", "Operation timed out", "I/O error",
                    "HTTP error", "SSL", "TLS", "Certificate"
                ]
                
                error_lines = []
                for line in output.splitlines():
                    line_lower = line.lower()
                    if any(pattern.lower() in line_lower for pattern in error_patterns):
                        error_lines.append(line.strip())
                
                if error_lines:
                    logger.warning(f"  ⚠ ffmpeg error details:")
                    for error_line in error_lines[:MAX_ERROR_LINES_TO_LOG]:
                        logger.warning(f"     {error_line}")
                else:
                    # Log last few lines of output for debugging
                    logger.debug(f"  → Last lines of ffmpeg output:")
                    for line in output.splitlines()[-MAX_DEBUG_LINES_TO_LOG:]:
                        if line.strip():
                            logger.debug(f"     {line.strip()}")

        logger.debug(f"  → Analysis completed in {elapsed:.2f}s")

    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ({actual_timeout}s) while fetching bitrate")
        status = "Timeout"
        elapsed = actual_timeout
    except Exception as e:
        logger.error(f"Bitrate check failed: {e}")
        status = "Error"
        elapsed = 0

    return bitrate, status, elapsed


def analyze_stream(
    stream_url: str,
    stream_id: int,
    stream_name: str = "Unknown",
    ffmpeg_duration: int = 30,
    timeout: int = 30,
    retries: int = 1,
    retry_delay: int = 10,
    user_agent: str = 'VLC/3.0.14'
) -> Dict[str, Any]:
    """
    Perform complete stream analysis including codec, resolution, FPS, bitrate, and audio.

    This is the main entry point for stream checking. Uses a single ffmpeg call to extract
    all information, reducing network overhead and processing time compared to the previous
    two-step process (ffprobe + ffmpeg).

    Args:
        stream_url: URL of the stream to analyze
        stream_id: Unique identifier for the stream
        stream_name: Human-readable name for the stream
        ffmpeg_duration: Duration in seconds for analysis
        timeout: Timeout in seconds for the operation
        retries: Number of retry attempts on failure
        retry_delay: Delay in seconds between retries
        user_agent: User agent string to use for HTTP requests

    Returns:
        Dictionary containing analysis results with keys:
        - stream_id: Stream identifier
        - stream_name: Stream name
        - stream_url: Stream URL
        - timestamp: ISO format timestamp of analysis
        - video_codec: Video codec name (e.g., 'h264', 'hevc')
        - audio_codec: Audio codec name (e.g., 'aac', 'mp3')
        - resolution: Resolution string (e.g., '1920x1080')
        - fps: Frames per second (float)
        - bitrate_kbps: Bitrate in kbps (float or None)
        - status: "OK", "Timeout", or "Error"
    """
    logger.info(f"▶ Analyzing stream: {stream_name} (ID: {stream_id})")

    for attempt in range(retries + 1):
        if attempt > 0:
            logger.info(f"  Retry attempt {attempt}/{retries} for {stream_name}")
            time.sleep(retry_delay)

        # Use single ffmpeg call to get all stream information
        logger.info("  Analyzing stream (single ffmpeg call)...")
        result_data = get_stream_info_and_bitrate(
            url=stream_url,
            duration=ffmpeg_duration,
            timeout=timeout,
            user_agent=user_agent
        )

        # Build result dictionary with metadata
        result = {
            'stream_id': stream_id,
            'stream_name': stream_name,
            'stream_url': stream_url,
            'timestamp': datetime.now().isoformat(),
            'video_codec': result_data['video_codec'],
            'audio_codec': result_data['audio_codec'],
            'resolution': result_data['resolution'],
            'fps': result_data['fps'],
            'bitrate_kbps': result_data['bitrate_kbps'],
            'status': result_data['status']
        }

        # Log results
        if result['video_codec'] != 'N/A' or result['resolution'] != '0x0':
            logger.info(f"    ✓ Video: {result['video_codec']}, {result['resolution']}, {result['fps']} FPS")
        else:
            logger.warning("    ✗ No video info found")

        if result['audio_codec'] != 'N/A':
            logger.info(f"    ✓ Audio: {result['audio_codec']}")
        else:
            logger.warning("    ✗ No audio info found")

        if result['status'] == "OK":
            if result['bitrate_kbps'] is not None:
                logger.info(f"    ✓ Bitrate: {result['bitrate_kbps']:.2f} kbps (elapsed: {result_data['elapsed_time']:.2f}s)")
            else:
                logger.warning(f"    ⚠ Bitrate detection failed (elapsed: {result_data['elapsed_time']:.2f}s)")
            logger.info(f"  ✓ Stream analysis complete for {stream_name}")
            break
        else:
            logger.warning(f"    ✗ Status: {result['status']} (elapsed: {result_data['elapsed_time']:.2f}s)")

            # If not the last attempt, continue to retry
            if attempt < retries:
                logger.warning(f"  Stream '{stream_name}' failed with status '{result['status']}'. Retrying in {retry_delay} seconds... ({attempt + 1}/{retries})")

    return result
