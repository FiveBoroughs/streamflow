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
import logging
import re
import subprocess
import time
from datetime import datetime
from typing import Dict, Optional, Tuple, Any

from logging_config import setup_logging

logger = setup_logging(__name__)


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
        
        # Log if bitrate detection failed
        if bitrate is None:
            logger.warning(f"  ⚠ Failed to detect bitrate from ffmpeg output (analyzed for {duration}s)")
            logger.debug(f"  → Searched {len(output.splitlines())} lines of output")
        
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
    
    This is the main entry point for stream checking. It performs the following steps:
    1. Get codec, resolution, and FPS using ffprobe
    2. Get bitrate using ffmpeg
    3. Retry on failure if configured
    
    Args:
        stream_url: URL of the stream to analyze
        stream_id: Unique identifier for the stream
        stream_name: Human-readable name for the stream
        ffmpeg_duration: Duration in seconds for bitrate analysis
        timeout: Timeout in seconds for each operation
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
        
        # Initialize result dictionary
        result = {
            'stream_id': stream_id,
            'stream_name': stream_name,
            'stream_url': stream_url,
            'timestamp': datetime.now().isoformat(),
            'video_codec': 'N/A',
            'audio_codec': 'N/A',
            'resolution': '0x0',
            'fps': 0,
            'bitrate_kbps': None,
            'status': 'N/A'
        }
        
        # Step 1: Get codec, resolution, and FPS from ffprobe
        logger.info(f"  [1/2] Fetching codec/resolution/FPS info...")
        video_info, audio_info = get_stream_info(stream_url, timeout, user_agent)
        
        if video_info:
            result['video_codec'] = video_info.get('codec_name', 'N/A')
            width = video_info.get('width', 0)
            height = video_info.get('height', 0)
            result['resolution'] = f"{width}x{height}"
            
            # Parse FPS from avg_frame_rate (format: "num/den")
            fps_str = video_info.get('avg_frame_rate', '0/1')
            try:
                num, den = map(int, fps_str.split('/'))
                result['fps'] = round(num / den, 2) if den != 0 else 0
            except (ValueError, ZeroDivisionError):
                result['fps'] = 0
            
            logger.info(f"    ✓ Video: {result['video_codec']}, {result['resolution']}, {result['fps']} FPS")
        else:
            logger.warning(f"    ✗ No video info found")
        
        if audio_info:
            result['audio_codec'] = audio_info.get('codec_name', 'N/A')
            logger.info(f"    ✓ Audio: {result['audio_codec']}")
        else:
            logger.warning(f"    ✗ No audio info found")
        
        # Step 2: Get bitrate from ffmpeg
        logger.info(f"  [2/2] Analyzing bitrate...")
        bitrate, status, elapsed = get_stream_bitrate(stream_url, ffmpeg_duration, timeout, user_agent)
        result['bitrate_kbps'] = bitrate
        result['status'] = status
        
        if status == "OK":
            if bitrate is not None:
                logger.info(f"    ✓ Bitrate: {bitrate:.2f} kbps (elapsed: {elapsed:.2f}s)")
            else:
                logger.warning(f"    ⚠ Bitrate detection failed (elapsed: {elapsed:.2f}s)")
            logger.info(f"  ✓ Stream analysis complete for {stream_name}")
            break
        else:
            logger.warning(f"    ✗ Status: {status} (elapsed: {elapsed:.2f}s)")
            
            # If not the last attempt, continue to retry
            if attempt < retries:
                logger.warning(f"  Stream '{stream_name}' failed with status '{status}'. Retrying in {retry_delay} seconds... ({attempt + 1}/{retries})")
    
    return result
