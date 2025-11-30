"""
API data fetching for the Universal Data Index (UDI) system.

Handles fetching data from the Dispatcharr API for initial load and refresh operations.
"""

import os
import sys
import time
import json
from typing import Dict, List, Optional, Any
import requests
from pathlib import Path
from dotenv import load_dotenv, set_key

from logging_config import setup_logging, log_api_request, log_api_response

logger = setup_logging(__name__)

env_path = Path('.') / '.env'

# Load environment variables from .env file if it exists
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


def _get_base_url() -> Optional[str]:
    """Get the base URL from environment variables.
    
    Returns:
        The Dispatcharr base URL or None if not set.
    """
    return os.getenv("DISPATCHARR_BASE_URL")


def _validate_token(token: str) -> bool:
    """Validate if a token is still valid by making a test API request.
    
    Args:
        token: The authentication token to validate
        
    Returns:
        True if token is valid, False otherwise
    """
    base_url = _get_base_url()
    if not base_url or not token:
        return False
    
    try:
        test_url = f"{base_url}/api/channels/channels/"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        resp = requests.get(test_url, headers=headers, timeout=5, params={'page_size': 1})
        return resp.status_code == 200
    except Exception:
        return False


def _login() -> bool:
    """Log into Dispatcharr and save the token.
    
    Returns:
        True if login successful, False otherwise.
    """
    username = os.getenv("DISPATCHARR_USER")
    password = os.getenv("DISPATCHARR_PASS")
    base_url = _get_base_url()

    if not all([username, password, base_url]):
        logger.error(
            "DISPATCHARR_USER, DISPATCHARR_PASS, and "
            "DISPATCHARR_BASE_URL must be set."
        )
        return False

    login_url = f"{base_url}/api/accounts/token/"
    logger.info(f"Attempting to log in to {base_url}...")

    try:
        resp = requests.post(
            login_url,
            headers={"Content-Type": "application/json"},
            json={"username": username, "password": password}
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access") or data.get("token")

        if token:
            if env_path.exists():
                set_key(env_path, "DISPATCHARR_TOKEN", token)
                logger.info("Login successful. Token saved.")
            else:
                os.environ["DISPATCHARR_TOKEN"] = token
                logger.info("Login successful. Token stored in memory.")
            return True
        else:
            logger.error("Login failed: No access token found in response.")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Login failed: {e}")
        return False
    except json.JSONDecodeError:
        logger.error("Login failed: Invalid JSON response from server.")
        return False


def _get_auth_headers() -> Dict[str, str]:
    """Get authorization headers for API requests.
    
    Returns:
        Dictionary containing authorization headers.
        
    Raises:
        SystemExit: If login fails or token cannot be retrieved.
    """
    current_token = os.getenv("DISPATCHARR_TOKEN")
    
    if current_token and _validate_token(current_token):
        return {
            "Authorization": f"Bearer {current_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    if current_token:
        logger.info("Existing token is invalid. Attempting to log in...")
    else:
        logger.info("DISPATCHARR_TOKEN not found. Attempting to log in...")
    
    if _login():
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
        current_token = os.getenv("DISPATCHARR_TOKEN")
        if not current_token:
            logger.error("Login succeeded, but token not found. Aborting.")
            sys.exit(1)
    else:
        logger.error("Login failed. Check credentials. Aborting.")
        sys.exit(1)

    return {
        "Authorization": f"Bearer {current_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


def _refresh_token() -> bool:
    """Refresh the authentication token.
    
    Returns:
        True if refresh successful, False otherwise.
    """
    logger.info("Token expired or invalid. Attempting to refresh...")
    if _login():
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
        logger.info("Token refreshed successfully.")
        return True
    else:
        logger.error("Token refresh failed.")
        return False


class UDIFetcher:
    """Fetches data from the Dispatcharr API for the UDI system."""
    
    def __init__(self):
        """Initialize the UDI fetcher."""
        self.base_url = _get_base_url()
    
    def _fetch_url(self, url: str) -> Optional[Any]:
        """Fetch data from a URL with authentication and retry logic.
        
        Args:
            url: The URL to fetch
            
        Returns:
            JSON response data or None if failed
        """
        try:
            start_time = time.time()
            log_api_request(logger, "GET", url)
            resp = requests.get(url, headers=_get_auth_headers())
            elapsed = time.time() - start_time
            log_api_response(logger, "GET", url, resp.status_code, elapsed)
            
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                if _refresh_token():
                    logger.info("Retrying request with new token...")
                    resp = requests.get(url, headers=_get_auth_headers())
                    resp.raise_for_status()
                    return resp.json()
            logger.error(f"Error fetching {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _fetch_paginated(self, base_url: str, page_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch paginated data from an API endpoint.
        
        Args:
            base_url: The base URL for the endpoint
            page_size: Number of items per page
            
        Returns:
            List of all items from all pages
        """
        all_items: List[Dict[str, Any]] = []
        url = f"{base_url}?page_size={page_size}"
        
        while url:
            response = self._fetch_url(url)
            if not response:
                break
            
            if isinstance(response, dict) and 'results' in response:
                all_items.extend(response.get('results', []))
                url = response.get('next')
            else:
                if isinstance(response, list):
                    all_items.extend(response)
                break
        
        return all_items
    
    def fetch_channels(self) -> List[Dict[str, Any]]:
        """Fetch all channels from Dispatcharr.
        
        Returns:
            List of channel dictionaries
        """
        if not self.base_url:
            logger.error("DISPATCHARR_BASE_URL not set")
            return []
        
        url = f"{self.base_url}/api/channels/channels/"
        channels = self._fetch_paginated(url)
        logger.info(f"Fetched {len(channels)} channels")
        return channels
    
    def fetch_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a specific channel by ID.
        
        Args:
            channel_id: The channel ID
            
        Returns:
            Channel dictionary or None
        """
        if not self.base_url:
            return None
        
        url = f"{self.base_url}/api/channels/channels/{channel_id}/"
        return self._fetch_url(url)
    
    def fetch_channel_streams(self, channel_id: int) -> List[Dict[str, Any]]:
        """Fetch streams for a specific channel.
        
        Args:
            channel_id: The channel ID
            
        Returns:
            List of stream dictionaries
        """
        if not self.base_url:
            return []
        
        url = f"{self.base_url}/api/channels/channels/{channel_id}/streams/"
        streams = self._fetch_url(url)
        return streams if isinstance(streams, list) else []
    
    def fetch_streams(self) -> List[Dict[str, Any]]:
        """Fetch all streams from Dispatcharr.
        
        Returns:
            List of stream dictionaries
        """
        if not self.base_url:
            logger.error("DISPATCHARR_BASE_URL not set")
            return []
        
        url = f"{self.base_url}/api/channels/streams/"
        streams = self._fetch_paginated(url)
        logger.info(f"Fetched {len(streams)} streams")
        return streams
    
    def fetch_stream_by_id(self, stream_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a specific stream by ID.
        
        Args:
            stream_id: The stream ID
            
        Returns:
            Stream dictionary or None
        """
        if not self.base_url:
            return None
        
        url = f"{self.base_url}/api/channels/streams/{stream_id}/"
        return self._fetch_url(url)
    
    def fetch_channel_groups(self) -> List[Dict[str, Any]]:
        """Fetch all channel groups from Dispatcharr.
        
        Returns:
            List of channel group dictionaries
        """
        if not self.base_url:
            logger.error("DISPATCHARR_BASE_URL not set")
            return []
        
        url = f"{self.base_url}/api/channels/groups/"
        groups = self._fetch_url(url)
        if isinstance(groups, list):
            logger.info(f"Fetched {len(groups)} channel groups")
            return groups
        return []
    
    def fetch_logos(self) -> List[Dict[str, Any]]:
        """Fetch all logos from Dispatcharr.
        
        Returns:
            List of logo dictionaries
        """
        if not self.base_url:
            logger.error("DISPATCHARR_BASE_URL not set")
            return []
        
        url = f"{self.base_url}/api/channels/logos/"
        logos = self._fetch_paginated(url)
        logger.info(f"Fetched {len(logos)} logos")
        return logos
    
    def fetch_logo_by_id(self, logo_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a specific logo by ID.
        
        Args:
            logo_id: The logo ID
            
        Returns:
            Logo dictionary or None
        """
        if not self.base_url:
            return None
        
        url = f"{self.base_url}/api/channels/logos/{logo_id}/"
        return self._fetch_url(url)
    
    def fetch_m3u_accounts(self) -> List[Dict[str, Any]]:
        """Fetch all M3U accounts from Dispatcharr.
        
        Returns:
            List of M3U account dictionaries
        """
        if not self.base_url:
            logger.error("DISPATCHARR_BASE_URL not set")
            return []
        
        url = f"{self.base_url}/api/m3u/accounts/"
        accounts = self._fetch_url(url)
        if isinstance(accounts, list):
            logger.info(f"Fetched {len(accounts)} M3U accounts")
            return accounts
        return []
    
    def refresh_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch all data from Dispatcharr.
        
        Returns:
            Dictionary with all fetched data
        """
        logger.info("Starting full data refresh from Dispatcharr API...")
        
        data = {
            'channels': self.fetch_channels(),
            'streams': self.fetch_streams(),
            'channel_groups': self.fetch_channel_groups(),
            'logos': self.fetch_logos(),
            'm3u_accounts': self.fetch_m3u_accounts()
        }
        
        logger.info(
            f"Full refresh complete: {len(data['channels'])} channels, "
            f"{len(data['streams'])} streams, {len(data['channel_groups'])} groups, "
            f"{len(data['logos'])} logos, {len(data['m3u_accounts'])} M3U accounts"
        )
        
        return data
