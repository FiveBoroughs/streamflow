"""
Redis-based storage layer for the Universal Data Index (UDI) system.

Provides Redis-backed persistence for cached data with thread-safe operations,
replacing the file-based JSON storage for better performance and distributed access.
"""

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from redis import Redis, ConnectionPool

from logging_config import setup_logging

logger = setup_logging(__name__)


class UDIRedisStorage:
    """Redis-based storage for UDI data with thread-safe operations."""
    
    # Redis key prefixes for different entity types
    KEY_PREFIX = 'udi:'
    CHANNELS_KEY = f'{KEY_PREFIX}channels'
    STREAMS_KEY = f'{KEY_PREFIX}streams'
    CHANNEL_GROUPS_KEY = f'{KEY_PREFIX}channel_groups'
    LOGOS_KEY = f'{KEY_PREFIX}logos'
    M3U_ACCOUNTS_KEY = f'{KEY_PREFIX}m3u_accounts'
    METADATA_KEY = f'{KEY_PREFIX}metadata'
    
    # Index keys for fast lookups
    CHANNEL_INDEX_KEY = f'{KEY_PREFIX}index:channels:'
    STREAM_INDEX_KEY = f'{KEY_PREFIX}index:streams:'
    STREAM_URL_INDEX_KEY = f'{KEY_PREFIX}index:stream_urls:'
    
    def __init__(self, redis_client: Optional[Redis] = None):
        """Initialize the UDI Redis storage.
        
        Args:
            redis_client: Optional Redis client instance. If not provided,
                         creates a new connection pool and client.
        """
        if redis_client is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
            redis_port = int(os.environ.get('REDIS_PORT', 6379))
            redis_db = int(os.environ.get('REDIS_DB', 0))
            
            # Create connection pool for thread-safe operations
            self.pool = ConnectionPool(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                max_connections=20,
                socket_timeout=5,
                socket_connect_timeout=5,
                decode_responses=True
            )
            self.redis = Redis(connection_pool=self.pool)
        else:
            self.redis = redis_client
            self.pool = None
        
        # Thread locks for each data type (for local operation consistency)
        self._channels_lock = threading.Lock()
        self._streams_lock = threading.Lock()
        self._channel_groups_lock = threading.Lock()
        self._logos_lock = threading.Lock()
        self._m3u_accounts_lock = threading.Lock()
        self._metadata_lock = threading.Lock()
        
        logger.info(f"UDI Redis storage initialized")
    
    def _serialize(self, data: Any) -> str:
        """Serialize data to JSON string.
        
        Args:
            data: Data to serialize
            
        Returns:
            JSON string
        """
        try:
            return json.dumps(data, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to serialize data: {e}")
            raise
    
    def _deserialize(self, data: Optional[str]) -> Optional[Any]:
        """Deserialize JSON string to Python object.
        
        Args:
            data: JSON string
            
        Returns:
            Deserialized Python object or None
        """
        if data is None:
            return None
        
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize data: {e}")
            return None
    
    # Channels
    def load_channels(self) -> List[Dict[str, Any]]:
        """Load all channels from Redis storage.
        
        Returns:
            List of channel dictionaries
        """
        with self._channels_lock:
            try:
                data = self.redis.get(self.CHANNELS_KEY)
                if data:
                    channels = self._deserialize(data)
                    logger.debug(f"Loaded {len(channels) if channels else 0} channels from Redis")
                    return channels or []
                return []
            except Exception as e:
                logger.error(f"Failed to load channels from Redis: {e}")
                return []
    
    def save_channels(self, channels: List[Dict[str, Any]]) -> bool:
        """Save all channels to Redis storage.
        
        Args:
            channels: List of channel dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        with self._channels_lock:
            try:
                # Save main list
                data = self._serialize(channels)
                self.redis.set(self.CHANNELS_KEY, data)
                
                # Build and save index for fast lookups
                pipe = self.redis.pipeline()
                
                # Clear old index
                old_keys = self.redis.keys(f'{self.CHANNEL_INDEX_KEY}*')
                if old_keys:
                    pipe.delete(*old_keys)
                
                # Build new index
                for channel in channels:
                    channel_id = channel.get('id')
                    if channel_id:
                        index_key = f'{self.CHANNEL_INDEX_KEY}{channel_id}'
                        pipe.set(index_key, self._serialize(channel))
                
                pipe.execute()
                
                logger.debug(f"Saved {len(channels)} channels to Redis")
                return True
            except Exception as e:
                logger.error(f"Failed to save channels to Redis: {e}")
                return False
    
    def get_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific channel by ID using the index.
        
        Args:
            channel_id: Channel ID
            
        Returns:
            Channel dictionary or None
        """
        try:
            index_key = f'{self.CHANNEL_INDEX_KEY}{channel_id}'
            data = self.redis.get(index_key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Failed to get channel {channel_id} from Redis: {e}")
            return None
    
    # Streams
    def load_streams(self) -> List[Dict[str, Any]]:
        """Load all streams from Redis storage.
        
        Returns:
            List of stream dictionaries
        """
        with self._streams_lock:
            try:
                data = self.redis.get(self.STREAMS_KEY)
                if data:
                    streams = self._deserialize(data)
                    logger.debug(f"Loaded {len(streams) if streams else 0} streams from Redis")
                    return streams or []
                return []
            except Exception as e:
                logger.error(f"Failed to load streams from Redis: {e}")
                return []
    
    def save_streams(self, streams: List[Dict[str, Any]]) -> bool:
        """Save all streams to Redis storage.
        
        Args:
            streams: List of stream dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        with self._streams_lock:
            try:
                # Save main list
                data = self._serialize(streams)
                self.redis.set(self.STREAMS_KEY, data)
                
                # Build and save indexes for fast lookups
                pipe = self.redis.pipeline()
                
                # Clear old indexes
                old_id_keys = self.redis.keys(f'{self.STREAM_INDEX_KEY}*')
                old_url_keys = self.redis.keys(f'{self.STREAM_URL_INDEX_KEY}*')
                if old_id_keys:
                    pipe.delete(*old_id_keys)
                if old_url_keys:
                    pipe.delete(*old_url_keys)
                
                # Build new indexes
                for stream in streams:
                    stream_id = stream.get('id')
                    stream_url = stream.get('url')
                    
                    if stream_id:
                        # ID index
                        id_index_key = f'{self.STREAM_INDEX_KEY}{stream_id}'
                        pipe.set(id_index_key, self._serialize(stream))
                    
                    if stream_url:
                        # URL index (for dead stream tracking)
                        # Use SHA-256 hash of URL to create a safe cache key
                        import hashlib
                        url_hash = hashlib.sha256(stream_url.encode()).hexdigest()
                        url_index_key = f'{self.STREAM_URL_INDEX_KEY}{url_hash}'
                        pipe.set(url_index_key, self._serialize(stream))
                
                pipe.execute()
                
                logger.debug(f"Saved {len(streams)} streams to Redis")
                return True
            except Exception as e:
                logger.error(f"Failed to save streams to Redis: {e}")
                return False
    
    def get_stream_by_id(self, stream_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific stream by ID using the index.
        
        Args:
            stream_id: Stream ID
            
        Returns:
            Stream dictionary or None
        """
        try:
            index_key = f'{self.STREAM_INDEX_KEY}{stream_id}'
            data = self.redis.get(index_key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Failed to get stream {stream_id} from Redis: {e}")
            return None
    
    # Channel Groups
    def load_channel_groups(self) -> List[Dict[str, Any]]:
        """Load all channel groups from Redis storage.
        
        Returns:
            List of channel group dictionaries
        """
        with self._channel_groups_lock:
            try:
                data = self.redis.get(self.CHANNEL_GROUPS_KEY)
                if data:
                    groups = self._deserialize(data)
                    logger.debug(f"Loaded {len(groups) if groups else 0} channel groups from Redis")
                    return groups or []
                return []
            except Exception as e:
                logger.error(f"Failed to load channel groups from Redis: {e}")
                return []
    
    def save_channel_groups(self, groups: List[Dict[str, Any]]) -> bool:
        """Save all channel groups to Redis storage.
        
        Args:
            groups: List of channel group dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        with self._channel_groups_lock:
            try:
                data = self._serialize(groups)
                self.redis.set(self.CHANNEL_GROUPS_KEY, data)
                logger.debug(f"Saved {len(groups)} channel groups to Redis")
                return True
            except Exception as e:
                logger.error(f"Failed to save channel groups to Redis: {e}")
                return False
    
    # Logos
    def load_logos(self) -> List[Dict[str, Any]]:
        """Load all logos from Redis storage.
        
        Returns:
            List of logo dictionaries
        """
        with self._logos_lock:
            try:
                data = self.redis.get(self.LOGOS_KEY)
                if data:
                    logos = self._deserialize(data)
                    logger.debug(f"Loaded {len(logos) if logos else 0} logos from Redis")
                    return logos or []
                return []
            except Exception as e:
                logger.error(f"Failed to load logos from Redis: {e}")
                return []
    
    def save_logos(self, logos: List[Dict[str, Any]]) -> bool:
        """Save all logos to Redis storage.
        
        Args:
            logos: List of logo dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        with self._logos_lock:
            try:
                data = self._serialize(logos)
                self.redis.set(self.LOGOS_KEY, data)
                logger.debug(f"Saved {len(logos)} logos to Redis")
                return True
            except Exception as e:
                logger.error(f"Failed to save logos to Redis: {e}")
                return False
    
    # M3U Accounts
    def load_m3u_accounts(self) -> List[Dict[str, Any]]:
        """Load all M3U accounts from Redis storage.
        
        Returns:
            List of M3U account dictionaries
        """
        with self._m3u_accounts_lock:
            try:
                data = self.redis.get(self.M3U_ACCOUNTS_KEY)
                if data:
                    accounts = self._deserialize(data)
                    logger.debug(f"Loaded {len(accounts) if accounts else 0} M3U accounts from Redis")
                    return accounts or []
                return []
            except Exception as e:
                logger.error(f"Failed to load M3U accounts from Redis: {e}")
                return []
    
    def save_m3u_accounts(self, accounts: List[Dict[str, Any]]) -> bool:
        """Save all M3U accounts to Redis storage.
        
        Args:
            accounts: List of M3U account dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        with self._m3u_accounts_lock:
            try:
                # Save main list
                data = self._serialize(accounts)
                self.redis.set(self.M3U_ACCOUNTS_KEY, data)
                
                # Build index for fast lookups
                pipe = self.redis.pipeline()
                index_prefix = f'{self.KEY_PREFIX}index:m3u_accounts:'
                
                # Clear old index
                old_keys = self.redis.keys(f'{index_prefix}*')
                if old_keys:
                    pipe.delete(*old_keys)
                
                # Build new index
                for account in accounts:
                    account_id = account.get('id')
                    if account_id:
                        index_key = f'{index_prefix}{account_id}'
                        pipe.set(index_key, self._serialize(account))
                
                pipe.execute()
                
                logger.debug(f"Saved {len(accounts)} M3U accounts to Redis")
                return True
            except Exception as e:
                logger.error(f"Failed to save M3U accounts to Redis: {e}")
                return False
    
    def get_m3u_account_by_id(self, account_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific M3U account by ID using the index.
        
        Args:
            account_id: M3U account ID
            
        Returns:
            M3U account dictionary or None
        """
        try:
            index_key = f'{self.KEY_PREFIX}index:m3u_accounts:{account_id}'
            data = self.redis.get(index_key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Failed to get M3U account {account_id} from Redis: {e}")
            return None
    
    # Metadata
    def load_metadata(self) -> Dict[str, Any]:
        """Load metadata from Redis storage.
        
        Returns:
            Metadata dictionary
        """
        with self._metadata_lock:
            try:
                data = self.redis.get(self.METADATA_KEY)
                if data:
                    metadata = self._deserialize(data)
                    return metadata or {}
                return {}
            except Exception as e:
                logger.error(f"Failed to load metadata from Redis: {e}")
                return {}
    
    def save_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Save metadata to Redis storage.
        
        Args:
            metadata: Metadata dictionary
            
        Returns:
            True if successful, False otherwise
        """
        with self._metadata_lock:
            try:
                data = self._serialize(metadata)
                self.redis.set(self.METADATA_KEY, data)
                logger.debug("Saved metadata to Redis")
                return True
            except Exception as e:
                logger.error(f"Failed to save metadata to Redis: {e}")
                return False
    
    def is_initialized(self) -> bool:
        """Check if storage has been initialized with data.
        
        Returns:
            True if any data exists in Redis
        """
        try:
            # Check if main keys exist
            return (
                self.redis.exists(self.CHANNELS_KEY) or
                self.redis.exists(self.STREAMS_KEY) or
                self.redis.exists(self.M3U_ACCOUNTS_KEY)
            )
        except Exception as e:
            logger.error(f"Failed to check if Redis storage is initialized: {e}")
            return False
    
    def clear_all(self) -> bool:
        """Clear all UDI data from Redis storage.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get all UDI keys
            keys = self.redis.keys(f'{self.KEY_PREFIX}*')
            if keys:
                self.redis.delete(*keys)
                logger.info(f"Cleared {len(keys)} keys from Redis storage")
            return True
        except Exception as e:
            logger.error(f"Failed to clear Redis storage: {e}")
            return False
    
    def health_check(self) -> bool:
        """Check if Redis connection is healthy.
        
        Returns:
            True if Redis is accessible and working
        """
        try:
            self.redis.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
    
    def close(self) -> None:
        """Close Redis connections and cleanup resources."""
        try:
            if self.pool:
                self.pool.disconnect()
            logger.info("Redis storage connections closed")
        except Exception as e:
            logger.error(f"Error closing Redis connections: {e}")
