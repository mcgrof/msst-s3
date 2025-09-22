"""
Test fixtures and utilities for S3 testing
"""

import uuid
import random
import string
import io
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class TestFixture:
    """Base test fixture with common utilities"""

    def __init__(self, s3_client, config: Dict[str, Any]):
        """
        Initialize test fixture

        Args:
            s3_client: S3Client instance
            config: Test configuration
        """
        self.s3 = s3_client
        self.config = config
        self.bucket_prefix = config.get('s3_bucket_prefix', 'msst-test')
        self.created_buckets = []
        self.created_objects = []

    def generate_bucket_name(self, suffix: str = None) -> str:
        """Generate a unique bucket name"""
        name_parts = [self.bucket_prefix]
        if suffix:
            name_parts.append(suffix)
        name_parts.append(str(uuid.uuid4())[:8])
        return '-'.join(name_parts).lower()

    def generate_key_name(self, prefix: str = 'test-object') -> str:
        """Generate a unique object key"""
        return f"{prefix}-{uuid.uuid4()}"

    def generate_random_data(self, size: int) -> bytes:
        """Generate random binary data of specified size"""
        return bytes(random.getrandbits(8) for _ in range(size))

    def generate_text_data(self, size: int) -> str:
        """Generate random text data of specified size"""
        chars = string.ascii_letters + string.digits + string.punctuation + ' \n'
        return ''.join(random.choice(chars) for _ in range(size))

    def parse_size(self, size_str: str) -> int:
        """
        Parse size string (e.g., '1KB', '10MB', '1GB') to bytes

        Args:
            size_str: Size string with unit

        Returns:
            Size in bytes
        """
        units = {
            'B': 1,
            'KB': 1024,
            'MB': 1024 * 1024,
            'GB': 1024 * 1024 * 1024,
        }

        size_str = size_str.upper().strip()
        for unit, multiplier in units.items():
            if size_str.endswith(unit):
                number_str = size_str[:-len(unit)]
                try:
                    return int(float(number_str) * multiplier)
                except ValueError:
                    raise ValueError(f"Invalid size format: {size_str}")

        # If no unit specified, assume bytes
        try:
            return int(size_str)
        except ValueError:
            raise ValueError(f"Invalid size format: {size_str}")

    def create_test_bucket(self, bucket_name: str = None) -> str:
        """
        Create a test bucket and track it for cleanup

        Args:
            bucket_name: Optional bucket name, generated if not provided

        Returns:
            The bucket name
        """
        if not bucket_name:
            bucket_name = self.generate_bucket_name()

        self.s3.create_bucket(bucket_name)
        self.created_buckets.append(bucket_name)
        logger.debug(f"Created test bucket: {bucket_name}")
        return bucket_name

    def create_test_object(self, bucket_name: str, key: str = None,
                          data: bytes = None, size: int = None) -> str:
        """
        Create a test object and track it for cleanup

        Args:
            bucket_name: Bucket to create object in
            key: Optional object key, generated if not provided
            data: Optional data to upload
            size: Size of random data to generate if data not provided

        Returns:
            The object key
        """
        if not key:
            key = self.generate_key_name()

        if data is None:
            if size is None:
                size = 1024  # Default 1KB
            data = self.generate_random_data(size)

        self.s3.put_object(bucket_name, key, data)
        self.created_objects.append((bucket_name, key))
        logger.debug(f"Created test object: {bucket_name}/{key}")
        return key

    def cleanup(self):
        """Clean up all created resources"""
        # Delete all created objects
        for bucket_name, key in self.created_objects:
            try:
                self.s3.delete_object(bucket_name, key)
                logger.debug(f"Deleted test object: {bucket_name}/{key}")
            except Exception as e:
                logger.warning(f"Failed to delete object {bucket_name}/{key}: {e}")

        # Empty and delete all created buckets
        for bucket_name in self.created_buckets:
            try:
                # Empty the bucket first
                self.s3.empty_bucket(bucket_name)
                # Delete the bucket
                self.s3.delete_bucket(bucket_name)
                logger.debug(f"Deleted test bucket: {bucket_name}")
            except Exception as e:
                logger.warning(f"Failed to delete bucket {bucket_name}: {e}")

        self.created_objects.clear()
        self.created_buckets.clear()

@contextmanager
def cleanup_bucket(s3_client, bucket_name: str):
    """
    Context manager to ensure bucket cleanup

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket name to manage
    """
    try:
        yield bucket_name
    finally:
        try:
            # Empty the bucket
            s3_client.empty_bucket(bucket_name)
            # Delete the bucket
            s3_client.delete_bucket(bucket_name)
            logger.debug(f"Cleaned up bucket: {bucket_name}")
        except Exception as e:
            logger.warning(f"Failed to cleanup bucket {bucket_name}: {e}")

def create_multipart_chunks(data: bytes, chunk_size: int = 5 * 1024 * 1024) -> List[bytes]:
    """
    Split data into chunks for multipart upload

    Args:
        data: Data to split
        chunk_size: Size of each chunk (default 5MB)

    Returns:
        List of data chunks
    """
    chunks = []
    for i in range(0, len(data), chunk_size):
        chunks.append(data[i:i + chunk_size])
    return chunks

def calculate_etag(data: bytes) -> str:
    """
    Calculate ETag for data (MD5 hash)

    Args:
        data: Data to hash

    Returns:
        ETag string
    """
    import hashlib
    return f'"{hashlib.md5(data).hexdigest()}"'

def compare_data(data1: bytes, data2: bytes) -> bool:
    """
    Compare two data objects

    Args:
        data1: First data object
        data2: Second data object

    Returns:
        True if data matches, False otherwise
    """
    return data1 == data2

def generate_test_files(directory: str, count: int = 10,
                       min_size: int = 1024,
                       max_size: int = 10485760) -> List[str]:
    """
    Generate test files for upload testing

    Args:
        directory: Directory to create files in
        count: Number of files to create
        min_size: Minimum file size in bytes
        max_size: Maximum file size in bytes

    Returns:
        List of created file paths
    """
    import os
    import tempfile

    files = []
    os.makedirs(directory, exist_ok=True)

    for i in range(count):
        size = random.randint(min_size, max_size)
        data = bytes(random.getrandbits(8) for _ in range(size))

        filename = os.path.join(directory, f"test_file_{i}_{size}.bin")
        with open(filename, 'wb') as f:
            f.write(data)

        files.append(filename)
        logger.debug(f"Generated test file: {filename} ({size} bytes)")

    return files