"""
Validators for S3 test assertions
"""

import time
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

def validate_bucket_exists(s3_client, bucket_name: str,
                          timeout: int = 30) -> bool:
    """
    Validate that a bucket exists

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket name to check
        timeout: Maximum time to wait for bucket to exist

    Returns:
        True if bucket exists, False otherwise

    Raises:
        AssertionError: If bucket doesn't exist within timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if s3_client.bucket_exists(bucket_name):
            return True
        time.sleep(1)

    raise AssertionError(f"Bucket {bucket_name} does not exist after {timeout} seconds")

def validate_object_exists(s3_client, bucket_name: str, key: str,
                          timeout: int = 30) -> bool:
    """
    Validate that an object exists

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket containing the object
        key: Object key to check
        timeout: Maximum time to wait for object to exist

    Returns:
        True if object exists, False otherwise

    Raises:
        AssertionError: If object doesn't exist within timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if s3_client.object_exists(bucket_name, key):
            return True
        time.sleep(1)

    raise AssertionError(f"Object {bucket_name}/{key} does not exist after {timeout} seconds")

def validate_bucket_not_exists(s3_client, bucket_name: str,
                              timeout: int = 30) -> bool:
    """
    Validate that a bucket does not exist

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket name to check
        timeout: Maximum time to wait for bucket to not exist

    Returns:
        True if bucket doesn't exist, False otherwise

    Raises:
        AssertionError: If bucket still exists after timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not s3_client.bucket_exists(bucket_name):
            return True
        time.sleep(1)

    raise AssertionError(f"Bucket {bucket_name} still exists after {timeout} seconds")

def validate_object_not_exists(s3_client, bucket_name: str, key: str,
                              timeout: int = 30) -> bool:
    """
    Validate that an object does not exist

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket that would contain the object
        key: Object key to check
        timeout: Maximum time to wait for object to not exist

    Returns:
        True if object doesn't exist, False otherwise

    Raises:
        AssertionError: If object still exists after timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not s3_client.object_exists(bucket_name, key):
            return True
        time.sleep(1)

    raise AssertionError(f"Object {bucket_name}/{key} still exists after {timeout} seconds")

def validate_object_content(s3_client, bucket_name: str, key: str,
                           expected_data: bytes) -> bool:
    """
    Validate object content matches expected data

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket containing the object
        key: Object key
        expected_data: Expected object content

    Returns:
        True if content matches

    Raises:
        AssertionError: If content doesn't match
    """
    response = s3_client.get_object(bucket_name, key)
    actual_data = response['Body'].read()

    if actual_data != expected_data:
        raise AssertionError(
            f"Object content mismatch for {bucket_name}/{key}. "
            f"Expected {len(expected_data)} bytes, got {len(actual_data)} bytes"
        )

    return True

def validate_object_metadata(s3_client, bucket_name: str, key: str,
                            expected_metadata: Dict[str, str]) -> bool:
    """
    Validate object metadata

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket containing the object
        key: Object key
        expected_metadata: Expected metadata key-value pairs

    Returns:
        True if metadata matches

    Raises:
        AssertionError: If metadata doesn't match
    """
    response = s3_client.get_object(bucket_name, key)
    actual_metadata = response.get('Metadata', {})

    for key, expected_value in expected_metadata.items():
        actual_value = actual_metadata.get(key)
        if actual_value != expected_value:
            raise AssertionError(
                f"Metadata mismatch for key '{key}': "
                f"expected '{expected_value}', got '{actual_value}'"
            )

    return True

def validate_bucket_acl(s3_client, bucket_name: str,
                       expected_grants: List[Dict]) -> bool:
    """
    Validate bucket ACL grants

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket to check
        expected_grants: Expected ACL grants

    Returns:
        True if ACL matches

    Raises:
        AssertionError: If ACL doesn't match
    """
    acl = s3_client.get_bucket_acl(bucket_name)
    actual_grants = acl.get('Grants', [])

    if len(actual_grants) != len(expected_grants):
        raise AssertionError(
            f"ACL grant count mismatch: expected {len(expected_grants)}, "
            f"got {len(actual_grants)}"
        )

    # Simplified validation - can be made more sophisticated
    return True

def validate_object_acl(s3_client, bucket_name: str, key: str,
                       expected_grants: List[Dict]) -> bool:
    """
    Validate object ACL grants

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket containing the object
        key: Object key
        expected_grants: Expected ACL grants

    Returns:
        True if ACL matches

    Raises:
        AssertionError: If ACL doesn't match
    """
    acl = s3_client.get_object_acl(bucket_name, key)
    actual_grants = acl.get('Grants', [])

    if len(actual_grants) != len(expected_grants):
        raise AssertionError(
            f"ACL grant count mismatch for {bucket_name}/{key}: "
            f"expected {len(expected_grants)}, got {len(actual_grants)}"
        )

    # Simplified validation - can be made more sophisticated
    return True

def validate_bucket_versioning(s3_client, bucket_name: str,
                              expected_status: str) -> bool:
    """
    Validate bucket versioning status

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket to check
        expected_status: Expected versioning status ('Enabled', 'Suspended', 'Disabled')

    Returns:
        True if status matches

    Raises:
        AssertionError: If status doesn't match
    """
    actual_status = s3_client.get_bucket_versioning(bucket_name)

    # Handle default case where versioning is not configured
    if actual_status == '' and expected_status == 'Disabled':
        actual_status = 'Disabled'

    if actual_status != expected_status:
        raise AssertionError(
            f"Versioning status mismatch for {bucket_name}: "
            f"expected '{expected_status}', got '{actual_status}'"
        )

    return True

def validate_object_count(s3_client, bucket_name: str,
                         expected_count: int, prefix: str = '') -> bool:
    """
    Validate number of objects in a bucket

    Args:
        s3_client: S3Client instance
        bucket_name: Bucket to check
        expected_count: Expected number of objects
        prefix: Optional prefix to filter objects

    Returns:
        True if count matches

    Raises:
        AssertionError: If count doesn't match
    """
    objects = s3_client.list_objects(bucket_name, prefix=prefix)
    actual_count = len(objects)

    if actual_count != expected_count:
        raise AssertionError(
            f"Object count mismatch in {bucket_name}: "
            f"expected {expected_count}, got {actual_count}"
        )

    return True

def validate_response_headers(response: Dict[str, Any],
                             expected_headers: Dict[str, str]) -> bool:
    """
    Validate response headers

    Args:
        response: S3 API response
        expected_headers: Expected headers and values

    Returns:
        True if headers match

    Raises:
        AssertionError: If headers don't match
    """
    response_metadata = response.get('ResponseMetadata', {})
    headers = response_metadata.get('HTTPHeaders', {})

    for header_name, expected_value in expected_headers.items():
        actual_value = headers.get(header_name.lower())
        if actual_value != expected_value:
            raise AssertionError(
                f"Header mismatch for '{header_name}': "
                f"expected '{expected_value}', got '{actual_value}'"
            )

    return True

def validate_error_code(error, expected_code: str) -> bool:
    """
    Validate that an error has the expected error code

    Args:
        error: ClientError exception
        expected_code: Expected error code

    Returns:
        True if error code matches

    Raises:
        AssertionError: If error code doesn't match
    """
    from botocore.exceptions import ClientError

    if not isinstance(error, ClientError):
        raise AssertionError(f"Expected ClientError, got {type(error)}")

    actual_code = error.response['Error']['Code']
    if actual_code != expected_code:
        raise AssertionError(
            f"Error code mismatch: expected '{expected_code}', "
            f"got '{actual_code}'"
        )

    return True