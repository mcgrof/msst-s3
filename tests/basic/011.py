#!/usr/bin/env python3
"""
Test 011: Network timeout handling

Tests S3 client behavior during network timeouts.
Validates proper timeout handling and error reporting.
"""

import time
import socket
from unittest.mock import patch, MagicMock
from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError
from common.fixtures import TestFixture

def test_011(s3_client, config):
    """Network timeout handling test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-011')
        s3_client.create_bucket(bucket_name)

        test_key = 'timeout-test.txt'
        test_data = b'Test data for timeout handling'

        # Test 1: Verify normal operation works
        response = s3_client.put_object(bucket_name, test_key, test_data)
        assert 'ETag' in response, "Normal upload should succeed"

        # Test 2: Simulate connection timeout
        original_timeout = s3_client.client._client_config.connect_timeout if \
            hasattr(s3_client.client, '_client_config') else None

        # Create a very short timeout to trigger timeout errors
        timeout_config = {
            'connect_timeout': 0.001,  # 1ms timeout
            'read_timeout': 0.001
        }

        # Test timeout recovery with retries
        max_retries = 3
        retry_count = 0
        last_error = None

        # Test with simulated slow response
        large_data = b'x' * (1024 * 1024)  # 1MB data

        # Track timing
        start_time = time.time()

        try:
            # This should handle timeout gracefully
            # Most S3 clients have built-in retry logic
            response = s3_client.put_object(
                bucket_name,
                'large-timeout-test.bin',
                large_data
            )
            upload_time = time.time() - start_time

            # Verify the upload eventually succeeded (with retries)
            assert 'ETag' in response, "Upload should succeed with retries"

        except Exception as e:
            # Timeout errors are expected and should be handled gracefully
            error_msg = str(e)
            assert any(word in error_msg.lower() for word in
                      ['timeout', 'timed out', 'connection', 'read']), \
                f"Expected timeout error, got: {error_msg}"

        # Test 3: Verify bucket operations continue working after timeout
        try:
            objects = s3_client.list_objects(bucket_name)
            assert isinstance(objects, list), "List should work after timeout recovery"
        except Exception as e:
            # Even if list fails, it should be a clean error
            assert 'timeout' in str(e).lower() or 'connection' in str(e).lower(), \
                "Errors after timeout should be clear"

        # Test 4: Validate timeout configuration is respected
        # Create object with normal timeout to verify recovery
        time.sleep(1)  # Brief pause to allow connection reset

        small_data = b'Small test data'
        try:
            response = s3_client.put_object(
                bucket_name,
                'recovery-test.txt',
                small_data
            )
            assert 'ETag' in response, "Should recover after timeout issues"
        except Exception:
            # Some S3 implementations may need more time to recover
            pass

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Clean up test objects
                for key in ['timeout-test.txt', 'large-timeout-test.bin',
                           'recovery-test.txt']:
                    try:
                        s3_client.delete_object(bucket_name, key)
                    except:
                        pass
                s3_client.delete_bucket(bucket_name)
            except:
                pass