#!/usr/bin/env python3
"""
Test 006: Concurrent upload integrity check

Tests data integrity when multiple clients upload simultaneously.
Ensures S3 handles concurrent operations without data corruption.
"""

import hashlib
import io
import threading
import time
from common.fixtures import TestFixture

def upload_object(s3_client, bucket_name, object_key, data, results, index):
    """Helper function for concurrent uploads"""
    try:
        # Calculate hash of data
        data_hash = hashlib.md5(data).hexdigest()

        # Upload object
        response = s3_client.put_object(
            bucket_name,
            object_key,
            io.BytesIO(data),
            Metadata={'hash': data_hash, 'thread': str(index)}
        )

        # Store result
        results[index] = {
            'success': True,
            'etag': response.get('ETag', '').strip('"'),
            'hash': data_hash,
            'key': object_key
        }
    except Exception as e:
        results[index] = {
            'success': False,
            'error': str(e),
            'key': object_key
        }

def test_006(s3_client, config):
    """Concurrent upload integrity check"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    uploaded_keys = []

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-006')
        s3_client.create_bucket(bucket_name)

        # Prepare concurrent upload tasks
        num_concurrent = 10
        threads = []
        results = {}
        test_data = {}

        # Create unique data for each upload
        for i in range(num_concurrent):
            object_key = f'concurrent-upload-{i}.dat'
            # Each file has unique content
            data = f"Thread {i} data: ".encode() + bytes(range(256)) * 100
            test_data[object_key] = {
                'data': data,
                'hash': hashlib.md5(data).hexdigest()
            }
            uploaded_keys.append(object_key)

            # Create thread for upload
            thread = threading.Thread(
                target=upload_object,
                args=(s3_client, bucket_name, object_key, data, results, i)
            )
            threads.append(thread)

        # Start all uploads simultaneously
        start_time = time.time()
        for thread in threads:
            thread.start()

        # Wait for all uploads to complete
        for thread in threads:
            thread.join(timeout=30)

        upload_duration = time.time() - start_time

        # Verify all uploads succeeded
        failed_uploads = [r for r in results.values() if not r['success']]
        assert not failed_uploads, \
            f"Some uploads failed: {failed_uploads}"

        # Verify data integrity for each uploaded object
        for i in range(num_concurrent):
            object_key = f'concurrent-upload-{i}.dat'
            original_data = test_data[object_key]['data']
            original_hash = test_data[object_key]['hash']

            # Download and verify
            response = s3_client.get_object(bucket_name, object_key)
            downloaded_data = response['Body'].read()
            downloaded_hash = hashlib.md5(downloaded_data).hexdigest()

            # Check integrity
            assert downloaded_hash == original_hash, \
                f"Object {object_key} corrupted: expected {original_hash}, got {downloaded_hash}"
            assert downloaded_data == original_data, \
                f"Object {object_key} data mismatch"

            # Verify metadata
            metadata = response.get('Metadata', {})
            assert metadata.get('hash') == original_hash, \
                f"Object {object_key} metadata hash mismatch"

        # List bucket to ensure all objects exist
        objects = s3_client.list_objects(bucket_name)
        object_keys = [obj['Key'] for obj in objects]
        for key in uploaded_keys:
            assert key in object_keys, f"Object {key} not found in bucket listing"

        # Performance check
        assert upload_duration < 30, \
            f"Concurrent uploads took too long: {upload_duration}s"

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                for key in uploaded_keys:
                    try:
                        s3_client.delete_object(bucket_name, key)
                    except:
                        pass
                s3_client.delete_bucket(bucket_name)
            except:
                pass