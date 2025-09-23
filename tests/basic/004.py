#!/usr/bin/env python3
"""
Test 004: MD5/ETag validation on upload/download

Tests data integrity by validating MD5 checksums and ETags.
Ensures data is not corrupted during upload or download.
"""

import hashlib
import io
from common.fixtures import TestFixture
from common.validators import validate_object_exists

def test_004(s3_client, config):
    """MD5/ETag validation on upload/download"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-004')
        s3_client.create_bucket(bucket_name)

        # Generate test data with known MD5
        test_data = b"This is test data for MD5 validation" * 100
        original_md5 = hashlib.md5(test_data).hexdigest()

        # Upload object with MD5
        object_key = 'test-md5-validation.bin'
        response = s3_client.put_object(
            bucket_name,
            object_key,
            io.BytesIO(test_data),
            Metadata={'original-md5': original_md5}
        )

        # Verify ETag matches MD5 (for non-multipart uploads)
        etag = response.get('ETag', '').strip('"')
        assert etag == original_md5, f"ETag {etag} doesn't match MD5 {original_md5}"

        # Download and verify data integrity
        downloaded = s3_client.get_object(bucket_name, object_key)
        downloaded_data = downloaded['Body'].read()
        downloaded_md5 = hashlib.md5(downloaded_data).hexdigest()

        # Verify data integrity
        assert downloaded_md5 == original_md5, \
            f"Data corrupted: expected MD5 {original_md5}, got {downloaded_md5}"
        assert downloaded_data == test_data, "Downloaded data doesn't match original"

        # Verify metadata preserved
        metadata = downloaded.get('Metadata', {})
        assert metadata.get('original-md5') == original_md5, \
            "Metadata MD5 not preserved"

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                s3_client.delete_object(bucket_name, object_key)
                s3_client.delete_bucket(bucket_name)
            except:
                pass