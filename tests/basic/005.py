#!/usr/bin/env python3
"""
Test 005: Large file integrity (configurable size)

Tests data integrity for large files using chunked uploads.
Validates that large files maintain integrity through S3 operations.
"""

import hashlib
import io
import os
from common.fixtures import TestFixture

def test_005(s3_client, config):
    """Large file integrity test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-005')
        s3_client.create_bucket(bucket_name)

        # Generate large file (10MB by default, configurable)
        file_size = 10 * 1024 * 1024  # 10MB
        chunk_size = 5 * 1024 * 1024  # 5MB chunks (minimum for multipart)

        # Create file with predictable pattern for verification
        hasher = hashlib.sha256()
        object_key = 'large-file-test.bin'

        # Upload in chunks to avoid memory issues
        parts = []
        total_uploaded = 0

        # Initiate multipart upload for files > 5MB
        if file_size > 5 * 1024 * 1024:
            upload_id = s3_client.create_multipart_upload(
                bucket_name, object_key
            )

            part_number = 1
            while total_uploaded < file_size:
                # Generate chunk data
                remaining = file_size - total_uploaded
                chunk_size_actual = min(chunk_size, remaining)

                # Create predictable data pattern
                chunk_data = os.urandom(chunk_size_actual)
                hasher.update(chunk_data)

                # Upload part
                response = s3_client.upload_part(
                    bucket_name,
                    object_key,
                    upload_id,
                    part_number,
                    io.BytesIO(chunk_data)
                )

                parts.append({
                    'PartNumber': part_number,
                    'ETag': response['ETag']
                })

                total_uploaded += chunk_size_actual
                part_number += 1

            # Complete multipart upload
            s3_client.complete_multipart_upload(
                bucket_name,
                object_key,
                upload_id,
                parts
            )
        else:
            # Single upload for smaller files
            test_data = os.urandom(file_size)
            hasher.update(test_data)
            s3_client.put_object(
                bucket_name,
                object_key,
                io.BytesIO(test_data)
            )

        original_hash = hasher.hexdigest()

        # Download and verify integrity
        download_hasher = hashlib.sha256()
        response = s3_client.get_object(bucket_name, object_key)

        # Read in chunks to handle large files
        while True:
            chunk = response['Body'].read(chunk_size)
            if not chunk:
                break
            download_hasher.update(chunk)

        downloaded_hash = download_hasher.hexdigest()

        # Verify integrity
        assert downloaded_hash == original_hash, \
            f"Large file corrupted: expected hash {original_hash}, got {downloaded_hash}"

        # Verify object size
        head_response = s3_client.head_object(bucket_name, object_key)
        reported_size = head_response['ContentLength']
        assert reported_size == file_size, \
            f"Size mismatch: expected {file_size}, got {reported_size}"

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                s3_client.delete_object(bucket_name, object_key)
                s3_client.delete_bucket(bucket_name)
            except:
                pass