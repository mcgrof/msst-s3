#!/usr/bin/env python3
"""
Test 100: Basic multipart upload

Tests multipart upload functionality for files > 5MB.
Validates proper handling of multipart upload lifecycle.
"""

import hashlib
import io
import os
from common.fixtures import TestFixture

def test_100(s3_client, config):
    """Basic multipart upload test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    upload_id = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-100')
        s3_client.create_bucket(bucket_name)

        object_key = 'multipart-test.bin'

        # Create 10MB file (requires multipart for S3)
        part_size = 5 * 1024 * 1024  # 5MB per part (minimum)
        num_parts = 2
        total_size = part_size * num_parts

        # Initiate multipart upload
        upload_id = s3_client.create_multipart_upload(bucket_name, object_key)

        # Track parts and data for verification
        parts = []
        full_data = b''
        hasher = hashlib.sha256()

        # Upload parts
        for part_num in range(1, num_parts + 1):
            # Generate part data
            part_data = os.urandom(part_size)
            full_data += part_data
            hasher.update(part_data)

            # Upload part
            part_response = s3_client.upload_part(
                bucket_name,
                object_key,
                upload_id,
                part_num,
                io.BytesIO(part_data)
            )

            parts.append({
                'PartNumber': part_num,
                'ETag': part_response['ETag']
            })

        original_hash = hasher.hexdigest()

        # Complete multipart upload
        complete_response = s3_client.complete_multipart_upload(
            bucket_name,
            object_key,
            upload_id,
            parts
        )

        # Verify the upload ID is no longer valid
        upload_id = None  # Mark as completed

        # Verify object exists and has correct size
        head_response = s3_client.head_object(bucket_name, object_key)
        assert head_response['ContentLength'] == total_size, \
            f"Size mismatch: expected {total_size}, got {head_response['ContentLength']}"

        # Download and verify integrity
        download_response = s3_client.get_object(bucket_name, object_key)
        downloaded_data = download_response['Body'].read()

        download_hasher = hashlib.sha256()
        download_hasher.update(downloaded_data)
        downloaded_hash = download_hasher.hexdigest()

        assert downloaded_hash == original_hash, \
            f"Data corrupted: expected hash {original_hash}, got {downloaded_hash}"
        assert len(downloaded_data) == total_size, \
            f"Downloaded size mismatch: expected {total_size}, got {len(downloaded_data)}"

        # Verify multipart ETag format (contains dash)
        etag = head_response.get('ETag', '').strip('"')
        assert '-' in etag, f"Multipart ETag should contain dash, got: {etag}"

    finally:
        # Cleanup
        if upload_id and bucket_name:
            # Abort incomplete multipart upload
            try:
                s3_client.abort_multipart_upload(
                    bucket_name,
                    object_key,
                    upload_id
                )
            except:
                pass

        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                s3_client.delete_object(bucket_name, object_key)
                s3_client.delete_bucket(bucket_name)
            except:
                pass