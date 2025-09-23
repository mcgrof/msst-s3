#!/usr/bin/env python3
"""
Test 102: Abort multipart upload

Tests cleanup of incomplete multipart uploads.
Validates proper resource cleanup and error handling.
"""

import io
import os
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_102(s3_client, config):
    """Abort multipart upload test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-102')
        s3_client.create_bucket(bucket_name)

        # Test 1: Start and abort a multipart upload
        object_key = 'abort-test.bin'
        upload_id = s3_client.create_multipart_upload(bucket_name, object_key)

        # Upload some parts
        part_size = 5 * 1024 * 1024  # 5MB
        parts_uploaded = []

        for part_num in range(1, 3):  # Upload 2 parts
            part_data = os.urandom(part_size)
            response = s3_client.upload_part(
                bucket_name,
                object_key,
                upload_id,
                part_num,
                io.BytesIO(part_data)
            )
            parts_uploaded.append({
                'PartNumber': part_num,
                'ETag': response['ETag']
            })

        # Abort the upload
        s3_client.abort_multipart_upload(bucket_name, object_key, upload_id)

        # Verify the object doesn't exist
        try:
            s3_client.get_object(bucket_name, object_key)
            assert False, "Object should not exist after abort"
        except ClientError as e:
            assert e.response['Error']['Code'] in ['NoSuchKey', '404'], \
                "Expected NoSuchKey after abort"

        # Test 2: Verify upload ID is invalid after abort
        try:
            # Try to upload another part with aborted upload ID
            s3_client.upload_part(
                bucket_name,
                object_key,
                upload_id,
                3,
                io.BytesIO(b'test data')
            )
            assert False, "Should not be able to upload with aborted ID"
        except ClientError as e:
            # Expected - upload ID should be invalid
            error_code = e.response['Error']['Code']
            assert error_code in ['NoSuchUpload', 'InvalidUploadId'], \
                f"Expected invalid upload error, got: {error_code}"

        # Test 3: Multiple uploads with same key
        upload_ids = []

        # Start multiple uploads for same object
        for i in range(3):
            new_upload_id = s3_client.create_multipart_upload(
                bucket_name, f'multi-abort-{i}.bin'
            )
            upload_ids.append((f'multi-abort-{i}.bin', new_upload_id))

        # Upload parts to each
        for key, uid in upload_ids:
            part_data = os.urandom(5 * 1024 * 1024)
            s3_client.upload_part(
                bucket_name, key, uid, 1, io.BytesIO(part_data)
            )

        # Abort all uploads
        for key, uid in upload_ids:
            s3_client.abort_multipart_upload(bucket_name, key, uid)

        # Verify none of the objects exist
        for key, _ in upload_ids:
            try:
                s3_client.get_object(bucket_name, key)
                assert False, f"Object {key} should not exist after abort"
            except ClientError as e:
                assert e.response['Error']['Code'] in ['NoSuchKey', '404']

        # Test 4: Complete one, abort another
        key1 = 'complete-this.bin'
        key2 = 'abort-this.bin'

        upload_id1 = s3_client.create_multipart_upload(bucket_name, key1)
        upload_id2 = s3_client.create_multipart_upload(bucket_name, key2)

        # Upload parts to both
        part_data = os.urandom(5 * 1024 * 1024)

        part1_response = s3_client.upload_part(
            bucket_name, key1, upload_id1, 1, io.BytesIO(part_data)
        )
        part2_response = s3_client.upload_part(
            bucket_name, key2, upload_id2, 1, io.BytesIO(part_data)
        )

        # Complete the first
        s3_client.complete_multipart_upload(
            bucket_name, key1, upload_id1,
            [{'PartNumber': 1, 'ETag': part1_response['ETag']}]
        )

        # Abort the second
        s3_client.abort_multipart_upload(bucket_name, key2, upload_id2)

        # Verify first exists, second doesn't
        response = s3_client.get_object(bucket_name, key1)
        assert response['ContentLength'] == len(part_data), \
            "Completed upload should exist"

        try:
            s3_client.get_object(bucket_name, key2)
            assert False, "Aborted upload should not exist"
        except ClientError as e:
            assert e.response['Error']['Code'] in ['NoSuchKey', '404']

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass