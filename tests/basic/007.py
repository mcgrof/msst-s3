#!/usr/bin/env python3
"""
Test 007: Resume interrupted multipart uploads

Tests ability to resume multipart uploads after interruption.
Validates that incomplete uploads can be continued from where they left off.
"""

import hashlib
import io
import os
import time
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_007(s3_client, config):
    """Resume interrupted multipart uploads test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    upload_id = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-007')
        s3_client.create_bucket(bucket_name)

        object_key = 'resume-multipart-test.bin'
        part_size = 5 * 1024 * 1024  # 5MB per part
        total_parts = 4

        # Generate test data for all parts
        all_parts_data = []
        for i in range(total_parts):
            part_data = os.urandom(part_size)
            all_parts_data.append(part_data)

        # Start multipart upload
        upload_id = s3_client.create_multipart_upload(bucket_name, object_key)

        # Upload first 2 parts (simulating partial upload)
        uploaded_parts = []
        for part_num in range(1, 3):  # Parts 1 and 2
            response = s3_client.upload_part(
                bucket_name,
                object_key,
                upload_id,
                part_num,
                io.BytesIO(all_parts_data[part_num - 1])
            )
            uploaded_parts.append({
                'PartNumber': part_num,
                'ETag': response['ETag']
            })

        # Simulate interruption - just pause
        time.sleep(1)

        # List multipart uploads to find our upload
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        uploads = response.get('Uploads', [])

        # Find our upload ID in the list
        found_upload = None
        for upload in uploads:
            if upload['Key'] == object_key and upload['UploadId'] == upload_id:
                found_upload = upload
                break

        assert found_upload is not None, \
            "Multipart upload should be findable after interruption"

        # List parts already uploaded
        response = s3_client.client.list_parts(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id
        )
        existing_parts = response.get('Parts', [])

        # Verify we have the expected parts
        assert len(existing_parts) == 2, \
            f"Expected 2 uploaded parts, found {len(existing_parts)}"

        # Resume upload - upload remaining parts
        for part_num in range(3, total_parts + 1):  # Parts 3 and 4
            response = s3_client.upload_part(
                bucket_name,
                object_key,
                upload_id,
                part_num,
                io.BytesIO(all_parts_data[part_num - 1])
            )
            uploaded_parts.append({
                'PartNumber': part_num,
                'ETag': response['ETag']
            })

        # Complete the multipart upload with all parts
        complete_response = s3_client.complete_multipart_upload(
            bucket_name,
            object_key,
            upload_id,
            uploaded_parts
        )
        upload_id = None  # Mark as completed

        # Verify the completed object
        response = s3_client.head_object(bucket_name, object_key)
        expected_size = part_size * total_parts
        actual_size = response['ContentLength']

        assert actual_size == expected_size, \
            f"Size mismatch: expected {expected_size}, got {actual_size}"

        # Download and verify data integrity
        response = s3_client.get_object(bucket_name, object_key)
        downloaded_data = response['Body'].read()

        # Reconstruct expected data
        expected_data = b''.join(all_parts_data)
        expected_hash = hashlib.sha256(expected_data).hexdigest()
        actual_hash = hashlib.sha256(downloaded_data).hexdigest()

        assert actual_hash == expected_hash, \
            f"Data integrity check failed after resume"

        # Test 2: Simulate finding and resuming an unknown upload
        # Start another upload but don't track parts
        object_key2 = 'resume-test-2.bin'
        upload_id2 = s3_client.create_multipart_upload(bucket_name, object_key2)

        # Upload one part
        s3_client.upload_part(
            bucket_name,
            object_key2,
            upload_id2,
            1,
            io.BytesIO(all_parts_data[0])
        )

        # List uploads and find it
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        found = False
        for upload in response.get('Uploads', []):
            if upload['Key'] == object_key2:
                found = True
                # We could resume this upload by listing its parts
                # and continuing from there
                break

        assert found, "Should be able to find incomplete uploads"

        # Clean up the incomplete upload
        s3_client.abort_multipart_upload(bucket_name, object_key2, upload_id2)

    finally:
        # Cleanup
        if upload_id and bucket_name:
            try:
                s3_client.abort_multipart_upload(bucket_name, object_key, upload_id)
            except:
                pass

        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass