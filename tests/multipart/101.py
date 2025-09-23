#!/usr/bin/env python3
"""
Test 101: Multipart upload - 10-part upload

Tests multipart upload functionality: uploading file in 10 parts
"""

import io
import os
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_101(s3_client, config):
    """Multipart upload - 10-part upload"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-101')
        s3_client.create_bucket(bucket_name)

        # 10-part multipart upload
        key = 'multipart-10parts.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        parts = []
        for i in range(1, 11):
            data = bytes([65 + i]) * part_size  # Different byte for each part
            response = s3_client.upload_part(bucket_name, key, upload_id, i, io.BytesIO(data))
            parts.append({'PartNumber': i, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)
        print("10-part multipart upload: ✓")

        print(f"\nMultipart test 101 completed: ✓")

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
