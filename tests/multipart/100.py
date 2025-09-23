#!/usr/bin/env python3
"""
Test 100: Multipart upload - basic 2-part upload

Tests multipart upload functionality: uploading file in 2 parts
"""

import io
import os
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_100(s3_client, config):
    """Multipart upload - basic 2-part upload"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-100')
        s3_client.create_bucket(bucket_name)

        # Basic 2-part multipart upload
        key = 'multipart-2parts.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        parts = []
        for i in range(1, 3):
            data = (b'A' if i == 1 else b'B') * part_size
            response = s3_client.upload_part(bucket_name, key, upload_id, i, io.BytesIO(data))
            parts.append({'PartNumber': i, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)
        print("2-part multipart upload: ✓")

        print(f"\nMultipart test 100 completed: ✓")

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
