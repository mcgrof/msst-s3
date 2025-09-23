#!/usr/bin/env python3
"""
Test 108: Multipart 6 parts

Tests multipart upload with 6 parts
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_108(s3_client, config):
    """Multipart 6 parts"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-108')
        s3_client.create_bucket(bucket_name)

        # Multipart upload with 6 parts
        key = 'multipart-6parts.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        upload_id = s3_client.create_multipart_upload(bucket_name, key)
        parts = []

        for part_num in range(1, 7):
            data = bytes([65 + (part_num % 26)]) * part_size
            response = s3_client.upload_part(
                bucket_name, key, upload_id, part_num, io.BytesIO(data)
            )
            parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)

        print(f"\nTest 108 - Multipart 6 parts: ✓")

    except ClientError as e:
        print(f"Error in test 108: {e.response['Error']['Code']}")
        raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
