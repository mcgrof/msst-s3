#!/usr/bin/env python3
"""
Test 139: Part size 19MB

Tests multipart with 19MB parts
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_139(s3_client, config):
    """Part size 19MB"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-139')
        s3_client.create_bucket(bucket_name)

        # Multipart with 19MB parts
        key = 'multipart-19mb.bin'
        part_size = 19 * 1024 * 1024

        upload_id = s3_client.create_multipart_upload(bucket_name, key)
        parts = []

        for part_num in range(1, 3):  # 2 parts
            data = b'P' * part_size
            response = s3_client.upload_part(
                bucket_name, key, upload_id, part_num, io.BytesIO(data)
            )
            parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)

        print(f"\nTest 139 - Part size 19MB: ✓")

    except ClientError as e:
        print(f"Error in test 139: {e.response['Error']['Code']}")
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
