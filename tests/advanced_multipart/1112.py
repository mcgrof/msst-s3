#!/usr/bin/env python3
"""
Test 1112: Variable parts 1112

Tests multipart with variable part sizes
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1112(s3_client, config):
    """Variable parts 1112"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1112')
        s3_client.create_bucket(bucket_name)

        # Test multipart with variable part sizes
        key = 'variable-parts.bin'
        upload_id = s3_client.create_multipart_upload(bucket_name, key)
        parts = []

        # Create parts with increasing sizes
        for part_num in range(1, 4):
            size = 5 * 1024 * 1024 * part_num  # 5MB, 10MB, 15MB
            data = bytes([65 + part_num]) * size
            response = s3_client.upload_part(
                bucket_name, key, upload_id, part_num, io.BytesIO(data)
            )
            parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)
        print("Variable size multipart upload completed")

        print(f"\nTest 1112 - Variable parts 1112: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1112 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1112: {error_code}")
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
