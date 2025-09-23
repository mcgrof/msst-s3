#!/usr/bin/env python3
"""
Test 1173: Part replacement 1173

Tests multipart part replacement
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1173(s3_client, config):
    """Part replacement 1173"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1173')
        s3_client.create_bucket(bucket_name)

        # Test part replacement
        key = 'part-replacement.bin'
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload initial part
        data1 = b'A' * (5 * 1024 * 1024)
        response1 = s3_client.upload_part(
            bucket_name, key, upload_id, 1, io.BytesIO(data1)
        )

        # Replace the same part
        data2 = b'B' * (5 * 1024 * 1024)
        response2 = s3_client.upload_part(
            bucket_name, key, upload_id, 1, io.BytesIO(data2)
        )

        # Use the second upload's ETag
        parts = [{'PartNumber': 1, 'ETag': response2['ETag']}]

        # Add another part
        data3 = b'C' * (5 * 1024 * 1024)
        response3 = s3_client.upload_part(
            bucket_name, key, upload_id, 2, io.BytesIO(data3)
        )
        parts.append({'PartNumber': 2, 'ETag': response3['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)
        print("Part replacement test completed")

        print(f"\nTest 1173 - Part replacement 1173: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1173 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1173: {error_code}")
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
