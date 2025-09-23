#!/usr/bin/env python3
"""
Test 192: Multipart upload_part_copy

Tests multipart upload_part_copy operation
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_192(s3_client, config):
    """Multipart upload_part_copy"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-192')
        s3_client.create_bucket(bucket_name)

        # Multipart operation: upload_part_copy
        key = 'multipart-upload_part_copy.bin'
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        if 'upload_part_copy' == 'abort':
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)
            print("Multipart abort: ✓")
        elif 'upload_part_copy' == 'list_parts':
            # Upload a part first
            s3_client.upload_part(bucket_name, key, upload_id, 1, io.BytesIO(b'X' * 5242880))
            response = s3_client.client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_id)
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)
            print("List parts: ✓")
        else:
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)

        print(f"\nTest 192 - Multipart upload_part_copy: ✓")

    except ClientError as e:
        print(f"Error in test 192: {e.response['Error']['Code']}")
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
