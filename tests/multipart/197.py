#!/usr/bin/env python3
"""
Test 197: Multipart abort

Tests multipart abort operation
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_197(s3_client, config):
    """Multipart abort"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-197')
        s3_client.create_bucket(bucket_name)

        # Multipart operation: abort
        key = 'multipart-abort.bin'
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        if 'abort' == 'abort':
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)
            print("Multipart abort: ✓")
        elif 'abort' == 'list_parts':
            # Upload a part first
            s3_client.upload_part(bucket_name, key, upload_id, 1, io.BytesIO(b'X' * 5242880))
            response = s3_client.client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_id)
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)
            print("List parts: ✓")
        else:
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)

        print(f"\nTest 197 - Multipart abort: ✓")

    except ClientError as e:
        print(f"Error in test 197: {e.response['Error']['Code']}")
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
