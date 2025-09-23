#!/usr/bin/env python3
"""
Test 1129: Concurrent uploads 6

Tests 6 concurrent multipart uploads
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1129(s3_client, config):
    """Concurrent uploads 6"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1129')
        s3_client.create_bucket(bucket_name)

        # Test 6 concurrent multipart uploads
        import threading

        def upload_multipart(index):
            key = f'concurrent-{index}.bin'
            upload_id = s3_client.create_multipart_upload(bucket_name, key)

            # Upload 2 parts
            parts = []
            for part_num in range(1, 3):
                data = b'X' * (5 * 1024 * 1024)
                response = s3_client.upload_part(
                    bucket_name, key, upload_id, part_num, io.BytesIO(data)
                )
                parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

            s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)

        threads = []
        for j in range(6):
            t = threading.Thread(target=upload_multipart, args=(j,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print(f"Completed {upload_count} concurrent uploads")

        print(f"\nTest 1129 - Concurrent uploads 6: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1129 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1129: {error_code}")
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
