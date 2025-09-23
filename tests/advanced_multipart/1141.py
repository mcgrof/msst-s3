#!/usr/bin/env python3
"""
Test 1141: Multipart copy 1141

Tests multipart copy operation
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1141(s3_client, config):
    """Multipart copy 1141"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1141')
        s3_client.create_bucket(bucket_name)

        # Test multipart copy
        source_key = 'source-object.bin'
        dest_key = 'copied-object.bin'

        # Create source object (10MB)
        source_data = b'S' * (10 * 1024 * 1024)
        s3_client.put_object(bucket_name, source_key, io.BytesIO(source_data))

        # Initiate multipart copy
        upload_id = s3_client.create_multipart_upload(bucket_name, dest_key)

        # Copy in parts
        parts = []
        part_size = 5 * 1024 * 1024
        for part_num in range(1, 3):
            start = (part_num - 1) * part_size
            end = min(part_num * part_size - 1, len(source_data) - 1)

            response = s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                Key=dest_key,
                UploadId=upload_id,
                PartNumber=part_num,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                CopySourceRange=f'bytes={start}-{end}'
            )
            parts.append({
                'PartNumber': part_num,
                'ETag': response['CopyPartResult']['ETag']
            })

        s3_client.complete_multipart_upload(bucket_name, dest_key, upload_id, parts)
        print("Multipart copy completed")

        print(f"\nTest 1141 - Multipart copy 1141: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1141 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1141: {error_code}")
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
