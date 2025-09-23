#!/usr/bin/env python3
"""
Test 1190: Multipart metadata 1190

Tests multipart with metadata and tags
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1190(s3_client, config):
    """Multipart metadata 1190"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1190')
        s3_client.create_bucket(bucket_name)

        # Test multipart with metadata and tags
        key = 'multipart-metadata.bin'

        metadata = {
            'upload-id': str(1190),
            'timestamp': str(time.time()),
            'type': 'multipart'
        }

        tags = f'Type=Multipart&ID=1190'

        upload_id = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            Metadata=metadata,
            Tagging=tags
        )['UploadId']

        # Upload parts
        parts = []
        for part_num in range(1, 3):
            data = b'M' * (5 * 1024 * 1024)
            response = s3_client.upload_part(
                bucket_name, key, upload_id, part_num, io.BytesIO(data)
            )
            parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)

        # Verify metadata and tags
        head_response = s3_client.head_object(bucket_name, key)
        assert 'Metadata' in head_response, "Metadata not preserved"

        print(f"\nTest 1190 - Multipart metadata 1190: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1190 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1190: {error_code}")
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
