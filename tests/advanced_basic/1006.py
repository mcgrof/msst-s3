#!/usr/bin/env python3
"""
Test 1006: Object with 6 tags

Tests object tagging with 6 tags
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1006(s3_client, config):
    """Object with 6 tags"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1006')
        s3_client.create_bucket(bucket_name)

        # Test object with 6 tags
        key = 'tagged-object.txt'
        tags = {}
        for j in range(6):
            tags[f'Tag{j}'] = f'Value{j}'

        tag_str = '&'.join([f'{k}={v}' for k, v in tags.items()])

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'Tagged content'),
            Tagging=tag_str
        )

        # Verify tags
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key)
        retrieved_tags = response.get('TagSet', [])
        assert len(retrieved_tags) >= 6, f"Expected 6 tags"

        print(f"\nTest 1006 - Object with 6 tags: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1006 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1006: {error_code}")
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
