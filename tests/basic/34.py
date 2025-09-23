#!/usr/bin/env python3
"""
Test 34: Content-Encoding tests

Tests content encoding like gzip
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_34(s3_client, config):
    """Content-Encoding tests"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-34')
        s3_client.create_bucket(bucket_name)

        # Test operations
        # Test content encoding
        import gzip

        # Create gzipped content
        original = b'This is the original content that will be compressed' * 10
        compressed = gzip.compress(original)

        key = 'compressed.txt.gz'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(compressed),
            ContentEncoding='gzip',
            ContentType='text/plain'
        )

        # Verify encoding
        response = s3_client.head_object(bucket_name, key)
        assert response.get('ContentEncoding') == 'gzip'
        print("Content-Encoding tested: ✓")

        print(f"\nContent-Encoding tests completed: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Error: {error_code}")
        raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
