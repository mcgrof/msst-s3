#!/usr/bin/env python3
"""
Test 33: Cache-Control headers

Tests cache control settings for objects
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_33(s3_client, config):
    """Cache-Control headers"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-33')
        s3_client.create_bucket(bucket_name)

        # Test operations
        # Test cache control headers
        cache_configs = [
            ('no-cache.txt', 'no-cache'),
            ('public.txt', 'public, max-age=3600'),
            ('private.txt', 'private, max-age=0'),
            ('immutable.txt', 'public, max-age=31536000, immutable')
        ]

        for key, cache_control in cache_configs:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'Cached content'),
                CacheControl=cache_control
            )

            # Verify cache control
            response = s3_client.head_object(bucket_name, key)
            assert response.get('CacheControl') == cache_control

        print(f"Cache-Control headers tested: ✓")

        print(f"\nCache-Control headers completed: ✓")

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
