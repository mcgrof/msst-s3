#!/usr/bin/env python3
"""
Test 32: Content-Type variations

Tests various content types for objects
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_32(s3_client, config):
    """Content-Type variations"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-32')
        s3_client.create_bucket(bucket_name)

        # Test operations
        # Test various content types
        content_types = [
            ('file.html', 'text/html', b'<html></html>'),
            ('file.css', 'text/css', b'body { }'),
            ('file.js', 'application/javascript', b'console.log()'),
            ('file.json', 'application/json', b'{"key": "value"}'),
            ('file.xml', 'application/xml', b'<root></root>')
        ]

        for key, content_type, content in content_types:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(content),
                ContentType=content_type
            )

        print(f"Created {len(content_types)} objects with different content types: ✓")

        print(f"\nContent-Type variations completed: ✓")

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
