#!/usr/bin/env python3
"""
Test 35: Content-Disposition tests

Tests content disposition for downloads
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_35(s3_client, config):
    """Content-Disposition tests"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-35')
        s3_client.create_bucket(bucket_name)

        # Test operations
        # Test content disposition
        dispositions = [
            ('inline-file.pdf', 'inline'),
            ('download.pdf', 'attachment'),
            ('named.pdf', 'attachment; filename="custom-name.pdf"')
        ]

        for key, disposition in dispositions:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'PDF content'),
                ContentDisposition=disposition
            )

            response = s3_client.head_object(bucket_name, key)
            assert response.get('ContentDisposition') == disposition

        print("Content-Disposition tested: ✓")

        print(f"\nContent-Disposition tests completed: ✓")

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
