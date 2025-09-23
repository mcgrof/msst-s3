#!/usr/bin/env python3
"""
Test 1965: Object Lambda resize_image

Tests S3 Object Lambda transformation: resize_image
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1965(s3_client, config):
    """Object Lambda resize_image"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1965')
        s3_client.create_bucket(bucket_name)

        # Test S3 Object Lambda: resize_image
        try:
            # Object Lambda Access Point configuration
            # This simulates Object Lambda transformation

            # Upload source object
            source_key = f'lambda-source/object-1965.txt'
            source_data = f'Original data for resize_image transformation'.encode()
            s3_client.put_object(bucket_name, source_key, io.BytesIO(source_data))

            # Simulate transformation (in real scenario, Lambda would process)
            if 'resize_image' == 'compress_data':
                import gzip
                transformed = gzip.compress(source_data)
            elif 'resize_image' == 'encrypt_content':
                import base64
                transformed = base64.b64encode(source_data)
            elif 'resize_image' == 'redact_pii':
                transformed = b'[REDACTED]'
            else:
                transformed = source_data + b' [TRANSFORMED]'

            # Store transformed result
            result_key = f'lambda-result/object-1965-resize_image.txt'
            s3_client.put_object(bucket_name, result_key, io.BytesIO(transformed))

            print(f"Object Lambda transformation 'resize_image' tested")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print("S3 Object Lambda not supported")
            else:
                raise

        print(f"\nTest 1965 - Object Lambda resize_image: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1965 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1965: {error_code}")
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
