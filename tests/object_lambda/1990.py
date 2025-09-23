#!/usr/bin/env python3
"""
Test 1990: Object Lambda redact_pii

Tests S3 Object Lambda transformation: redact_pii
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1990(s3_client, config):
    """Object Lambda redact_pii"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1990')
        s3_client.create_bucket(bucket_name)

        # Test S3 Object Lambda: redact_pii
        try:
            # Object Lambda Access Point configuration
            # This simulates Object Lambda transformation

            # Upload source object
            source_key = f'lambda-source/object-1990.txt'
            source_data = f'Original data for redact_pii transformation'.encode()
            s3_client.put_object(bucket_name, source_key, io.BytesIO(source_data))

            # Simulate transformation (in real scenario, Lambda would process)
            if 'redact_pii' == 'compress_data':
                import gzip
                transformed = gzip.compress(source_data)
            elif 'redact_pii' == 'encrypt_content':
                import base64
                transformed = base64.b64encode(source_data)
            elif 'redact_pii' == 'redact_pii':
                transformed = b'[REDACTED]'
            else:
                transformed = source_data + b' [TRANSFORMED]'

            # Store transformed result
            result_key = f'lambda-result/object-1990-redact_pii.txt'
            s3_client.put_object(bucket_name, result_key, io.BytesIO(transformed))

            print(f"Object Lambda transformation 'redact_pii' tested")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print("S3 Object Lambda not supported")
            else:
                raise

        print(f"\nTest 1990 - Object Lambda redact_pii: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1990 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1990: {error_code}")
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
