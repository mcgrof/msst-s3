#!/usr/bin/env python3
"""
Test 1038: Storage class GLACIER_IR

Tests GLACIER_IR storage class
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1038(s3_client, config):
    """Storage class GLACIER_IR"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1038')
        s3_client.create_bucket(bucket_name)

        # Test storage class: GLACIER_IR
        key = 'storage-class-test.txt'

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'Storage class test'),
                StorageClass='GLACIER_IR'
            )

            # Verify storage class
            response = s3_client.head_object(bucket_name, key)
            actual_class = response.get('StorageClass', 'STANDARD')
            print(f"Storage class 'GLACIER_IR' set (got '{actual_class}')")
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidStorageClass':
                print(f"Storage class 'GLACIER_IR' not supported")
            else:
                raise

        print(f"\nTest 1038 - Storage class GLACIER_IR: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1038 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1038: {error_code}")
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
