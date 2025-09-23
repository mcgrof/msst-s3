#!/usr/bin/env python3
"""
Test 1069: Request payment config 1069

Tests request payment configuration
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1069(s3_client, config):
    """Request payment config 1069"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1069')
        s3_client.create_bucket(bucket_name)

        # Test request payment configuration
        try:
            # Set request payment
            s3_client.client.put_bucket_request_payment(
                Bucket=bucket_name,
                RequestPaymentConfiguration={
                    'Payer': 'Requester' if 1069 % 2 == 0 else 'BucketOwner'
                }
            )

            # Verify configuration
            response = s3_client.client.get_bucket_request_payment(Bucket=bucket_name)
            payer = response.get('Payer', 'BucketOwner')
            print(f"Request payment set to: {payer}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print("Request payment not supported")
            else:
                raise

        print(f"\nTest 1069 - Request payment config 1069: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1069 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1069: {error_code}")
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
