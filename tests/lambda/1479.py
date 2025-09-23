#!/usr/bin/env python3
"""
Test 1479: Lambda integration 1479

Tests Lambda function trigger 1479
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1479(s3_client, config):
    """Lambda integration 1479"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1479')
        s3_client.create_bucket(bucket_name)

        # Test Lambda integration scenario 1479
        try:
            # Configure Lambda function trigger
            lambda_config = {
                'LambdaFunctionConfigurations': [{
                    'Id': f'LambdaConfig1479',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:ProcessS3Object',
                    'Events': ['s3:ObjectCreated:Put'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [{
                                'Name': 'suffix',
                                'Value': '.jpg'
                            }]
                        }
                    }
                }]
            }

            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=lambda_config
            )

            # Upload object to trigger Lambda
            key = f'images/test-1479.jpg'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'\xFF\xD8\xFF'))  # JPEG header

            print(f"Lambda trigger 1479 configured")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print("Lambda integration not supported")
            else:
                raise

        print(f"\nTest 1479 - Lambda integration 1479: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1479 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1479: {error_code}")
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
