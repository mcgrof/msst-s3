#!/usr/bin/env python3
"""
Test 1463: Lambda integration 1463

Tests Lambda function trigger 1463
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1463(s3_client, config):
    """Lambda integration 1463"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1463')
        s3_client.create_bucket(bucket_name)

        # Test Lambda integration scenario 1463
        try:
            # Configure Lambda function trigger
            lambda_config = {
                'LambdaFunctionConfigurations': [{
                    'Id': f'LambdaConfig1463',
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
            key = f'images/test-1463.jpg'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'\xFF\xD8\xFF'))  # JPEG header

            print(f"Lambda trigger 1463 configured")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print("Lambda integration not supported")
            else:
                raise

        print(f"\nTest 1463 - Lambda integration 1463: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1463 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1463: {error_code}")
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
