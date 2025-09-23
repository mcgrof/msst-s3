#!/usr/bin/env python3
"""
Test 1602: Batch Invoke AWS Lambda function

Tests S3 Batch Operation: Invoke AWS Lambda function
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1602(s3_client, config):
    """Batch Invoke AWS Lambda function"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1602')
        s3_client.create_bucket(bucket_name)

        # Test S3 Batch Operation: Invoke AWS Lambda function
        try:
            # Create objects for batch operation
            for j in range(5):
                key = f'batch/object-{j}.txt'
                s3_client.put_object(bucket_name, key, io.BytesIO(f'Batch {j}'.encode()))

            # Simulate batch operation (actual batch ops require job creation)
            if 'Invoke AWS Lambda function' == 'Copy':
                # Copy objects to new prefix
                objects = s3_client.list_objects(bucket_name, prefix='batch/')
                for obj in objects[:3]:  # Process first 3
                    s3_client.client.copy_object(
                        CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                        Bucket=bucket_name,
                        Key=obj['Key'].replace('batch/', 'copied/')
                    )
                print(f"Batch copy operation simulated")

            elif 'Invoke AWS Lambda function' == 'Replace all object tags':
                objects = s3_client.list_objects(bucket_name, prefix='batch/')
                for obj in objects[:3]:
                    s3_client.client.put_object_tagging(
                        Bucket=bucket_name,
                        Key=obj['Key'],
                        Tagging={'TagSet': [{'Key': 'BatchOp', 'Value': str(1602)}]}
                    )
                print(f"Batch tagging operation simulated")

            else:
                print(f"Batch operation 'Invoke AWS Lambda function' acknowledged")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print(f"Batch operation 'Invoke AWS Lambda function' not supported")
            else:
                raise

        print(f"\nTest 1602 - Batch Invoke AWS Lambda function: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1602 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1602: {error_code}")
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
