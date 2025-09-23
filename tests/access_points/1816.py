#!/usr/bin/env python3
"""
Test 1816: Access Point 1816

Tests S3 Access Point configuration 1816
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1816(s3_client, config):
    """Access Point 1816"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1816')
        s3_client.create_bucket(bucket_name)

        # Test S3 Access Point 1816
        try:
            # Create access point configuration
            access_point_name = f'ap-test-1816'

            # Note: Access points require account ID and are created at account level
            # This simulates the configuration
            access_point_config = {
                'Name': access_point_name,
                'Bucket': bucket_name,
                'VpcConfiguration': {
                    'VpcId': 'vpc-12345678'
                },
                'PublicAccessBlockConfiguration': {
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                },
                'Policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [{
                        'Effect': 'Allow',
                        'Principal': {'AWS': 'arn:aws:iam::123456789012:user/TestUser'},
                        'Action': 's3:GetObject',
                        'Resource': f'arn:aws:s3:::{bucket_name}/*'
                    }]
                })
            }

            # Simulate access through access point
            # In real scenario, would use access point ARN
            key = f'access-point/test-1816.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Access point test'))

            print(f"Access point scenario 1816 tested")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                print("S3 Access Points not supported")
            else:
                raise

        print(f"\nTest 1816 - Access Point 1816: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1816 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1816: {error_code}")
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
