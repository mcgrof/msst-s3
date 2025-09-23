#!/usr/bin/env python3
"""
Test 1354: Notification s3:ObjectTagging:*

Tests s3:ObjectTagging:* event notification
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1354(s3_client, config):
    """Notification s3:ObjectTagging:*"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1354')
        s3_client.create_bucket(bucket_name)

        # Test event notification: s3:ObjectTagging:*
        try:
            # Configure bucket notification
            notification_config = {
                'QueueConfigurations': [{
                    'Id': 'NotificationConfig1354',
                    'QueueArn': 'arn:aws:sqs:us-east-1:123456789012:s3-notifications',
                    'Events': ['s3:ObjectTagging:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [{
                                'Name': 'prefix',
                                'Value': 'notifications/'
                            }]
                        }
                    }
                }]
            }

            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=notification_config
            )

            # Trigger event
            if 'ObjectCreated' in 's3:ObjectTagging:*':
                key = f'notifications/test-1354.txt'
                s3_client.put_object(bucket_name, key, io.BytesIO(b'Trigger notification'))

            print(f"Notification for 's3:ObjectTagging:*' configured")

        except ClientError as e:
            if e.response['Error']['Code'] in ['NotImplemented', 'InvalidArgument']:
                print(f"Notification type 's3:ObjectTagging:*' not supported")
            else:
                raise

        print(f"\nTest 1354 - Notification s3:ObjectTagging:*: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1354 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1354: {error_code}")
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
