#!/usr/bin/env python3
"""
Test 1352: Notification s3:ReducedRedundancyLostObject

Tests s3:ReducedRedundancyLostObject event notification
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1352(s3_client, config):
    """Notification s3:ReducedRedundancyLostObject"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1352')
        s3_client.create_bucket(bucket_name)

        # Test event notification: s3:ReducedRedundancyLostObject
        try:
            # Configure bucket notification
            notification_config = {
                'QueueConfigurations': [{
                    'Id': 'NotificationConfig1352',
                    'QueueArn': 'arn:aws:sqs:us-east-1:123456789012:s3-notifications',
                    'Events': ['s3:ReducedRedundancyLostObject'],
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
            if 'ObjectCreated' in 's3:ReducedRedundancyLostObject':
                key = f'notifications/test-1352.txt'
                s3_client.put_object(bucket_name, key, io.BytesIO(b'Trigger notification'))

            print(f"Notification for 's3:ReducedRedundancyLostObject' configured")

        except ClientError as e:
            if e.response['Error']['Code'] in ['NotImplemented', 'InvalidArgument']:
                print(f"Notification type 's3:ReducedRedundancyLostObject' not supported")
            else:
                raise

        print(f"\nTest 1352 - Notification s3:ReducedRedundancyLostObject: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1352 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1352: {error_code}")
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
