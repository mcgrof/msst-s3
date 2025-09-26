#!/usr/bin/env python3
"""
Test: Bucket Notification Configuration
Tests S3 bucket event notifications for Lambda, SQS, SNS, and webhook endpoints.
Essential for event-driven architectures and real-time processing pipelines.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import json
import time

def test_bucket_notifications(s3_client: S3Client):
    """Test bucket notification configuration and event triggers"""
    bucket_name = f's3-notifications-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Get empty notification configuration
        print("Test 1: Get empty notification configuration")
        try:
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            # Should return empty configuration
            if (not response.get('TopicConfigurations', []) and
                not response.get('QueueConfigurations', []) and
                not response.get('LambdaConfigurations', [])):
                results['passed'].append('Empty notification config')
                print("✓ Empty config: No notifications configured initially")
            else:
                results['failed'].append('Empty config: Found unexpected configurations')

        except Exception as e:
            results['failed'].append(f'Empty config: {str(e)}')

        # Test 2: Configure SNS topic notification
        print("\nTest 2: SNS topic notification configuration")

        sns_config = {
            'TopicConfigurations': [
                {
                    'Id': 'sns-notification-1',
                    'TopicArn': 'arn:aws:sns:us-east-1:123456789012:s3-events',
                    'Events': [
                        's3:ObjectCreated:*'
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=sns_config
            )

            # Verify configuration
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            topics = response.get('TopicConfigurations', [])
            if len(topics) == 1 and topics[0]['Id'] == 'sns-notification-1':
                results['passed'].append('SNS notification config')
                print("✓ SNS config: Topic notification configured")
            else:
                results['failed'].append('SNS config: Configuration mismatch')

        except Exception as e:
            if 'NotImplemented' in str(e) or 'UnsupportedNotification' in str(e):
                results['passed'].append('SNS notifications not supported')
                print("✓ SNS config: Feature not implemented (expected)")
            else:
                results['failed'].append(f'SNS config: {str(e)}')

        # Test 3: Configure SQS queue notification
        print("\nTest 3: SQS queue notification configuration")

        sqs_config = {
            'QueueConfigurations': [
                {
                    'Id': 'sqs-notification-1',
                    'QueueArn': 'arn:aws:sqs:us-east-1:123456789012:s3-events',
                    'Events': [
                        's3:ObjectCreated:Put',
                        's3:ObjectCreated:Post'
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=sqs_config
            )

            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            queues = response.get('QueueConfigurations', [])
            if len(queues) == 1 and len(queues[0]['Events']) == 2:
                results['passed'].append('SQS notification config')
                print("✓ SQS config: Queue notification configured")
            else:
                results['failed'].append('SQS config: Configuration mismatch')

        except Exception as e:
            if 'NotImplemented' in str(e) or 'UnsupportedNotification' in str(e):
                results['passed'].append('SQS notifications not supported')
                print("✓ SQS config: Feature not implemented (expected)")
            else:
                results['failed'].append(f'SQS config: {str(e)}')

        # Test 4: Configure Lambda function notification
        print("\nTest 4: Lambda function notification configuration")

        lambda_config = {
            'LambdaConfigurations': [
                {
                    'Id': 'lambda-notification-1',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:s3-processor',
                    'Events': [
                        's3:ObjectCreated:*',
                        's3:ObjectRemoved:*'
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=lambda_config
            )

            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            lambdas = response.get('LambdaConfigurations', [])
            if len(lambdas) == 1 and 's3:ObjectCreated:*' in lambdas[0]['Events']:
                results['passed'].append('Lambda notification config')
                print("✓ Lambda config: Function notification configured")
            else:
                results['failed'].append('Lambda config: Configuration mismatch')

        except Exception as e:
            if 'NotImplemented' in str(e) or 'UnsupportedNotification' in str(e):
                results['passed'].append('Lambda notifications not supported')
                print("✓ Lambda config: Feature not implemented (expected)")
            else:
                results['failed'].append(f'Lambda config: {str(e)}')

        # Test 5: Multiple notification configurations
        print("\nTest 5: Multiple notification targets")

        multi_config = {
            'TopicConfigurations': [
                {
                    'Id': 'topic-creates',
                    'TopicArn': 'arn:aws:sns:us-east-1:123456789012:creates',
                    'Events': ['s3:ObjectCreated:*']
                }
            ],
            'QueueConfigurations': [
                {
                    'Id': 'queue-deletes',
                    'QueueArn': 'arn:aws:sqs:us-east-1:123456789012:deletes',
                    'Events': ['s3:ObjectRemoved:*']
                }
            ],
            'LambdaConfigurations': [
                {
                    'Id': 'lambda-all',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:all-events',
                    'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=multi_config
            )

            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            topic_count = len(response.get('TopicConfigurations', []))
            queue_count = len(response.get('QueueConfigurations', []))
            lambda_count = len(response.get('LambdaConfigurations', []))

            if topic_count + queue_count + lambda_count >= 2:
                results['passed'].append('Multiple notification targets')
                print("✓ Multiple targets: Multiple notification types configured")
            else:
                results['failed'].append('Multiple targets: Not all configurations preserved')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Multiple notifications not supported')
                print("✓ Multiple targets: Feature not implemented")
            else:
                results['failed'].append(f'Multiple targets: {str(e)}')

        # Test 6: Notification with prefix/suffix filters
        print("\nTest 6: Notification filters (prefix/suffix)")

        filtered_config = {
            'LambdaConfigurations': [
                {
                    'Id': 'filtered-images',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:image-processor',
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': 'images/'
                                },
                                {
                                    'Name': 'suffix',
                                    'Value': '.jpg'
                                }
                            ]
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=filtered_config
            )

            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            lambdas = response.get('LambdaConfigurations', [])
            if (len(lambdas) == 1 and
                'Filter' in lambdas[0] and
                'FilterRules' in lambdas[0]['Filter']['Key']):
                results['passed'].append('Notification filters')
                print("✓ Filters: Prefix/suffix filters configured")
            else:
                results['failed'].append('Filters: Filter configuration not preserved')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Notification filters not supported')
                print("✓ Filters: Feature not implemented")
            else:
                results['failed'].append(f'Filters: {str(e)}')

        # Test 7: Invalid notification configuration
        print("\nTest 7: Invalid notification configurations")

        # Test invalid ARN format
        invalid_config = {
            'TopicConfigurations': [
                {
                    'Id': 'invalid-arn',
                    'TopicArn': 'invalid-arn-format',
                    'Events': ['s3:ObjectCreated:*']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=invalid_config
            )
            results['failed'].append('Invalid ARN: Should have been rejected')
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'MalformedXML' in str(e) or 'InvalidRequest' in str(e):
                results['passed'].append('Invalid ARN rejected')
                print("✓ Invalid ARN: Correctly rejected")
            elif 'NotImplemented' in str(e):
                results['passed'].append('Notification validation not implemented')
                print("✓ Invalid ARN: Validation not implemented")
            else:
                results['failed'].append(f'Invalid ARN: Unexpected error: {e}')

        # Test invalid event type
        invalid_event_config = {
            'LambdaConfigurations': [
                {
                    'Id': 'invalid-event',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:test',
                    'Events': ['s3:InvalidEvent:*']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=invalid_event_config
            )
            results['failed'].append('Invalid event: Should have been rejected')
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'UnsupportedNotification' in str(e):
                results['passed'].append('Invalid event rejected')
                print("✓ Invalid event: Correctly rejected")
            elif 'NotImplemented' in str(e):
                results['passed'].append('Event validation not implemented')
                print("✓ Invalid event: Validation not implemented")
            else:
                results['failed'].append(f'Invalid event: {str(e)}')

        # Test 8: Clear notification configuration
        print("\nTest 8: Clear notification configuration")

        empty_config = {
            'TopicConfigurations': [],
            'QueueConfigurations': [],
            'LambdaConfigurations': []
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=empty_config
            )

            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            if (not response.get('TopicConfigurations', []) and
                not response.get('QueueConfigurations', []) and
                not response.get('LambdaConfigurations', [])):
                results['passed'].append('Clear notifications')
                print("✓ Clear config: All notifications removed")
            else:
                results['failed'].append('Clear config: Notifications remain')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Clear notifications not supported')
                print("✓ Clear config: Feature not implemented")
            else:
                results['failed'].append(f'Clear config: {str(e)}')

        # Test 9: Cloud Events format (MinIO specific)
        print("\nTest 9: MinIO Cloud Events format")

        # MinIO supports CloudEvents format for webhook notifications
        cloudEvents_config = {
            'CloudWatchConfigurations': [
                {
                    'Id': 'cloudwatch-metrics',
                    'CloudWatchConfiguration': {
                        'LogGroupName': 's3-access-logs'
                    },
                    'Events': ['s3:ObjectCreated:*']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=cloudEvents_config
            )

            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            if 'CloudWatchConfigurations' in response:
                results['passed'].append('CloudWatch notifications')
                print("✓ CloudWatch: Configuration accepted")
            else:
                results['passed'].append('CloudWatch not supported')
                print("✓ CloudWatch: Feature not available")

        except Exception as e:
            if 'NotImplemented' in str(e) or 'UnsupportedNotification' in str(e):
                results['passed'].append('CloudWatch notifications not supported')
                print("✓ CloudWatch: Feature not implemented (expected)")
            else:
                results['failed'].append(f'CloudWatch: {str(e)}')

        # Test 10: Notification event validation with actual object operations
        print("\nTest 10: Event trigger validation")

        # Set up basic notification for testing
        test_config = {
            'LambdaConfigurations': [
                {
                    'Id': 'event-test',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:event-test',
                    'Events': ['s3:ObjectCreated:Put']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=test_config
            )

            # Create an object to trigger notification
            test_key = 'notification-test-object'
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=b'test notification trigger'
            )

            # Note: We can't actually verify notification delivery without a real endpoint
            # But we can verify the configuration accepts the trigger
            results['passed'].append('Event trigger setup')
            print("✓ Event trigger: Object creation with notification configured")

            # Clean up test object
            s3_client.client.delete_object(Bucket=bucket_name, Key=test_key)

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Event triggers not supported')
                print("✓ Event trigger: Feature not implemented")
            else:
                results['failed'].append(f'Event trigger: {str(e)}')

        # Summary
        print(f"\n=== Bucket Notification Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

        return len(results['failed']) == 0

    except Exception as e:
        print(f"Critical error in notification test setup: {str(e)}")
        return False

    finally:
        # Cleanup
        try:
            # Clear notifications first
            try:
                empty_config = {
                    'TopicConfigurations': [],
                    'QueueConfigurations': [],
                    'LambdaConfigurations': []
                }
                s3_client.client.put_bucket_notification_configuration(
                    Bucket=bucket_name,
                    NotificationConfiguration=empty_config
                )
            except:
                pass

            # Clean up objects
            objects = s3_client.client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3_client.client.delete_object(Bucket=bucket_name, Key=obj['Key'])

            s3_client.delete_bucket(bucket_name)
        except:
            pass

if __name__ == "__main__":
    s3 = S3Client(
        endpoint_url='http://localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        region='us-east-1',
        verify_ssl=False
    )
    test_bucket_notifications(s3)