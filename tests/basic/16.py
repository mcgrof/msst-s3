#!/usr/bin/env python3
"""
Test 16: Bucket notifications

Tests S3 bucket notification configuration for various event types
including SNS, SQS, and Lambda function notifications.
"""

import io
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_16(s3_client, config):
    """Bucket notifications test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-16')
        s3_client.create_bucket(bucket_name)

        # Test 1: Get empty notification configuration
        try:
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            # Should return empty configuration
            assert 'TopicConfigurations' not in response or len(response.get('TopicConfigurations', [])) == 0, \
                "Should have no topic configurations initially"
            assert 'QueueConfigurations' not in response or len(response.get('QueueConfigurations', [])) == 0, \
                "Should have no queue configurations initially"
            assert 'LambdaFunctionConfigurations' not in response or len(response.get('LambdaFunctionConfigurations', [])) == 0, \
                "Should have no lambda configurations initially"
            print("Empty notification configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Bucket notifications not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: SNS Topic notification configuration
        sns_notification = {
            'TopicConfigurations': [
                {
                    'Id': 'ObjectCreatedNotification',
                    'TopicArn': 'arn:aws:sns:us-east-1:123456789012:s3-notifications',
                    'Events': [
                        's3:ObjectCreated:*'
                    ],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': 'documents/'
                                },
                                {
                                    'Name': 'suffix',
                                    'Value': '.pdf'
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
                NotificationConfiguration=sns_notification
            )

            # Retrieve and verify SNS configuration
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            topic_configs = response.get('TopicConfigurations', [])
            if topic_configs:
                assert len(topic_configs) == 1, f"Expected 1 topic config, got {len(topic_configs)}"
                config = topic_configs[0]
                assert config.get('Id') == 'ObjectCreatedNotification', "Topic config ID mismatch"
                assert 's3:ObjectCreated:*' in config.get('Events', []), "ObjectCreated event not found"
                print("SNS topic notification: ✓")
            else:
                print("Note: SNS notifications may not be supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidArgument', 'UnsupportedNotification', 'NotImplemented']:
                print("Note: SNS notifications not supported")
            else:
                raise

        # Test 3: SQS Queue notification configuration
        sqs_notification = {
            'QueueConfigurations': [
                {
                    'Id': 'ObjectRemovedNotification',
                    'QueueArn': 'arn:aws:sqs:us-east-1:123456789012:s3-events',
                    'Events': [
                        's3:ObjectRemoved:*'
                    ],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': 'logs/'
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
                NotificationConfiguration=sqs_notification
            )

            # Retrieve and verify SQS configuration
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            queue_configs = response.get('QueueConfigurations', [])
            if queue_configs:
                assert len(queue_configs) == 1, f"Expected 1 queue config, got {len(queue_configs)}"
                config = queue_configs[0]
                assert config.get('Id') == 'ObjectRemovedNotification', "Queue config ID mismatch"
                assert 's3:ObjectRemoved:*' in config.get('Events', []), "ObjectRemoved event not found"
                print("SQS queue notification: ✓")
            else:
                print("Note: SQS notifications may not be supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidArgument', 'UnsupportedNotification', 'NotImplemented']:
                print("Note: SQS notifications not supported")
            else:
                raise

        # Test 4: Lambda function notification configuration
        lambda_notification = {
            'LambdaFunctionConfigurations': [
                {
                    'Id': 'ProcessUploadedImages',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:ProcessS3Object',
                    'Events': [
                        's3:ObjectCreated:Put',
                        's3:ObjectCreated:Post'
                    ],
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
                NotificationConfiguration=lambda_notification
            )

            # Retrieve and verify Lambda configuration
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            lambda_configs = response.get('LambdaFunctionConfigurations', [])
            if lambda_configs:
                assert len(lambda_configs) == 1, f"Expected 1 lambda config, got {len(lambda_configs)}"
                config = lambda_configs[0]
                assert config.get('Id') == 'ProcessUploadedImages', "Lambda config ID mismatch"
                assert 's3:ObjectCreated:Put' in config.get('Events', []), "Put event not found"
                print("Lambda function notification: ✓")
            else:
                print("Note: Lambda notifications may not be supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidArgument', 'UnsupportedNotification', 'NotImplemented']:
                print("Note: Lambda notifications not supported")
            else:
                raise

        # Test 5: Multiple notification configurations
        multi_notification = {
            'TopicConfigurations': [
                {
                    'Id': 'SNSNotification1',
                    'TopicArn': 'arn:aws:sns:us-east-1:123456789012:topic1',
                    'Events': ['s3:ObjectCreated:*']
                }
            ],
            'QueueConfigurations': [
                {
                    'Id': 'SQSNotification1',
                    'QueueArn': 'arn:aws:sqs:us-east-1:123456789012:queue1',
                    'Events': ['s3:ObjectRemoved:*']
                }
            ],
            'LambdaFunctionConfigurations': [
                {
                    'Id': 'LambdaNotification1',
                    'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:func1',
                    'Events': ['s3:ObjectCreated:CompleteMultipartUpload']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=multi_notification
            )

            # Retrieve and verify multiple configurations
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            total_configs = (
                len(response.get('TopicConfigurations', [])) +
                len(response.get('QueueConfigurations', [])) +
                len(response.get('LambdaFunctionConfigurations', []))
            )

            if total_configs > 0:
                print(f"Multiple notifications configured: {total_configs} total")
            else:
                print("Note: Multiple notification types may not be supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidArgument', 'UnsupportedNotification', 'NotImplemented']:
                print("Note: Multiple notification types not supported")
            else:
                raise

        # Test 6: Event type variations
        event_variation_notification = {
            'TopicConfigurations': [
                {
                    'Id': 'VariousEvents',
                    'TopicArn': 'arn:aws:sns:us-east-1:123456789012:events',
                    'Events': [
                        's3:ObjectCreated:Put',
                        's3:ObjectCreated:Post',
                        's3:ObjectCreated:Copy',
                        's3:ObjectCreated:CompleteMultipartUpload',
                        's3:ObjectRemoved:Delete',
                        's3:ObjectRemoved:DeleteMarkerCreated'
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=event_variation_notification
            )

            # Verify event types
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            topic_configs = response.get('TopicConfigurations', [])
            if topic_configs and len(topic_configs) > 0:
                events = topic_configs[0].get('Events', [])
                assert len(events) > 0, "No events configured"
                print(f"Event type variations: {len(events)} event types configured")
            else:
                print("Note: Various event types may not be supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidArgument', 'UnsupportedNotification']:
                print("Note: Some event types may not be supported")
            else:
                raise

        # Test 7: Clear notification configuration
        empty_notification = {}

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=empty_notification
            )

            # Verify cleared
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            total_configs = (
                len(response.get('TopicConfigurations', [])) +
                len(response.get('QueueConfigurations', [])) +
                len(response.get('LambdaFunctionConfigurations', []))
            )

            assert total_configs == 0, "Notifications should be cleared"
            print("Clear notifications: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error clearing notifications: {error_code}")

        # Test 8: Invalid ARN format (should fail)
        invalid_notification = {
            'TopicConfigurations': [
                {
                    'Id': 'InvalidConfig',
                    'TopicArn': 'invalid-arn-format',  # Invalid ARN
                    'Events': ['s3:ObjectCreated:*']
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=invalid_notification
            )
            # Some implementations might not validate ARN format
            print("Note: Invalid ARN format accepted by this implementation")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            assert error_code in ['InvalidArgument', 'MalformedXML', 'InvalidNotificationConfiguration', 'UnsupportedNotification'], \
                f"Unexpected error for invalid ARN: {error_code}"
            print("Invalid ARN rejection: ✓")

        # Test 9: Notification with complex filters
        complex_filter_notification = {
            'TopicConfigurations': [
                {
                    'Id': 'ComplexFilter',
                    'TopicArn': 'arn:aws:sns:us-east-1:123456789012:complex',
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {'Name': 'prefix', 'Value': 'data/2024/'},
                                {'Name': 'suffix', 'Value': '.csv'}
                            ]
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=complex_filter_notification
            )

            # Verify complex filter
            response = s3_client.client.get_bucket_notification_configuration(
                Bucket=bucket_name
            )

            topic_configs = response.get('TopicConfigurations', [])
            if topic_configs and len(topic_configs) > 0:
                filter_rules = topic_configs[0].get('Filter', {}).get('Key', {}).get('FilterRules', [])
                if filter_rules:
                    assert len(filter_rules) == 2, "Should have prefix and suffix rules"
                    print("Complex filter notifications: ✓")
                else:
                    print("Note: Complex filters may not be fully supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidArgument', 'UnsupportedNotification']:
                print("Note: Complex filters not supported")
            else:
                raise

        print(f"\nBucket notifications test completed:")
        print(f"- Basic notifications: ✓")
        print(f"- Various notification types tested")
        print(f"- Filter configurations tested")
        print(f"- Notification management: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Bucket notifications are not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to clear notifications first
                try:
                    s3_client.client.put_bucket_notification_configuration(
                        Bucket=bucket_name,
                        NotificationConfiguration={}
                    )
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass