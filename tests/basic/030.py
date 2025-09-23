#!/usr/bin/env python3
"""
Test 030: Logging configuration

Tests S3 server access logging configuration.
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_030(s3_client, config):
    """Logging configuration test"""
    fixture = TestFixture(s3_client, config)
    source_bucket = None
    log_bucket = None

    try:
        # Create buckets
        source_bucket = fixture.generate_bucket_name('test-030')
        log_bucket = fixture.generate_bucket_name('test-030-logs')
        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(log_bucket)

        # Test 1: Get default logging (should be disabled)
        try:
            response = s3_client.client.get_bucket_logging(Bucket=source_bucket)

            if 'LoggingEnabled' in response:
                print("Warning: Logging enabled by default")
            else:
                print("Logging disabled by default: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Logging not supported")
                return
            else:
                raise

        # Test 2: Enable logging
        logging_config = {
            'LoggingEnabled': {
                'TargetBucket': log_bucket,
                'TargetPrefix': 'access-logs/',
                'TargetGrants': []
            }
        }

        try:
            s3_client.client.put_bucket_logging(
                Bucket=source_bucket,
                BucketLoggingStatus=logging_config
            )

            # Verify
            response = s3_client.client.get_bucket_logging(Bucket=source_bucket)
            assert 'LoggingEnabled' in response, "Logging not enabled"
            print("Logging enabled: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidTargetBucketForLogging', 'MalformedXML']:
                print("Note: Logging configuration not supported")
                return
            else:
                raise

        # Test 3: Update logging prefix
        logging_config['LoggingEnabled']['TargetPrefix'] = 'new-logs/'

        s3_client.client.put_bucket_logging(
            Bucket=source_bucket,
            BucketLoggingStatus=logging_config
        )

        response = s3_client.client.get_bucket_logging(Bucket=source_bucket)
        prefix = response.get('LoggingEnabled', {}).get('TargetPrefix')
        assert prefix == 'new-logs/', "Prefix not updated"
        print("Logging configuration updated: ✓")

        # Test 4: Disable logging
        s3_client.client.put_bucket_logging(
            Bucket=source_bucket,
            BucketLoggingStatus={}
        )

        response = s3_client.client.get_bucket_logging(Bucket=source_bucket)
        assert 'LoggingEnabled' not in response, "Logging not disabled"
        print("Logging disabled: ✓")

        print(f"\nLogging configuration test completed: ✓")

    finally:
        # Cleanup
        for bucket in [source_bucket, log_bucket]:
            if bucket and s3_client.bucket_exists(bucket):
                try:
                    objects = s3_client.list_objects(bucket)
                    for obj in objects:
                        s3_client.delete_object(bucket, obj['Key'])
                    s3_client.delete_bucket(bucket)
                except:
                    pass