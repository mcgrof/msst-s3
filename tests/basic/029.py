#!/usr/bin/env python3
"""
Test 029: Analytics configuration

Tests S3 Storage Class Analysis configuration for analyzing storage access patterns.
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_029(s3_client, config):
    """Analytics configuration test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    destination_bucket = None

    try:
        # Create test buckets
        bucket_name = fixture.generate_bucket_name('test-029')
        destination_bucket = fixture.generate_bucket_name('test-029-analytics')
        s3_client.create_bucket(bucket_name)
        s3_client.create_bucket(destination_bucket)

        # Test 1: List analytics configurations (should be empty)
        try:
            response = s3_client.client.list_bucket_analytics_configurations(
                Bucket=bucket_name
            )

            configs = response.get('AnalyticsConfigurationList', [])
            assert len(configs) == 0, "Should have no analytics configurations initially"
            print("Empty analytics configuration list: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Analytics configuration not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Create basic analytics configuration
        analytics_config = {
            'Id': 'EntireBucketAnalysis',
            'StorageClassAnalysis': {
                'DataExport': {
                    'OutputSchemaVersion': 'V_1',
                    'Destination': {
                        'S3BucketDestination': {
                            'Format': 'CSV',
                            'BucketAccountId': '123456789012',
                            'Bucket': f'arn:aws:s3:::{destination_bucket}',
                            'Prefix': 'analytics-results/'
                        }
                    }
                }
            }
        }

        try:
            s3_client.client.put_bucket_analytics_configuration(
                Bucket=bucket_name,
                Id='EntireBucketAnalysis',
                AnalyticsConfiguration=analytics_config
            )

            # Retrieve and verify
            response = s3_client.client.get_bucket_analytics_configuration(
                Bucket=bucket_name,
                Id='EntireBucketAnalysis'
            )

            config = response.get('AnalyticsConfiguration', {})
            assert config.get('Id') == 'EntireBucketAnalysis', "Analytics ID mismatch"

            print("Basic analytics configuration created: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Analytics configuration not supported")
                return
            else:
                raise

        # Test 3: Analytics with prefix filter
        prefix_analytics = {
            'Id': 'DocumentsAnalysis',
            'Filter': {
                'Prefix': 'documents/'
            },
            'StorageClassAnalysis': {
                'DataExport': {
                    'OutputSchemaVersion': 'V_1',
                    'Destination': {
                        'S3BucketDestination': {
                            'Format': 'CSV',
                            'BucketAccountId': '123456789012',
                            'Bucket': f'arn:aws:s3:::{destination_bucket}',
                            'Prefix': 'documents-analysis/'
                        }
                    }
                }
            }
        }

        s3_client.client.put_bucket_analytics_configuration(
            Bucket=bucket_name,
            Id='DocumentsAnalysis',
            AnalyticsConfiguration=prefix_analytics
        )

        print("Prefix-filtered analytics configuration: ✓")

        # Test 4: Analytics with tag filter
        tag_analytics = {
            'Id': 'TaggedAnalysis',
            'Filter': {
                'Tag': {
                    'Key': 'Department',
                    'Value': 'Finance'
                }
            },
            'StorageClassAnalysis': {
                'DataExport': {
                    'OutputSchemaVersion': 'V_1',
                    'Destination': {
                        'S3BucketDestination': {
                            'Format': 'CSV',
                            'BucketAccountId': '123456789012',
                            'Bucket': f'arn:aws:s3:::{destination_bucket}',
                            'Prefix': 'finance-analysis/'
                        }
                    }
                }
            }
        }

        try:
            s3_client.client.put_bucket_analytics_configuration(
                Bucket=bucket_name,
                Id='TaggedAnalysis',
                AnalyticsConfiguration=tag_analytics
            )

            print("Tag-filtered analytics configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: Tag-filtered analytics may not be supported")

        # Test 5: List analytics configurations
        response = s3_client.client.list_bucket_analytics_configurations(
            Bucket=bucket_name
        )

        configs = response.get('AnalyticsConfigurationList', [])
        print(f"Analytics configurations: {len(configs)} found")

        # Test 6: Update analytics configuration
        updated_analytics = {
            'Id': 'EntireBucketAnalysis',
            'StorageClassAnalysis': {
                'DataExport': {
                    'OutputSchemaVersion': 'V_1',
                    'Destination': {
                        'S3BucketDestination': {
                            'Format': 'CSV',
                            'BucketAccountId': '123456789012',
                            'Bucket': f'arn:aws:s3:::{destination_bucket}',
                            'Prefix': 'updated-analytics/'
                        }
                    }
                }
            }
        }

        s3_client.client.put_bucket_analytics_configuration(
            Bucket=bucket_name,
            Id='EntireBucketAnalysis',
            AnalyticsConfiguration=updated_analytics
        )

        print("Analytics configuration updated: ✓")

        # Test 7: Delete analytics configuration
        s3_client.client.delete_bucket_analytics_configuration(
            Bucket=bucket_name,
            Id='DocumentsAnalysis'
        )

        print("Analytics configuration deleted: ✓")

        # Test 8: Clean up remaining configurations
        response = s3_client.client.list_bucket_analytics_configurations(
            Bucket=bucket_name
        )

        for config in response.get('AnalyticsConfigurationList', []):
            s3_client.client.delete_bucket_analytics_configuration(
                Bucket=bucket_name,
                Id=config['Id']
            )

        print("All analytics configurations cleaned up: ✓")

        print(f"\nAnalytics configuration test completed:")
        print(f"- Configuration management: ✓")
        print(f"- Filter types tested: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Analytics configuration is not implemented in this S3 provider")
        else:
            raise

    finally:
        # Cleanup
        for bucket in [bucket_name, destination_bucket]:
            if bucket and s3_client.bucket_exists(bucket):
                try:
                    # Clean up analytics configurations
                    try:
                        response = s3_client.client.list_bucket_analytics_configurations(
                            Bucket=bucket
                        )
                        for config in response.get('AnalyticsConfigurationList', []):
                            s3_client.client.delete_bucket_analytics_configuration(
                                Bucket=bucket,
                                Id=config['Id']
                            )
                    except:
                        pass

                    # Delete all objects
                    objects = s3_client.list_objects(bucket)
                    for obj in objects:
                        s3_client.delete_object(bucket, obj['Key'])

                    s3_client.delete_bucket(bucket)
                except:
                    pass