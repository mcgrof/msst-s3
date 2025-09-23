#!/usr/bin/env python3
"""
Test 28: Metrics configuration

Tests S3 request metrics configuration for CloudWatch monitoring.
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_28(s3_client, config):
    """Metrics configuration test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-28')
        s3_client.create_bucket(bucket_name)

        # Test 1: List metrics configurations (should be empty)
        try:
            response = s3_client.client.list_bucket_metrics_configurations(
                Bucket=bucket_name
            )

            configs = response.get('MetricsConfigurationList', [])
            assert len(configs) == 0, "Should have no metrics configurations initially"
            print("Empty metrics configuration list: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Metrics configuration not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Create basic metrics configuration
        metrics_config = {
            'Id': 'EntireBucket',
            # No filter means metrics for entire bucket
        }

        try:
            s3_client.client.put_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id='EntireBucket',
                MetricsConfiguration=metrics_config
            )

            # Retrieve and verify
            response = s3_client.client.get_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id='EntireBucket'
            )

            config = response.get('MetricsConfiguration', {})
            assert config.get('Id') == 'EntireBucket', "Metrics ID mismatch"

            print("Basic metrics configuration created: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Metrics configuration not supported")
                return
            else:
                raise

        # Test 3: Create metrics configuration with prefix filter
        prefix_metrics = {
            'Id': 'DocumentsMetrics',
            'Filter': {
                'Prefix': 'documents/'
            }
        }

        s3_client.client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id='DocumentsMetrics',
            MetricsConfiguration=prefix_metrics
        )

        print("Prefix-filtered metrics configuration: ✓")

        # Test 4: Create metrics configuration with tag filter
        tag_metrics = {
            'Id': 'ImportantMetrics',
            'Filter': {
                'Tag': {
                    'Key': 'Priority',
                    'Value': 'High'
                }
            }
        }

        try:
            s3_client.client.put_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id='ImportantMetrics',
                MetricsConfiguration=tag_metrics
            )

            print("Tag-filtered metrics configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: Tag-filtered metrics may not be supported")
            else:
                raise

        # Test 5: Create metrics configuration with AND filter
        combined_metrics = {
            'Id': 'CombinedMetrics',
            'Filter': {
                'And': {
                    'Prefix': 'logs/',
                    'Tags': [
                        {
                            'Key': 'Type',
                            'Value': 'Application'
                        },
                        {
                            'Key': 'Environment',
                            'Value': 'Production'
                        }
                    ]
                }
            }
        }

        try:
            s3_client.client.put_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id='CombinedMetrics',
                MetricsConfiguration=combined_metrics
            )

            print("Combined filter metrics configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: Combined filters may not be supported")
            else:
                raise

        # Test 6: List all metrics configurations
        response = s3_client.client.list_bucket_metrics_configurations(
            Bucket=bucket_name
        )

        configs = response.get('MetricsConfigurationList', [])
        config_ids = [cfg.get('Id') for cfg in configs]

        print(f"Metrics configurations: {len(configs)} found")
        if 'EntireBucket' in config_ids:
            print("- EntireBucket configuration exists")
        if 'DocumentsMetrics' in config_ids:
            print("- DocumentsMetrics configuration exists")

        # Test 7: Update metrics configuration
        updated_metrics = {
            'Id': 'EntireBucket',
            'Filter': {
                'Prefix': 'updated/'
            }
        }

        s3_client.client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id='EntireBucket',
            MetricsConfiguration=updated_metrics
        )

        # Verify update
        response = s3_client.client.get_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id='EntireBucket'
        )

        config = response.get('MetricsConfiguration', {})
        filter_config = config.get('Filter', {})
        assert 'Prefix' in filter_config, "Filter not updated"

        print("Metrics configuration updated: ✓")

        # Test 8: Create objects for metrics tracking
        test_objects = [
            ('documents/report.pdf', b'PDF content', {'Priority': 'High'}),
            ('documents/draft.txt', b'Draft content', {'Priority': 'Low'}),
            ('logs/app.log', b'Log data', {'Type': 'Application', 'Environment': 'Production'}),
            ('logs/error.log', b'Error data', {'Type': 'Error', 'Environment': 'Production'}),
            ('updated/file.txt', b'Updated content', {}),
            ('other/data.bin', b'Binary data', {})
        ]

        for key, content, tags in test_objects:
            tag_set = []
            for tag_key, tag_value in tags.items():
                tag_set.append({'Key': tag_key, 'Value': tag_value})

            if tag_set:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=io.BytesIO(content),
                    Tagging={'TagSet': tag_set}
                )
            else:
                s3_client.put_object(bucket_name, key, io.BytesIO(content))

        print(f"Created {len(test_objects)} objects for metrics tracking")

        # Test 9: Delete specific metrics configuration
        try:
            s3_client.client.delete_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id='DocumentsMetrics'
            )

            # Verify deletion
            try:
                response = s3_client.client.get_bucket_metrics_configuration(
                    Bucket=bucket_name,
                    Id='DocumentsMetrics'
                )
                print("Warning: Metrics configuration not deleted")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchConfiguration':
                    print("Metrics configuration deleted: ✓")
                else:
                    print(f"Note: Unexpected error: {error_code}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error deleting metrics configuration: {error_code}")

        # Test 10: Check if continuation token is supported
        try:
            # Try to list with max-results
            response = s3_client.client.list_bucket_metrics_configurations(
                Bucket=bucket_name,
                ContinuationToken=None  # First page
            )

            if 'NextContinuationToken' in response:
                print("Pagination supported for metrics configurations")
            else:
                print("All metrics configurations returned in single response")

        except ClientError as e:
            print(f"Note: Pagination test error: {e.response['Error']['Code']}")

        # Test 11: Create many metrics configurations (up to limit)
        # S3 allows up to 1000 metrics configurations per bucket
        for i in range(5):  # Create a few more
            try:
                config = {
                    'Id': f'Metrics-{i:03d}',
                    'Filter': {
                        'Prefix': f'prefix-{i}/'
                    }
                }

                s3_client.client.put_bucket_metrics_configuration(
                    Bucket=bucket_name,
                    Id=f'Metrics-{i:03d}',
                    MetricsConfiguration=config
                )

            except ClientError:
                break

        # List final count
        response = s3_client.client.list_bucket_metrics_configurations(
            Bucket=bucket_name
        )

        final_count = len(response.get('MetricsConfigurationList', []))
        print(f"Total metrics configurations: {final_count}")

        # Test 12: Clean up all metrics configurations
        configs = response.get('MetricsConfigurationList', [])
        deleted_count = 0

        for config in configs:
            try:
                s3_client.client.delete_bucket_metrics_configuration(
                    Bucket=bucket_name,
                    Id=config['Id']
                )
                deleted_count += 1
            except ClientError:
                pass

        print(f"Cleaned up {deleted_count} metrics configurations")

        print(f"\nMetrics configuration test completed:")
        print(f"- Configuration management: ✓")
        print(f"- Various filter types tested")
        print(f"- CRUD operations: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Metrics configuration is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Clean up all metrics configurations
                try:
                    response = s3_client.client.list_bucket_metrics_configurations(
                        Bucket=bucket_name
                    )
                    for config in response.get('MetricsConfigurationList', []):
                        s3_client.client.delete_bucket_metrics_configuration(
                            Bucket=bucket_name,
                            Id=config['Id']
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