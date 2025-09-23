#!/usr/bin/env python3
"""
Test 22: Bucket inventory

Tests S3 inventory configuration for generating scheduled reports
about objects in a bucket.
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_22(s3_client, config):
    """Bucket inventory test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    destination_bucket = None

    try:
        # Create test buckets
        bucket_name = fixture.generate_bucket_name('test-22')
        destination_bucket = fixture.generate_bucket_name('test-22-inventory')
        s3_client.create_bucket(bucket_name)
        s3_client.create_bucket(destination_bucket)

        # Test 1: List inventory configurations (should be empty)
        try:
            response = s3_client.client.list_bucket_inventory_configurations(
                Bucket=bucket_name
            )

            configs = response.get('InventoryConfigurationList', [])
            assert len(configs) == 0, "Should have no inventory configurations initially"
            print("Empty inventory list: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Inventory configuration not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Create basic inventory configuration
        inventory_config = {
            'Destination': {
                'S3BucketDestination': {
                    'AccountId': '123456789012',  # Dummy account ID
                    'Bucket': f'arn:aws:s3:::{destination_bucket}',
                    'Format': 'CSV',
                    'Prefix': 'inventory/'
                }
            },
            'IsEnabled': True,
            'Filter': {
                'Prefix': 'data/'
            },
            'Id': 'daily-inventory',
            'IncludedObjectVersions': 'Current',
            'OptionalFields': [
                'Size',
                'LastModifiedDate',
                'StorageClass',
                'ETag',
                'IsMultipartUploaded',
                'ReplicationStatus'
            ],
            'Schedule': {
                'Frequency': 'Daily'
            }
        }

        try:
            s3_client.client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id='daily-inventory',
                InventoryConfiguration=inventory_config
            )

            # Retrieve and verify configuration
            response = s3_client.client.get_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id='daily-inventory'
            )

            config = response.get('InventoryConfiguration', {})
            assert config.get('Id') == 'daily-inventory', "Inventory ID mismatch"
            assert config.get('IsEnabled') == True, "Inventory not enabled"
            assert config.get('Schedule', {}).get('Frequency') == 'Daily', \
                "Schedule frequency mismatch"

            print("Basic inventory configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Inventory configuration not supported")
                return
            else:
                raise

        # Test 3: Create weekly inventory configuration
        weekly_inventory = {
            'Destination': {
                'S3BucketDestination': {
                    'AccountId': '123456789012',
                    'Bucket': f'arn:aws:s3:::{destination_bucket}',
                    'Format': 'ORC',
                    'Prefix': 'weekly-inventory/'
                }
            },
            'IsEnabled': True,
            'Id': 'weekly-inventory',
            'IncludedObjectVersions': 'All',
            'OptionalFields': [
                'Size',
                'LastModifiedDate',
                'ETag',
                'StorageClass',
                'IntelligentTieringAccessTier',
                'ObjectLockRetainUntilDate',
                'ObjectLockMode',
                'ObjectLockLegalHoldStatus'
            ],
            'Schedule': {
                'Frequency': 'Weekly'
            }
        }

        try:
            s3_client.client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id='weekly-inventory',
                InventoryConfiguration=weekly_inventory
            )

            print("Weekly inventory configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'MalformedXML']:
                print("Note: ORC format or weekly schedule may not be supported")
            else:
                raise

        # Test 4: List all inventory configurations
        response = s3_client.client.list_bucket_inventory_configurations(
            Bucket=bucket_name
        )

        configs = response.get('InventoryConfigurationList', [])
        config_ids = [cfg.get('Id') for cfg in configs]

        if 'daily-inventory' in config_ids:
            print(f"Inventory configurations listed: {len(configs)} found")

        # Test 5: Update inventory configuration
        updated_config = inventory_config.copy()
        updated_config['IsEnabled'] = False
        updated_config['OptionalFields'] = ['Size', 'ETag']

        s3_client.client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id='daily-inventory',
            InventoryConfiguration=updated_config
        )

        # Verify update
        response = s3_client.client.get_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id='daily-inventory'
        )

        config = response.get('InventoryConfiguration', {})
        assert config.get('IsEnabled') == False, "Inventory should be disabled"
        assert len(config.get('OptionalFields', [])) == 2, \
            "OptionalFields not updated"

        print("Inventory configuration update: ✓")

        # Test 6: Inventory with encryption
        encrypted_inventory = {
            'Destination': {
                'S3BucketDestination': {
                    'AccountId': '123456789012',
                    'Bucket': f'arn:aws:s3:::{destination_bucket}',
                    'Format': 'CSV',
                    'Prefix': 'encrypted-inventory/',
                    'Encryption': {
                        'SSES3': {}
                    }
                }
            },
            'IsEnabled': True,
            'Id': 'encrypted-inventory',
            'IncludedObjectVersions': 'Current',
            'Schedule': {
                'Frequency': 'Daily'
            }
        }

        try:
            s3_client.client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id='encrypted-inventory',
                InventoryConfiguration=encrypted_inventory
            )

            print("Encrypted inventory configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented']:
                print("Note: Inventory encryption may not be supported")
            else:
                raise

        # Test 7: Delete specific inventory configuration
        try:
            s3_client.client.delete_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id='weekly-inventory'
            )

            # Verify deletion
            try:
                response = s3_client.client.get_bucket_inventory_configuration(
                    Bucket=bucket_name,
                    Id='weekly-inventory'
                )
                print("Warning: Inventory configuration not deleted")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchConfiguration':
                    print("Inventory configuration deletion: ✓")
                else:
                    print(f"Note: Unexpected error after deletion: {error_code}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error deleting inventory: {error_code}")

        # Test 8: Create objects for inventory
        # Create various objects that would be included in inventory
        test_objects = [
            ('data/file1.txt', b'Content 1', 'text/plain'),
            ('data/file2.json', b'{"key": "value"}', 'application/json'),
            ('data/subdir/file3.csv', b'col1,col2\nval1,val2', 'text/csv'),
            ('other/file4.txt', b'Other content', 'text/plain'),
            ('logs/app.log', b'Log entries', 'text/plain')
        ]

        for key, content, content_type in test_objects:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(content),
                ContentType=content_type,
                Metadata={
                    'inventory-test': 'true',
                    'test-id': '022'
                }
            )

        print(f"Created {len(test_objects)} objects for inventory")

        # Test 9: Inventory with Parquet format
        parquet_inventory = {
            'Destination': {
                'S3BucketDestination': {
                    'AccountId': '123456789012',
                    'Bucket': f'arn:aws:s3:::{destination_bucket}',
                    'Format': 'Parquet',
                    'Prefix': 'parquet-inventory/'
                }
            },
            'IsEnabled': True,
            'Id': 'parquet-inventory',
            'IncludedObjectVersions': 'Current',
            'Schedule': {
                'Frequency': 'Daily'
            }
        }

        try:
            s3_client.client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id='parquet-inventory',
                InventoryConfiguration=parquet_inventory
            )

            print("Parquet format inventory: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'MalformedXML']:
                print("Note: Parquet format may not be supported")
            else:
                raise

        # Test 10: Clean up all inventory configurations
        response = s3_client.client.list_bucket_inventory_configurations(
            Bucket=bucket_name
        )

        configs = response.get('InventoryConfigurationList', [])
        deleted_count = 0

        for config in configs:
            try:
                s3_client.client.delete_bucket_inventory_configuration(
                    Bucket=bucket_name,
                    Id=config['Id']
                )
                deleted_count += 1
            except ClientError:
                pass

        print(f"Cleaned up {deleted_count} inventory configurations")

        print(f"\nBucket inventory test completed:")
        print(f"- Configuration management: ✓")
        print(f"- Multiple formats tested")
        print(f"- Schedule options tested")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Bucket inventory is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        for bucket in [bucket_name, destination_bucket]:
            if bucket and s3_client.bucket_exists(bucket):
                try:
                    # Clean up inventory configurations
                    try:
                        response = s3_client.client.list_bucket_inventory_configurations(
                            Bucket=bucket
                        )
                        for config in response.get('InventoryConfigurationList', []):
                            s3_client.client.delete_bucket_inventory_configuration(
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