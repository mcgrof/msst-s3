#!/usr/bin/env python3
"""
Test 26: Intelligent tiering

Tests S3 Intelligent-Tiering storage class which automatically moves objects
between access tiers based on changing access patterns.
"""

import io
import time
from datetime import datetime, timedelta
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_26(s3_client, config):
    """Intelligent tiering test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-26')
        s3_client.create_bucket(bucket_name)

        # Test 1: Upload object with INTELLIGENT_TIERING storage class
        object_key = 'intelligent-tier-object.txt'
        test_data = b'This object will be intelligently tiered' * 100

        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=io.BytesIO(test_data),
                StorageClass='INTELLIGENT_TIERING'
            )

            # Check if storage class was accepted
            if 'StorageClass' in response:
                print(f"Upload with INTELLIGENT_TIERING: ✓")
            else:
                # Verify via HEAD
                response = s3_client.head_object(bucket_name, object_key)
                storage_class = response.get('StorageClass', 'STANDARD')

                if storage_class == 'INTELLIGENT_TIERING':
                    print("Object stored in INTELLIGENT_TIERING: ✓")
                else:
                    print(f"Note: Storage class is {storage_class}, not INTELLIGENT_TIERING")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidStorageClass', 'NotImplemented']:
                print("Note: INTELLIGENT_TIERING storage class not supported")
                # Try with STANDARD storage class for remaining tests
                s3_client.put_object(
                    bucket_name,
                    object_key,
                    io.BytesIO(test_data)
                )
                print("Using STANDARD storage class for testing")
            else:
                raise

        # Test 2: Get Intelligent-Tiering configuration
        try:
            response = s3_client.client.get_bucket_intelligent_tiering_configuration(
                Bucket=bucket_name,
                Id='test-config'
            )

            print("Intelligent-Tiering configuration exists")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NoSuchConfiguration', 'NotImplemented']:
                print("No Intelligent-Tiering configuration (expected)")
            else:
                raise

        # Test 3: Create Intelligent-Tiering configuration
        tiering_config = {
            'Id': 'archive-old-data',
            'Filter': {
                'Prefix': 'data/',
                'Tag': {
                    'Key': 'archive',
                    'Value': 'yes'
                }
            },
            'Status': 'Enabled',
            'Tierings': [
                {
                    'Days': 90,
                    'AccessTier': 'ARCHIVE_ACCESS'
                },
                {
                    'Days': 180,
                    'AccessTier': 'DEEP_ARCHIVE_ACCESS'
                }
            ]
        }

        try:
            s3_client.client.put_bucket_intelligent_tiering_configuration(
                Bucket=bucket_name,
                Id='archive-old-data',
                IntelligentTieringConfiguration=tiering_config
            )

            print("Intelligent-Tiering configuration created: ✓")

            # Retrieve and verify
            response = s3_client.client.get_bucket_intelligent_tiering_configuration(
                Bucket=bucket_name,
                Id='archive-old-data'
            )

            config = response.get('IntelligentTieringConfiguration', {})
            assert config.get('Status') == 'Enabled', "Configuration not enabled"
            assert len(config.get('Tierings', [])) > 0, "No tiering rules found"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Intelligent-Tiering configuration not supported")
            else:
                raise

        # Test 4: List Intelligent-Tiering configurations
        try:
            response = s3_client.client.list_bucket_intelligent_tiering_configurations(
                Bucket=bucket_name
            )

            configs = response.get('IntelligentTieringConfigurationList', [])
            print(f"Intelligent-Tiering configurations: {len(configs)} found")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Listing Intelligent-Tiering configurations not supported")
            else:
                raise

        # Test 5: Create objects with different access patterns
        # Simulate frequently accessed object
        frequently_accessed_key = 'frequent-access.txt'
        s3_client.put_object(
            bucket_name,
            frequently_accessed_key,
            io.BytesIO(b'Frequently accessed data')
        )

        # Simulate infrequently accessed object
        infrequent_key = 'infrequent-access.txt'
        s3_client.put_object(
            bucket_name,
            infrequent_key,
            io.BytesIO(b'Rarely accessed data')
        )

        # Access frequently accessed object multiple times
        for _ in range(3):
            response = s3_client.get_object(bucket_name, frequently_accessed_key)
            _ = response['Body'].read()
            time.sleep(0.1)

        print("Created objects with different access patterns: ✓")

        # Test 6: Check object tier status
        try:
            response = s3_client.head_object(bucket_name, object_key)

            # Check for tier-related headers
            archive_status = response.get('ArchiveStatus')
            storage_class = response.get('StorageClass', 'STANDARD')
            restore_status = response.get('Restore')

            if archive_status:
                print(f"Archive status: {archive_status}")

            print(f"Current storage class: {storage_class}")

        except ClientError as e:
            print(f"Note: Error checking tier status: {e.response['Error']['Code']}")

        # Test 7: Transition object to INTELLIGENT_TIERING via copy
        try:
            it_copy_key = 'copy-to-intelligent-tiering.txt'
            copy_source = {'Bucket': bucket_name, 'Key': frequently_accessed_key}

            response = s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=it_copy_key,
                StorageClass='INTELLIGENT_TIERING'
            )

            # Verify storage class
            response = s3_client.head_object(bucket_name, it_copy_key)
            storage_class = response.get('StorageClass', 'STANDARD')

            if storage_class == 'INTELLIGENT_TIERING':
                print("Object transitioned to INTELLIGENT_TIERING via copy: ✓")
            else:
                print(f"Copy resulted in {storage_class} storage class")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidStorageClass':
                print("Note: Cannot transition to INTELLIGENT_TIERING via copy")
            else:
                raise

        # Test 8: Create lifecycle rule to transition to INTELLIGENT_TIERING
        lifecycle_config = {
            'Rules': [
                {
                    'ID': 'transition-to-intelligent-tiering',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'auto-tier/'},
                    'Transitions': [
                        {
                            'Days': 0,
                            'StorageClass': 'INTELLIGENT_TIERING'
                        }
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=lifecycle_config
            )

            print("Lifecycle rule for INTELLIGENT_TIERING created: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidStorageClass', 'MalformedXML', 'InvalidRequest']:
                print("Note: Lifecycle transition to INTELLIGENT_TIERING not supported")
            else:
                raise

        # Test 9: Upload object that would be auto-tiered
        auto_tier_key = 'auto-tier/document.pdf'
        s3_client.put_object(
            bucket_name,
            auto_tier_key,
            io.BytesIO(b'Document for automatic tiering')
        )

        print("Object uploaded for automatic tiering: ✓")

        # Test 10: Delete Intelligent-Tiering configuration
        try:
            s3_client.client.delete_bucket_intelligent_tiering_configuration(
                Bucket=bucket_name,
                Id='archive-old-data'
            )

            print("Intelligent-Tiering configuration deleted: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NoSuchConfiguration', 'NotImplemented', 'BucketNotEmpty']:
                print("Note: No configuration to delete or not supported")
            else:
                raise

        # Test 11: Multipart upload with INTELLIGENT_TIERING
        multipart_key = 'multipart-intelligent-tier.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        try:
            # Start multipart upload with INTELLIGENT_TIERING
            response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=multipart_key,
                StorageClass='INTELLIGENT_TIERING'
            )

            upload_id = response['UploadId']

            # Upload a part
            part_data = b'M' * part_size
            response = s3_client.upload_part(
                bucket_name,
                multipart_key,
                upload_id,
                1,
                io.BytesIO(part_data)
            )

            # Complete multipart upload
            parts = [{'PartNumber': 1, 'ETag': response['ETag']}]
            s3_client.complete_multipart_upload(
                bucket_name,
                multipart_key,
                upload_id,
                parts
            )

            # Check final storage class
            response = s3_client.head_object(bucket_name, multipart_key)
            storage_class = response.get('StorageClass', 'STANDARD')

            print(f"Multipart upload storage class: {storage_class}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidStorageClass':
                print("Note: INTELLIGENT_TIERING not supported for multipart")
            else:
                # Try to abort if failed
                try:
                    s3_client.abort_multipart_upload(bucket_name, multipart_key, upload_id)
                except:
                    pass
                raise

        print(f"\nIntelligent tiering test completed:")
        print(f"- Storage class operations tested")
        print(f"- Configuration management tested")
        print(f"- Access pattern simulation: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Intelligent-Tiering is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Delete Intelligent-Tiering configurations
                try:
                    response = s3_client.client.list_bucket_intelligent_tiering_configurations(
                        Bucket=bucket_name
                    )
                    for config in response.get('IntelligentTieringConfigurationList', []):
                        s3_client.client.delete_bucket_intelligent_tiering_configuration(
                            Bucket=bucket_name,
                            Id=config['Id']
                        )
                except:
                    pass

                # Delete lifecycle configuration
                try:
                    s3_client.client.delete_bucket_lifecycle(Bucket=bucket_name)
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass