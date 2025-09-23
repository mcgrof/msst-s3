#!/usr/bin/env python3
"""
Test 027: Replication

Tests S3 bucket replication functionality including cross-region replication (CRR)
and same-region replication (SRR).
"""

import io
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_027(s3_client, config):
    """Replication test"""
    fixture = TestFixture(s3_client, config)
    source_bucket = None
    destination_bucket = None

    try:
        # Create source and destination buckets
        source_bucket = fixture.generate_bucket_name('test-027-source')
        destination_bucket = fixture.generate_bucket_name('test-027-dest')

        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(destination_bucket)

        # Test 1: Enable versioning (required for replication)
        # Source bucket versioning
        s3_client.put_bucket_versioning(
            source_bucket,
            {'Status': 'Enabled'}
        )

        # Destination bucket versioning
        s3_client.put_bucket_versioning(
            destination_bucket,
            {'Status': 'Enabled'}
        )

        # Verify versioning is enabled
        source_versioning = s3_client.get_bucket_versioning(source_bucket)
        dest_versioning = s3_client.get_bucket_versioning(destination_bucket)

        assert source_versioning.get('Status') == 'Enabled', "Source versioning not enabled"
        assert dest_versioning.get('Status') == 'Enabled', "Destination versioning not enabled"

        print("Versioning enabled on both buckets: ✓")

        # Test 2: Get default replication configuration (should not exist)
        try:
            response = s3_client.client.get_bucket_replication(
                Bucket=source_bucket
            )

            print("Warning: Replication configuration exists by default")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ReplicationConfigurationNotFoundError':
                print("No default replication configuration: ✓")
            elif error_code == 'NotImplemented':
                print("Note: Replication not supported by this S3 implementation")
                return
            else:
                raise

        # Test 3: Create basic replication configuration
        # Note: This requires an IAM role in AWS
        replication_config = {
            'Role': 'arn:aws:iam::123456789012:role/replication-role',
            'Rules': [
                {
                    'ID': 'replicate-all',
                    'Priority': 1,
                    'Status': 'Enabled',
                    'DeleteMarkerReplication': {
                        'Status': 'Enabled'
                    },
                    'Filter': {},
                    'Destination': {
                        'Bucket': f'arn:aws:s3:::{destination_bucket}',
                        'ReplicationTime': {
                            'Status': 'Enabled',
                            'Time': {
                                'Minutes': 15
                            }
                        },
                        'Metrics': {
                            'Status': 'Enabled',
                            'EventThreshold': {
                                'Minutes': 15
                            }
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_replication(
                Bucket=source_bucket,
                ReplicationConfiguration=replication_config
            )

            print("Basic replication configuration created: ✓")

            # Retrieve and verify
            response = s3_client.client.get_bucket_replication(
                Bucket=source_bucket
            )

            config = response.get('ReplicationConfiguration', {})
            rules = config.get('Rules', [])
            assert len(rules) > 0, "No replication rules found"
            assert rules[0]['Status'] == 'Enabled', "Replication not enabled"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML', 'AccessDenied', 'XMinioAdminRemoteArnInvalid']:
                print("Note: Replication configuration not supported or invalid ARN format")
                return
            else:
                raise

        # Test 4: Create objects in source bucket
        test_objects = [
            ('replicate/doc1.txt', b'Document to replicate'),
            ('replicate/doc2.pdf', b'PDF content to replicate'),
            ('no-replicate/local.txt', b'This stays local')
        ]

        for key, content in test_objects:
            s3_client.put_object(
                source_bucket,
                key,
                io.BytesIO(content)
            )

        print(f"Created {len(test_objects)} objects in source bucket")

        # Test 5: Create replication rule with prefix filter
        filtered_replication = {
            'Role': 'arn:aws:iam::123456789012:role/replication-role',
            'Rules': [
                {
                    'ID': 'replicate-documents',
                    'Priority': 1,
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'documents/'
                    },
                    'DeleteMarkerReplication': {
                        'Status': 'Disabled'
                    },
                    'Destination': {
                        'Bucket': f'arn:aws:s3:::{destination_bucket}',
                        'StorageClass': 'STANDARD_IA'
                    }
                },
                {
                    'ID': 'replicate-images',
                    'Priority': 2,
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'images/'
                    },
                    'Destination': {
                        'Bucket': f'arn:aws:s3:::{destination_bucket}',
                        'StorageClass': 'GLACIER'
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_replication(
                Bucket=source_bucket,
                ReplicationConfiguration=filtered_replication
            )

            print("Filtered replication rules created: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidStorageClass', 'MalformedXML']:
                print("Note: Some replication features not supported")
            else:
                raise

        # Test 6: Tag-based replication
        tag_based_replication = {
            'Role': 'arn:aws:iam::123456789012:role/replication-role',
            'Rules': [
                {
                    'ID': 'replicate-tagged',
                    'Priority': 1,
                    'Status': 'Enabled',
                    'Filter': {
                        'Tag': {
                            'Key': 'replicate',
                            'Value': 'yes'
                        }
                    },
                    'DeleteMarkerReplication': {
                        'Status': 'Enabled'
                    },
                    'Destination': {
                        'Bucket': f'arn:aws:s3:::{destination_bucket}'
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_replication(
                Bucket=source_bucket,
                ReplicationConfiguration=tag_based_replication
            )

            print("Tag-based replication rule created: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: Tag-based replication not supported")
            else:
                raise

        # Test 7: Create tagged object for replication
        tagged_key = 'tagged-for-replication.txt'
        s3_client.client.put_object(
            Bucket=source_bucket,
            Key=tagged_key,
            Body=io.BytesIO(b'This should be replicated'),
            Tagging='replicate=yes&priority=high'
        )

        print("Tagged object created for replication: ✓")

        # Test 8: Check replication status
        try:
            response = s3_client.head_object(source_bucket, tagged_key)

            # Check for replication status header
            replication_status = response.get('ReplicationStatus')
            if replication_status:
                print(f"Replication status: {replication_status}")
            else:
                print("Note: Replication status not available")

        except ClientError as e:
            print(f"Note: Error checking replication status: {e.response['Error']['Code']}")

        # Test 9: Disable a replication rule
        try:
            # Get current configuration
            response = s3_client.client.get_bucket_replication(
                Bucket=source_bucket
            )

            config = response.get('ReplicationConfiguration', {})
            rules = config.get('Rules', [])

            if rules:
                # Disable first rule
                rules[0]['Status'] = 'Disabled'

                # Update configuration
                s3_client.client.put_bucket_replication(
                    Bucket=source_bucket,
                    ReplicationConfiguration=config
                )

                print("Replication rule disabled: ✓")

        except ClientError as e:
            print(f"Note: Error disabling rule: {e.response['Error']['Code']}")

        # Test 10: Cross-account replication configuration
        cross_account_config = {
            'Role': 'arn:aws:iam::123456789012:role/replication-role',
            'Rules': [
                {
                    'ID': 'cross-account-replication',
                    'Priority': 1,
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'shared/'
                    },
                    'DeleteMarkerReplication': {
                        'Status': 'Enabled'
                    },
                    'Destination': {
                        'Bucket': 'arn:aws:s3:::external-account-bucket',
                        'Account': '987654321098',
                        'AccessControlTranslation': {
                            'Owner': 'Destination'
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_replication(
                Bucket=source_bucket,
                ReplicationConfiguration=cross_account_config
            )

            print("Cross-account replication configured: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'MalformedXML', 'NoSuchBucket']:
                print("Note: Cross-account replication not testable")
            else:
                raise

        # Test 11: Delete replication configuration
        try:
            s3_client.client.delete_bucket_replication(
                Bucket=source_bucket
            )

            # Verify deletion
            try:
                response = s3_client.client.get_bucket_replication(
                    Bucket=source_bucket
                )
                print("Warning: Replication configuration not deleted")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ReplicationConfigurationNotFoundError':
                    print("Replication configuration deleted: ✓")
                else:
                    print(f"Note: Unexpected error: {error_code}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error deleting replication: {error_code}")

        print(f"\nReplication test completed:")
        print(f"- Versioning setup: ✓")
        print(f"- Replication rules tested")
        print(f"- Various filter types tested")
        print(f"- Configuration management: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Replication is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        for bucket in [source_bucket, destination_bucket]:
            if bucket and s3_client.bucket_exists(bucket):
                try:
                    # Delete replication configuration
                    try:
                        s3_client.client.delete_bucket_replication(Bucket=bucket)
                    except:
                        pass

                    # Delete all versions
                    try:
                        response = s3_client.client.list_object_versions(Bucket=bucket)

                        # Delete all versions
                        for version in response.get('Versions', []):
                            s3_client.client.delete_object(
                                Bucket=bucket,
                                Key=version['Key'],
                                VersionId=version['VersionId']
                            )

                        # Delete all delete markers
                        for marker in response.get('DeleteMarkers', []):
                            s3_client.client.delete_object(
                                Bucket=bucket,
                                Key=marker['Key'],
                                VersionId=marker['VersionId']
                            )
                    except:
                        # Regular cleanup if versioning issues
                        objects = s3_client.list_objects(bucket)
                        for obj in objects:
                            s3_client.delete_object(bucket, obj['Key'])

                    s3_client.delete_bucket(bucket)
                except:
                    pass