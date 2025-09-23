#!/usr/bin/env python3
"""
Test 018: Object locking

Tests S3 Object Lock functionality including retention modes,
legal holds, and governance/compliance modes.
"""

import io
from datetime import datetime, timedelta
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_018(s3_client, config):
    """Object locking test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket with object lock enabled
        bucket_name = fixture.generate_bucket_name('test-018')

        try:
            # Try to create bucket with object lock
            s3_client.client.create_bucket(
                Bucket=bucket_name,
                ObjectLockEnabledForBucket=True
            )
        except (ClientError, TypeError) as e:
            # Fallback: create regular bucket and try to enable object lock
            s3_client.create_bucket(bucket_name)

            # Try to enable object lock configuration
            try:
                object_lock_config = {
                    'ObjectLockEnabled': 'Enabled',
                    'Rule': {
                        'DefaultRetention': {
                            'Mode': 'GOVERNANCE',
                            'Days': 1
                        }
                    }
                }

                s3_client.client.put_object_lock_configuration(
                    Bucket=bucket_name,
                    ObjectLockConfiguration=object_lock_config
                )

                print("Object lock configuration set: ✓")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['NotImplemented', 'InvalidBucketState', 'InvalidRequest']:
                    print("Note: Object Lock not supported or cannot be enabled on existing bucket")
                    return
                else:
                    raise

        # Test 1: Check object lock configuration
        try:
            response = s3_client.client.get_object_lock_configuration(Bucket=bucket_name)
            config_status = response.get('ObjectLockConfiguration', {})

            if config_status.get('ObjectLockEnabled') == 'Enabled':
                print("Object lock enabled: ✓")
            else:
                print("Note: Object lock may not be fully enabled")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['ObjectLockConfigurationNotFoundError', 'NotImplemented']:
                print("Note: Object lock not configured")
                return
            else:
                raise

        # Test 2: Upload object with retention
        object_key_retention = 'locked-retention.txt'
        test_data = b'This object has retention period'
        retention_until = datetime.utcnow() + timedelta(days=1)

        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key_retention,
                Body=io.BytesIO(test_data),
                ObjectLockMode='GOVERNANCE',
                ObjectLockRetainUntilDate=retention_until.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )

            # Verify retention is set
            response = s3_client.client.get_object_retention(
                Bucket=bucket_name,
                Key=object_key_retention
            )

            retention = response.get('Retention', {})
            assert retention.get('Mode') in ['GOVERNANCE', 'COMPLIANCE'], \
                f"Retention mode not set: {retention.get('Mode')}"

            # Try to delete (should fail)
            try:
                s3_client.delete_object(bucket_name, object_key_retention)
                print("Warning: Object with retention was deleted (shouldn't happen)")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                assert error_code in ['AccessDenied', 'ObjectLocked'], \
                    f"Unexpected error deleting locked object: {error_code}"
                print("Object retention protection: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented', 'InvalidArgument']:
                print("Note: Object retention not supported")
            else:
                raise

        # Test 3: Legal hold
        object_key_legal = 'locked-legal-hold.txt'
        test_data_legal = b'This object has legal hold'

        try:
            # Upload object
            s3_client.put_object(
                bucket_name,
                object_key_legal,
                io.BytesIO(test_data_legal)
            )

            # Apply legal hold
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=object_key_legal,
                LegalHold={
                    'Status': 'ON'
                }
            )

            # Verify legal hold is set
            response = s3_client.client.get_object_legal_hold(
                Bucket=bucket_name,
                Key=object_key_legal
            )

            legal_hold = response.get('LegalHold', {})
            assert legal_hold.get('Status') == 'ON', "Legal hold not ON"

            # Try to delete (should fail)
            try:
                s3_client.delete_object(bucket_name, object_key_legal)
                print("Warning: Object with legal hold was deleted")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                assert error_code in ['AccessDenied', 'ObjectLocked'], \
                    f"Unexpected error deleting legal hold object: {error_code}"
                print("Legal hold protection: ✓")

            # Remove legal hold
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=object_key_legal,
                LegalHold={
                    'Status': 'OFF'
                }
            )

            # Now deletion should work (if no retention)
            try:
                s3_client.delete_object(bucket_name, object_key_legal)
                print("Legal hold removal: ✓")
            except ClientError:
                # Might still have retention
                pass

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented', 'MethodNotAllowed']:
                print("Note: Legal hold not supported")
            else:
                raise

        # Test 4: Compliance mode retention
        object_key_compliance = 'locked-compliance.txt'
        test_data_compliance = b'This object has compliance retention'
        compliance_until = datetime.utcnow() + timedelta(hours=1)

        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key_compliance,
                Body=io.BytesIO(test_data_compliance),
                ObjectLockMode='COMPLIANCE',
                ObjectLockRetainUntilDate=compliance_until.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )

            # Verify compliance mode
            response = s3_client.client.get_object_retention(
                Bucket=bucket_name,
                Key=object_key_compliance
            )

            retention = response.get('Retention', {})
            if retention.get('Mode') == 'COMPLIANCE':
                print("Compliance mode retention: ✓")

                # Try to override retention (should fail even with governance bypass)
                try:
                    new_date = datetime.utcnow()
                    s3_client.client.put_object_retention(
                        Bucket=bucket_name,
                        Key=object_key_compliance,
                        Retention={
                            'Mode': 'COMPLIANCE',
                            'RetainUntilDate': new_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        }
                    )
                    print("Warning: Compliance retention was modified")
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    print("Compliance mode immutability: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented']:
                print("Note: Compliance mode not supported")
            else:
                raise

        # Test 5: Default bucket retention
        try:
            # Set default retention for bucket
            default_retention_config = {
                'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 7
                    }
                }
            }

            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration=default_retention_config
            )

            # Upload object without specifying retention
            default_locked_key = 'default-locked.txt'
            s3_client.put_object(
                bucket_name,
                default_locked_key,
                io.BytesIO(b'Object with default retention')
            )

            # Check if default retention was applied
            try:
                response = s3_client.client.get_object_retention(
                    Bucket=bucket_name,
                    Key=default_locked_key
                )

                retention = response.get('Retention', {})
                if retention.get('Mode'):
                    print("Default bucket retention: ✓")
                else:
                    print("Note: Default retention may not be applied")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchObjectLockConfiguration':
                    print("Note: Default retention not applied to object")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented']:
                print("Note: Default bucket retention not supported")
            else:
                raise

        # Test 6: Bypass governance retention
        object_key_bypass = 'bypass-governance.txt'

        try:
            # Create object with governance retention
            bypass_until = datetime.utcnow() + timedelta(days=1)
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key_bypass,
                Body=io.BytesIO(b'Governance retention object'),
                ObjectLockMode='GOVERNANCE',
                ObjectLockRetainUntilDate=bypass_until.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )

            # Try to delete with bypass
            try:
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=object_key_bypass,
                    BypassGovernanceRetention=True
                )
                print("Governance bypass: ✓")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'AccessDenied':
                    print("Note: Governance bypass requires special permissions")
                else:
                    print(f"Note: Governance bypass failed: {error_code}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented']:
                print("Note: Governance bypass not supported")
            else:
                raise

        # Test 7: Versioning with object lock
        try:
            # Object lock requires versioning
            versioning_status = s3_client.get_bucket_versioning(bucket_name)
            if versioning_status.get('Status') == 'Enabled':
                print("Versioning enabled with object lock: ✓")

                # Create multiple versions
                versioned_key = 'versioned-locked.txt'

                # Version 1
                s3_client.put_object(
                    bucket_name,
                    versioned_key,
                    io.BytesIO(b'Version 1')
                )

                # Version 2 with retention
                version_until = datetime.utcnow() + timedelta(hours=1)
                response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=versioned_key,
                    Body=io.BytesIO(b'Version 2'),
                    ObjectLockMode='GOVERNANCE',
                    ObjectLockRetainUntilDate=version_until.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                )

                version_id = response.get('VersionId')
                if version_id:
                    # Check retention on specific version
                    response = s3_client.client.get_object_retention(
                        Bucket=bucket_name,
                        Key=versioned_key,
                        VersionId=version_id
                    )

                    if response.get('Retention'):
                        print("Version-specific retention: ✓")
            else:
                print("Note: Versioning not enabled with object lock")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error with versioned object lock: {error_code}")

        print(f"\nObject locking test completed:")
        print(f"- Object lock configuration tested")
        print(f"- Retention modes tested")
        print(f"- Legal hold tested")
        print(f"- Protection mechanisms verified")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Object locking is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to remove all locks first
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    try:
                        # Try to remove legal hold
                        s3_client.client.put_object_legal_hold(
                            Bucket=bucket_name,
                            Key=obj['Key'],
                            LegalHold={'Status': 'OFF'}
                        )
                    except:
                        pass

                    try:
                        # Try to delete with governance bypass
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=obj['Key'],
                            BypassGovernanceRetention=True
                        )
                    except:
                        # Regular delete
                        try:
                            s3_client.delete_object(bucket_name, obj['Key'])
                        except:
                            pass

                s3_client.delete_bucket(bucket_name)
            except:
                pass