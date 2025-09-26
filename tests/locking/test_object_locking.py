#!/usr/bin/env python3
"""
Test: Object Locking Compliance Features
Tests S3 Object Lock functionality including retention policies and legal holds.
This is a critical enterprise compliance feature for WORM (Write Once Read Many) requirements.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
from datetime import datetime, timezone, timedelta

def test_object_locking(s3_client: S3Client):
    """Test Object Lock compliance features"""
    bucket_name = f's3-object-lock-{random_string(8).lower()}'

    try:
        # Object Lock must be enabled at bucket creation time
        s3_client.client.create_bucket(
            Bucket=bucket_name,
            ObjectLockEnabledForBucket=True
        )

        results = {'passed': [], 'failed': []}

        # Test 1: Bucket Object Lock Configuration
        print("Test 1: Bucket Object Lock Configuration")
        try:
            # Set default retention configuration for bucket
            retention_config = {
                'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 30
                    }
                }
            }

            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration=retention_config
            )

            # Verify configuration
            config = s3_client.client.get_object_lock_configuration(Bucket=bucket_name)

            if (config['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled' and
                config['ObjectLockConfiguration']['Rule']['DefaultRetention']['Mode'] == 'GOVERNANCE'):
                results['passed'].append('Bucket lock configuration')
                print("✓ Bucket lock configuration: Set and retrieved successfully")
            else:
                results['failed'].append('Bucket lock configuration: Mismatch')

        except Exception as e:
            results['failed'].append(f'Bucket lock configuration: {str(e)}')
            print(f"✗ Bucket lock configuration: {str(e)}")

        # Test 2: Object Retention - GOVERNANCE Mode
        print("\nTest 2: Object Retention - GOVERNANCE Mode")
        governance_key = 'governance-retention-test'
        retain_until = datetime.now(timezone.utc) + timedelta(days=10)

        try:
            # Upload object with GOVERNANCE retention
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=governance_key,
                Body=b'governance retention test data',
                ObjectLockMode='GOVERNANCE',
                ObjectLockRetainUntilDate=retain_until
            )

            # Verify retention settings
            retention = s3_client.client.get_object_retention(
                Bucket=bucket_name,
                Key=governance_key
            )

            if (retention['Retention']['Mode'] == 'GOVERNANCE' and
                retention['Retention']['RetainUntilDate'].replace(microsecond=0) ==
                retain_until.replace(microsecond=0)):
                results['passed'].append('GOVERNANCE retention')
                print(f"✓ GOVERNANCE retention: Set until {retain_until}")
            else:
                results['failed'].append('GOVERNANCE retention: Settings mismatch')

            # Test deletion protection (should fail without bypass)
            try:
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=governance_key
                )
                results['failed'].append('GOVERNANCE protection: Delete succeeded (should fail)')
                print("✗ GOVERNANCE protection: Object deleted (should be protected)")
            except Exception as e:
                if 'AccessDenied' in str(e) or 'ObjectLocked' in str(e):
                    results['passed'].append('GOVERNANCE protection active')
                    print("✓ GOVERNANCE protection: Delete correctly blocked")
                else:
                    results['failed'].append(f'GOVERNANCE protection: Unexpected error: {e}')

            # Test bypass governance retention (with special header)
            try:
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=governance_key,
                    BypassGovernanceRetention=True
                )
                results['passed'].append('GOVERNANCE bypass')
                print("✓ GOVERNANCE bypass: Delete with bypass succeeded")
            except Exception as e:
                results['failed'].append(f'GOVERNANCE bypass: {str(e)}')

        except Exception as e:
            results['failed'].append(f'GOVERNANCE retention: {str(e)}')
            print(f"✗ GOVERNANCE retention: {str(e)}")

        # Test 3: Object Retention - COMPLIANCE Mode
        print("\nTest 3: Object Retention - COMPLIANCE Mode")
        compliance_key = 'compliance-retention-test'
        compliance_until = datetime.now(timezone.utc) + timedelta(days=5)

        try:
            # Upload object with COMPLIANCE retention
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=compliance_key,
                Body=b'compliance retention test data',
                ObjectLockMode='COMPLIANCE',
                ObjectLockRetainUntilDate=compliance_until
            )

            # Verify retention settings
            retention = s3_client.client.get_object_retention(
                Bucket=bucket_name,
                Key=compliance_key
            )

            if retention['Retention']['Mode'] == 'COMPLIANCE':
                results['passed'].append('COMPLIANCE retention')
                print(f"✓ COMPLIANCE retention: Set until {compliance_until}")

            # Test deletion protection (should fail even with bypass)
            try:
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=compliance_key,
                    BypassGovernanceRetention=True  # Should not work for COMPLIANCE
                )
                results['failed'].append('COMPLIANCE protection: Bypass worked (should fail)')
                print("✗ COMPLIANCE protection: Bypass worked (should be absolute)")
            except Exception as e:
                if 'AccessDenied' in str(e) or 'ObjectLocked' in str(e):
                    results['passed'].append('COMPLIANCE protection absolute')
                    print("✓ COMPLIANCE protection: Bypass correctly blocked")
                else:
                    results['failed'].append(f'COMPLIANCE protection: Unexpected error')

        except Exception as e:
            results['failed'].append(f'COMPLIANCE retention: {str(e)}')
            print(f"✗ COMPLIANCE retention: {str(e)}")

        # Test 4: Legal Hold
        print("\nTest 4: Legal Hold functionality")
        legal_hold_key = 'legal-hold-test'

        try:
            # Upload object
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=legal_hold_key,
                Body=b'legal hold test data'
            )

            # Apply legal hold
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=legal_hold_key,
                LegalHold={'Status': 'ON'}
            )

            # Verify legal hold
            legal_hold = s3_client.client.get_object_legal_hold(
                Bucket=bucket_name,
                Key=legal_hold_key
            )

            if legal_hold['LegalHold']['Status'] == 'ON':
                results['passed'].append('Legal hold application')
                print("✓ Legal hold: Applied successfully")

            # Test deletion protection with legal hold
            try:
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=legal_hold_key
                )
                results['failed'].append('Legal hold protection: Delete succeeded')
                print("✗ Legal hold protection: Object deleted (should be protected)")
            except Exception as e:
                if 'AccessDenied' in str(e) or 'ObjectLocked' in str(e):
                    results['passed'].append('Legal hold protection')
                    print("✓ Legal hold protection: Delete correctly blocked")

            # Remove legal hold
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=legal_hold_key,
                LegalHold={'Status': 'OFF'}
            )

            # Verify legal hold removal
            legal_hold = s3_client.client.get_object_legal_hold(
                Bucket=bucket_name,
                Key=legal_hold_key
            )

            if legal_hold['LegalHold']['Status'] == 'OFF':
                results['passed'].append('Legal hold removal')
                print("✓ Legal hold removal: Removed successfully")

                # Now deletion should work
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=legal_hold_key
                )
                results['passed'].append('Delete after legal hold removal')
                print("✓ Delete after legal hold removal: Succeeded")

        except Exception as e:
            results['failed'].append(f'Legal hold: {str(e)}')
            print(f"✗ Legal hold: {str(e)}")

        # Test 5: Combined Retention and Legal Hold
        print("\nTest 5: Combined retention and legal hold")
        combined_key = 'combined-protection-test'
        short_retention = datetime.now(timezone.utc) + timedelta(minutes=1)

        try:
            # Upload with both retention and legal hold
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=combined_key,
                Body=b'combined protection test',
                ObjectLockMode='GOVERNANCE',
                ObjectLockRetainUntilDate=short_retention,
                ObjectLockLegalHoldStatus='ON'
            )

            # Verify both protections are active
            retention = s3_client.client.get_object_retention(
                Bucket=bucket_name,
                Key=combined_key
            )
            legal_hold = s3_client.client.get_object_legal_hold(
                Bucket=bucket_name,
                Key=combined_key
            )

            if (retention['Retention']['Mode'] == 'GOVERNANCE' and
                legal_hold['LegalHold']['Status'] == 'ON'):
                results['passed'].append('Combined protections')
                print("✓ Combined protections: Both retention and legal hold active")

            # Test that legal hold prevents deletion even with retention bypass
            try:
                s3_client.client.delete_object(
                    Bucket=bucket_name,
                    Key=combined_key,
                    BypassGovernanceRetention=True
                )
                results['failed'].append('Combined protection: Delete succeeded')
            except Exception as e:
                if 'AccessDenied' in str(e) or 'ObjectLocked' in str(e):
                    results['passed'].append('Combined protection precedence')
                    print("✓ Combined protection: Legal hold takes precedence")

        except Exception as e:
            results['failed'].append(f'Combined protection: {str(e)}')
            print(f"✗ Combined protection: {str(e)}")

        # Test 6: Invalid retention scenarios
        print("\nTest 6: Invalid retention scenarios")

        # Test past date retention
        try:
            past_date = datetime.now(timezone.utc) - timedelta(days=1)
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='invalid-past-date',
                Body=b'test',
                ObjectLockMode='GOVERNANCE',
                ObjectLockRetainUntilDate=past_date
            )
            results['failed'].append('Past date retention: Should have failed')
            print("✗ Past date retention: Accepted (should reject)")
        except Exception as e:
            if 'InvalidRequest' in str(e) or 'PastObjectLockRetainDate' in str(e):
                results['passed'].append('Past date retention rejected')
                print("✓ Past date retention: Correctly rejected")
            else:
                results['failed'].append(f'Past date retention: Wrong error: {e}')

        # Test invalid mode
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='invalid-mode',
                Body=b'test',
                ObjectLockMode='INVALID_MODE',
                ObjectLockRetainUntilDate=datetime.now(timezone.utc) + timedelta(days=1)
            )
            results['failed'].append('Invalid mode: Should have failed')
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'MalformedXML' in str(e):
                results['passed'].append('Invalid mode rejected')
                print("✓ Invalid mode: Correctly rejected")

        # Test 7: Extend retention period
        print("\nTest 7: Extend retention period")
        extend_key = 'extend-retention-test'
        initial_date = datetime.now(timezone.utc) + timedelta(days=5)
        extended_date = datetime.now(timezone.utc) + timedelta(days=10)

        try:
            # Create object with initial retention
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=extend_key,
                Body=b'extend test',
                ObjectLockMode='GOVERNANCE',
                ObjectLockRetainUntilDate=initial_date
            )

            # Extend retention period
            s3_client.client.put_object_retention(
                Bucket=bucket_name,
                Key=extend_key,
                Retention={
                    'Mode': 'GOVERNANCE',
                    'RetainUntilDate': extended_date
                }
            )

            # Verify extension
            retention = s3_client.client.get_object_retention(
                Bucket=bucket_name,
                Key=extend_key
            )

            if retention['Retention']['RetainUntilDate'].replace(microsecond=0) == extended_date.replace(microsecond=0):
                results['passed'].append('Retention period extension')
                print("✓ Retention extension: Period successfully extended")
            else:
                results['failed'].append('Retention extension: Date not updated')

        except Exception as e:
            results['failed'].append(f'Retention extension: {str(e)}')

        # Summary
        print(f"\n=== Object Locking Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

        return len(results['failed']) == 0

    except Exception as e:
        print(f"Critical error in object locking test setup: {str(e)}")
        return False

    finally:
        # Cleanup - Object Lock buckets require special handling
        try:
            # Must remove all object versions and delete markers first
            try:
                versions = s3_client.client.list_object_versions(Bucket=bucket_name)

                # Delete all object versions (with bypass for governance retention)
                if 'Versions' in versions:
                    for version in versions['Versions']:
                        try:
                            s3_client.client.delete_object(
                                Bucket=bucket_name,
                                Key=version['Key'],
                                VersionId=version['VersionId'],
                                BypassGovernanceRetention=True
                            )
                        except:
                            pass  # Some may fail due to COMPLIANCE mode

                # Delete all delete markers
                if 'DeleteMarkers' in versions:
                    for marker in versions['DeleteMarkers']:
                        try:
                            s3_client.client.delete_object(
                                Bucket=bucket_name,
                                Key=marker['Key'],
                                VersionId=marker['VersionId']
                            )
                        except:
                            pass

            except:
                pass

            # Note: Object Lock buckets cannot be deleted until all retention periods expire
            # In a real test environment, you might need to wait or use lifecycle policies
            print(f"\nNote: Object Lock bucket '{bucket_name}' cannot be automatically deleted")
            print("All retention periods must expire before bucket deletion is possible")

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
    test_object_locking(s3)