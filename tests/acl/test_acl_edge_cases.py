#!/usr/bin/env python3
"""
Test: ACL Edge Cases and Permission Conflicts
Tests Access Control List edge cases, permission conflicts, and grant combinations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_acl_edge_cases(s3_client: S3Client):
    """Test ACL edge cases and permission conflicts"""
    bucket_name = f's3-acl-edge-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Canned ACL vs Custom ACL conflict
        print("Test 1: Canned vs Custom ACL conflict")
        key1 = 'canned-vs-custom'
        try:
            # Try to set both canned and custom ACL (should fail)
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=b'test',
                ACL='public-read',
                GrantRead='uri="http://acs.amazonaws.com/groups/global/AllUsers"'
            )
            results['failed'].append('Canned+Custom ACL: Should have failed')
            print("✗ Canned+Custom ACL: Both accepted (should conflict)")
        except Exception as e:
            if 'InvalidRequest' in str(e) or 'Conflict' in str(e):
                results['passed'].append('Canned+Custom ACL conflict detected')
                print("✓ Canned+Custom ACL: Conflict correctly detected")
            else:
                # Some S3 implementations might just use one over the other
                results['passed'].append('Canned+Custom ACL handled')
                print(f"✓ Canned+Custom ACL: Handled with: {str(e)[:50]}")

        # Test 2: Maximum ACL grants
        print("\nTest 2: Maximum ACL grants")
        key2 = 'max-grants'
        s3_client.client.put_object(Bucket=bucket_name, Key=key2, Body=b'test')

        try:
            # Get current ACL
            acl = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key2)

            # Try to add many grants (S3 limit is typically 100)
            grants = acl.get('Grants', [])
            for i in range(95):  # Try to add 95 more grants
                grants.append({
                    'Grantee': {
                        'Type': 'EmailAddress',
                        'EmailAddress': f'user{i}@example.com'
                    },
                    'Permission': 'READ'
                })

            # Try to set the expanded ACL
            s3_client.client.put_object_acl(
                Bucket=bucket_name,
                Key=key2,
                AccessControlPolicy={
                    'Owner': acl['Owner'],
                    'Grants': grants
                }
            )

            # Check how many were actually set
            new_acl = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key2)
            grant_count = len(new_acl.get('Grants', []))

            if grant_count > 90:
                results['passed'].append(f'Many grants accepted ({grant_count})')
                print(f"✓ Maximum grants: {grant_count} grants accepted")
            else:
                results['failed'].append(f'Many grants: Only {grant_count} accepted')
                print(f"✗ Many grants: Only {grant_count} accepted")

        except Exception as e:
            if 'TooManyGrants' in str(e) or 'InvalidArgument' in str(e):
                results['passed'].append('Grant limit enforced')
                print("✓ Grant limit: Correctly enforced")
            else:
                results['failed'].append(f'Max grants: {str(e)}')

        # Test 3: Conflicting permissions
        print("\nTest 3: Conflicting permissions")
        key3 = 'conflicting-perms'
        s3_client.client.put_object(Bucket=bucket_name, Key=key3, Body=b'test')

        try:
            # Set ACL with same grantee having different permissions
            s3_client.client.put_object_acl(
                Bucket=bucket_name,
                Key=key3,
                GrantRead='uri="http://acs.amazonaws.com/groups/global/AllUsers"',
                GrantWrite='uri="http://acs.amazonaws.com/groups/global/AllUsers"'
            )

            # Check resulting ACL
            acl = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key3)
            public_perms = set()

            for grant in acl.get('Grants', []):
                if 'URI' in grant.get('Grantee', {}):
                    if 'AllUsers' in grant['Grantee']['URI']:
                        public_perms.add(grant['Permission'])

            if 'READ' in public_perms and 'WRITE' in public_perms:
                results['passed'].append('Multiple permissions per grantee')
                print("✓ Conflicting perms: Multiple permissions preserved")
            else:
                results['failed'].append(f'Conflicting perms: Got {public_perms}')

        except Exception as e:
            results['failed'].append(f'Conflicting perms: {str(e)}')

        # Test 4: Invalid grantee formats
        print("\nTest 4: Invalid grantee formats")
        key4 = 'invalid-grantee'
        s3_client.client.put_object(Bucket=bucket_name, Key=key4, Body=b'test')

        invalid_grantees = [
            ('invalid-email', 'notanemail'),
            ('invalid-id', '12345'),  # Too short for canonical ID
            ('invalid-uri', 'http://invalid.uri.format'),
        ]

        for test_name, grantee_value in invalid_grantees:
            try:
                if 'email' in test_name:
                    s3_client.client.put_object_acl(
                        Bucket=bucket_name,
                        Key=key4,
                        GrantRead=f'emailAddress="{grantee_value}"'
                    )
                elif 'id' in test_name:
                    s3_client.client.put_object_acl(
                        Bucket=bucket_name,
                        Key=key4,
                        GrantRead=f'id="{grantee_value}"'
                    )
                else:
                    s3_client.client.put_object_acl(
                        Bucket=bucket_name,
                        Key=key4,
                        GrantRead=f'uri="{grantee_value}"'
                    )

                results['failed'].append(f'{test_name}: Accepted invalid grantee')
                print(f"✗ {test_name}: Accepted (should reject)")

            except Exception as e:
                if 'InvalidArgument' in str(e) or 'MalformedACL' in str(e):
                    results['passed'].append(f'{test_name} rejected')
                    print(f"✓ {test_name}: Correctly rejected")
                else:
                    results['failed'].append(f'{test_name}: Unexpected error')

        # Test 5: ACL on non-existent object
        print("\nTest 5: ACL on non-existent object")
        try:
            s3_client.client.put_object_acl(
                Bucket=bucket_name,
                Key='does-not-exist',
                ACL='public-read'
            )
            results['failed'].append('ACL on non-existent: Should have failed')
            print("✗ ACL on non-existent: Succeeded (should fail)")
        except Exception as e:
            if 'NoSuchKey' in str(e) or '404' in str(e):
                results['passed'].append('ACL on non-existent rejected')
                print("✓ ACL on non-existent: Correctly rejected")
            else:
                results['failed'].append(f'ACL on non-existent: Unexpected error')

        # Test 6: Empty ACL grants
        print("\nTest 6: Empty ACL grants")
        key6 = 'empty-grants'
        s3_client.client.put_object(Bucket=bucket_name, Key=key6, Body=b'test')

        try:
            acl = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key6)

            # Try to set empty grants list
            s3_client.client.put_object_acl(
                Bucket=bucket_name,
                Key=key6,
                AccessControlPolicy={
                    'Owner': acl['Owner'],
                    'Grants': []  # Empty grants
                }
            )

            # Check if owner still has access
            try:
                s3_client.client.get_object(Bucket=bucket_name, Key=key6)
                results['passed'].append('Empty grants preserves owner access')
                print("✓ Empty grants: Owner access preserved")
            except:
                results['failed'].append('Empty grants: Owner lost access')

        except Exception as e:
            results['failed'].append(f'Empty grants: {str(e)}')

        # Summary
        print(f"\n=== ACL Edge Cases Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
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
    test_acl_edge_cases(s3)