#!/usr/bin/env python3
"""
Test: Delete Markers and Versioning Edge Cases
Tests delete marker behavior, restoration, and version-specific operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import time

def test_delete_markers(s3_client: S3Client):
    """Test delete markers and version-specific operations"""
    bucket_name = f's3-delete-markers-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)

        # Enable versioning
        s3_client.client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )

        results = {'passed': [], 'failed': []}

        # Test 1: Basic delete marker creation
        print("Test 1: Basic delete marker creation")
        key1 = 'delete-marker-test'

        # Create object
        v1 = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key1,
            Body=b'version1'
        )
        version1_id = v1['VersionId']

        # Delete object (creates delete marker)
        delete_resp = s3_client.client.delete_object(
            Bucket=bucket_name,
            Key=key1
        )
        delete_marker_id = delete_resp.get('VersionId')

        # Try to GET the object (should fail)
        try:
            s3_client.client.get_object(Bucket=bucket_name, Key=key1)
            results['failed'].append('Delete marker: Object still accessible')
            print("✗ Delete marker: Object still accessible")
        except Exception as e:
            if 'NoSuchKey' in str(e) or '404' in str(e):
                results['passed'].append('Delete marker hides object')
                print("✓ Delete marker: Object correctly hidden")
            else:
                results['failed'].append(f'Delete marker: Unexpected error: {e}')

        # Test 2: Access object via version ID (bypass delete marker)
        print("\nTest 2: Access via version ID")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key1,
                VersionId=version1_id
            )
            content = response['Body'].read()
            if content == b'version1':
                results['passed'].append('Version access bypasses delete marker')
                print("✓ Version access: Bypasses delete marker")
            else:
                results['failed'].append('Version access: Wrong content')
        except Exception as e:
            results['failed'].append(f'Version access: {str(e)}')

        # Test 3: Delete the delete marker (restore object)
        print("\nTest 3: Delete the delete marker")
        try:
            s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=key1,
                VersionId=delete_marker_id
            )

            # Object should be accessible again
            response = s3_client.client.get_object(Bucket=bucket_name, Key=key1)
            content = response['Body'].read()
            if content == b'version1':
                results['passed'].append('Delete marker removal restores object')
                print("✓ Delete marker removal: Object restored")
            else:
                results['failed'].append('Delete marker removal: Wrong content')
        except Exception as e:
            results['failed'].append(f'Delete marker removal: {str(e)}')

        # Test 4: Multiple delete markers
        print("\nTest 4: Multiple delete markers")
        key2 = 'multiple-delete-markers'

        # Create and delete multiple times
        s3_client.client.put_object(Bucket=bucket_name, Key=key2, Body=b'v1')
        s3_client.client.delete_object(Bucket=bucket_name, Key=key2)  # Delete marker 1
        s3_client.client.put_object(Bucket=bucket_name, Key=key2, Body=b'v2')
        s3_client.client.delete_object(Bucket=bucket_name, Key=key2)  # Delete marker 2
        s3_client.client.put_object(Bucket=bucket_name, Key=key2, Body=b'v3')
        s3_client.client.delete_object(Bucket=bucket_name, Key=key2)  # Delete marker 3

        # List versions
        versions = s3_client.client.list_object_versions(
            Bucket=bucket_name,
            Prefix=key2
        )

        delete_marker_count = len(versions.get('DeleteMarkers', []))
        version_count = len(versions.get('Versions', []))

        if delete_marker_count == 3 and version_count == 3:
            results['passed'].append('Multiple delete markers tracked')
            print(f"✓ Multiple delete markers: {delete_marker_count} markers, {version_count} versions")
        else:
            results['failed'].append(f'Multiple delete markers: Expected 3/3, got {delete_marker_count}/{version_count}')
            print(f"✗ Multiple delete markers: Expected 3/3, got {delete_marker_count}/{version_count}")

        # Test 5: Delete specific version (not current)
        print("\nTest 5: Delete specific non-current version")
        key3 = 'specific-version-delete'

        # Create multiple versions
        v1_resp = s3_client.client.put_object(Bucket=bucket_name, Key=key3, Body=b'version1')
        v1_id = v1_resp['VersionId']
        v2_resp = s3_client.client.put_object(Bucket=bucket_name, Key=key3, Body=b'version2')
        v2_id = v2_resp['VersionId']
        v3_resp = s3_client.client.put_object(Bucket=bucket_name, Key=key3, Body=b'version3')
        v3_id = v3_resp['VersionId']

        # Delete middle version
        s3_client.client.delete_object(
            Bucket=bucket_name,
            Key=key3,
            VersionId=v2_id
        )

        # Verify v2 is gone but v1 and v3 remain
        try:
            s3_client.client.get_object(Bucket=bucket_name, Key=key3, VersionId=v2_id)
            results['failed'].append('Specific version delete: Version still exists')
        except:
            # Should fail - version deleted
            pass

        try:
            # v1 should still exist
            r1 = s3_client.client.get_object(Bucket=bucket_name, Key=key3, VersionId=v1_id)
            # v3 should be current
            r3 = s3_client.client.get_object(Bucket=bucket_name, Key=key3)

            if r1['Body'].read() == b'version1' and r3['Body'].read() == b'version3':
                results['passed'].append('Specific version delete preserves others')
                print("✓ Specific version delete: Other versions preserved")
            else:
                results['failed'].append('Specific version delete: Wrong content')
        except Exception as e:
            results['failed'].append(f'Specific version delete: {str(e)}')

        # Test 6: Delete marker on delete marker (edge case)
        print("\nTest 6: Delete already deleted object")
        key4 = 'double-delete'
        s3_client.client.put_object(Bucket=bucket_name, Key=key4, Body=b'test')

        # First delete (creates delete marker)
        d1 = s3_client.client.delete_object(Bucket=bucket_name, Key=key4)

        # Second delete (should create another delete marker)
        d2 = s3_client.client.delete_object(Bucket=bucket_name, Key=key4)

        if d1.get('VersionId') != d2.get('VersionId'):
            results['passed'].append('Multiple delete markers on same key')
            print("✓ Double delete: Creates separate delete markers")
        else:
            results['failed'].append('Double delete: Same delete marker ID')

        # Summary
        print(f"\n=== Delete Markers Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            versions = s3_client.client.list_object_versions(Bucket=bucket_name)

            # Delete all versions
            if 'Versions' in versions:
                for version in versions['Versions']:
                    s3_client.client.delete_object(
                        Bucket=bucket_name,
                        Key=version['Key'],
                        VersionId=version['VersionId']
                    )

            # Delete all delete markers
            if 'DeleteMarkers' in versions:
                for marker in versions['DeleteMarkers']:
                    s3_client.client.delete_object(
                        Bucket=bucket_name,
                        Key=marker['Key'],
                        VersionId=marker['VersionId']
                    )

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
    test_delete_markers(s3)