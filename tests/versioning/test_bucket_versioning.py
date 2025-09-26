#!/usr/bin/env python3
"""
Test: Bucket Versioning APIs
Tests versioning configuration, object version management, and delete markers.
Core S3 feature required for replication, object locking, and data protection.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import time

def test_bucket_versioning(s3_client: S3Client):
    """Test bucket versioning configuration and version management"""
    bucket_name = f's3-versioning-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Default versioning state
        print("Test 1: Default versioning state")
        try:
            response = s3_client.client.get_bucket_versioning(Bucket=bucket_name)

            # New buckets should have no versioning status or be disabled
            status = response.get('Status', 'Disabled')
            if status in ['Disabled', None] or 'Status' not in response:
                results['passed'].append('Default versioning state')
                print("✓ Default state: Versioning disabled by default")
            else:
                results['failed'].append(f'Default state: Unexpected status {status}')

        except Exception as e:
            results['failed'].append(f'Default state: {str(e)}')

        # Test 2: Enable versioning
        print("\nTest 2: Enable versioning")
        try:
            s3_client.client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )

            # Verify versioning is enabled
            response = s3_client.client.get_bucket_versioning(Bucket=bucket_name)
            if response.get('Status') == 'Enabled':
                results['passed'].append('Enable versioning')
                print("✓ Enable versioning: Successfully enabled")
            else:
                results['failed'].append('Enable versioning: Status not set')

        except Exception as e:
            results['failed'].append(f'Enable versioning: {str(e)}')

        # Test 3: Create multiple versions of same object
        print("\nTest 3: Create multiple object versions")
        try:
            key = 'versioned-object'

            # Create version 1
            v1_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b'Version 1 content'
            )
            version_id_1 = v1_response.get('VersionId')

            # Create version 2
            v2_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b'Version 2 content'
            )
            version_id_2 = v2_response.get('VersionId')

            # Create version 3
            v3_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b'Version 3 content'
            )
            version_id_3 = v3_response.get('VersionId')

            # Verify we have different version IDs
            version_ids = [version_id_1, version_id_2, version_id_3]
            if len(set(version_ids)) == 3 and all(vid for vid in version_ids):
                results['passed'].append('Multiple object versions')
                print("✓ Multiple versions: 3 distinct versions created")
            else:
                results['failed'].append('Multiple versions: Version IDs not unique')

        except Exception as e:
            results['failed'].append(f'Multiple versions: {str(e)}')

        # Test 4: Retrieve specific versions
        print("\nTest 4: Retrieve specific object versions")
        try:
            # Get current version (should be version 3)
            current = s3_client.client.get_object(Bucket=bucket_name, Key=key)
            current_content = current['Body'].read()

            # Get specific version 1
            v1_obj = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=version_id_1
            )
            v1_content = v1_obj['Body'].read()

            # Get specific version 2
            v2_obj = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=version_id_2
            )
            v2_content = v2_obj['Body'].read()

            if (current_content == b'Version 3 content' and
                v1_content == b'Version 1 content' and
                v2_content == b'Version 2 content'):
                results['passed'].append('Retrieve specific versions')
                print("✓ Version retrieval: All versions accessible")
            else:
                results['failed'].append('Version retrieval: Content mismatch')

        except Exception as e:
            results['failed'].append(f'Version retrieval: {str(e)}')

        # Test 5: List object versions
        print("\nTest 5: List object versions")
        try:
            response = s3_client.client.list_object_versions(Bucket=bucket_name)

            versions = response.get('Versions', [])
            # Should have 3 versions of our object
            our_versions = [v for v in versions if v['Key'] == key]

            if len(our_versions) == 3:
                # Check version ordering (newest first by default)
                latest_version = our_versions[0]
                if latest_version['VersionId'] == version_id_3:
                    results['passed'].append('List object versions')
                    print("✓ List versions: All versions listed correctly")
                else:
                    results['failed'].append('List versions: Order incorrect')
            else:
                results['failed'].append(f'List versions: Expected 3, got {len(our_versions)}')

        except Exception as e:
            results['failed'].append(f'List versions: {str(e)}')

        # Test 6: Delete specific version
        print("\nTest 6: Delete specific object version")
        try:
            # Delete version 2
            s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=version_id_2
            )

            # Verify version 2 is gone but others remain
            response = s3_client.client.list_object_versions(Bucket=bucket_name)
            versions = response.get('Versions', [])
            our_versions = [v for v in versions if v['Key'] == key]
            remaining_ids = [v['VersionId'] for v in our_versions]

            if (len(our_versions) == 2 and
                version_id_1 in remaining_ids and
                version_id_3 in remaining_ids and
                version_id_2 not in remaining_ids):
                results['passed'].append('Delete specific version')
                print("✓ Version deletion: Specific version deleted")
            else:
                results['failed'].append('Version deletion: Unexpected versions remain')

        except Exception as e:
            results['failed'].append(f'Version deletion: {str(e)}')

        # Test 7: Delete current version (creates delete marker)
        print("\nTest 7: Delete current version (delete marker)")
        try:
            delete_response = s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=key
            )

            # Should get a delete marker
            delete_marker_id = delete_response.get('DeleteMarker')
            if delete_marker_id:
                results['passed'].append('Delete marker creation')
                print("✓ Delete marker: Created when deleting current version")

                # Verify object appears deleted
                try:
                    s3_client.client.get_object(Bucket=bucket_name, Key=key)
                    results['failed'].append('Delete marker: Object still accessible')
                except Exception:
                    results['passed'].append('Delete marker effect')
                    print("✓ Delete marker: Object inaccessible")

            else:
                results['failed'].append('Delete marker: Not created')

        except Exception as e:
            results['failed'].append(f'Delete marker: {str(e)}')

        # Test 8: List delete markers
        print("\nTest 8: List delete markers")
        try:
            response = s3_client.client.list_object_versions(Bucket=bucket_name)

            delete_markers = response.get('DeleteMarkers', [])
            our_markers = [dm for dm in delete_markers if dm['Key'] == key]

            if len(our_markers) >= 1:
                results['passed'].append('List delete markers')
                print("✓ Delete markers: Listed in version output")
            else:
                results['failed'].append('List delete markers: Not found')

        except Exception as e:
            results['failed'].append(f'List delete markers: {str(e)}')

        # Test 9: Delete delete marker (undelete)
        print("\nTest 9: Delete delete marker (undelete)")
        try:
            # Get the delete marker ID
            response = s3_client.client.list_object_versions(Bucket=bucket_name)
            delete_markers = response.get('DeleteMarkers', [])
            our_marker = next(dm for dm in delete_markers if dm['Key'] == key)
            marker_id = our_marker['VersionId']

            # Delete the delete marker
            s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=marker_id
            )

            # Object should be accessible again
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key)
            content = obj['Body'].read()

            if content == b'Version 3 content':  # Should restore to latest version
                results['passed'].append('Undelete object')
                print("✓ Undelete: Object restored by removing delete marker")
            else:
                results['failed'].append('Undelete: Wrong content restored')

        except Exception as e:
            results['failed'].append(f'Undelete: {str(e)}')

        # Test 10: Suspend versioning
        print("\nTest 10: Suspend versioning")
        try:
            s3_client.client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Suspended'}
            )

            # Verify versioning is suspended
            response = s3_client.client.get_bucket_versioning(Bucket=bucket_name)
            if response.get('Status') == 'Suspended':
                results['passed'].append('Suspend versioning')
                print("✓ Suspend versioning: Successfully suspended")

                # Test behavior with suspended versioning
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key='suspended-test',
                    Body=b'test with suspended versioning'
                )

                # Should have null version ID
                head = s3_client.client.head_object(Bucket=bucket_name, Key='suspended-test')
                if head.get('VersionId') == 'null' or not head.get('VersionId'):
                    results['passed'].append('Suspended versioning behavior')
                    print("✓ Suspended behavior: Null version created")

            else:
                results['failed'].append('Suspend versioning: Status not set')

        except Exception as e:
            results['failed'].append(f'Suspend versioning: {str(e)}')

        # Test 11: Version metadata preservation
        print("\nTest 11: Version metadata preservation")
        try:
            # Re-enable versioning for this test
            s3_client.client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )

            metadata_key = 'metadata-test'

            # Create version with metadata
            v1_meta = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=metadata_key,
                Body=b'version 1 with metadata',
                Metadata={'test-key': 'test-value-1', 'version': '1'}
            )

            # Create version with different metadata
            v2_meta = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=metadata_key,
                Body=b'version 2 with metadata',
                Metadata={'test-key': 'test-value-2', 'version': '2'}
            )

            # Verify each version has its own metadata
            v1_head = s3_client.client.head_object(
                Bucket=bucket_name,
                Key=metadata_key,
                VersionId=v1_meta['VersionId']
            )

            v2_head = s3_client.client.head_object(
                Bucket=bucket_name,
                Key=metadata_key,
                VersionId=v2_meta['VersionId']
            )

            v1_version = v1_head.get('Metadata', {}).get('version')
            v2_version = v2_head.get('Metadata', {}).get('version')

            if v1_version == '1' and v2_version == '2':
                results['passed'].append('Version metadata preservation')
                print("✓ Metadata preservation: Each version retains metadata")
            else:
                results['failed'].append('Metadata preservation: Metadata not preserved')

        except Exception as e:
            results['failed'].append(f'Metadata preservation: {str(e)}')

        # Test 12: Copy with versioning
        print("\nTest 12: Copy operations with versioning")
        try:
            copy_source_key = 'copy-source'
            copy_dest_key = 'copy-dest'

            # Create source object
            source_resp = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=copy_source_key,
                Body=b'source for copying'
            )

            # Copy to new key
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=copy_dest_key,
                CopySource={'Bucket': bucket_name, 'Key': copy_source_key}
            )

            # Copy specific version
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=f'{copy_dest_key}-v2',
                CopySource={
                    'Bucket': bucket_name,
                    'Key': copy_source_key,
                    'VersionId': source_resp['VersionId']
                }
            )

            # Verify copies exist
            dest1 = s3_client.client.get_object(Bucket=bucket_name, Key=copy_dest_key)
            dest2 = s3_client.client.get_object(Bucket=bucket_name, Key=f'{copy_dest_key}-v2')

            if (dest1['Body'].read() == b'source for copying' and
                dest2['Body'].read() == b'source for copying'):
                results['passed'].append('Copy with versioning')
                print("✓ Copy versioning: Both current and specific version copies work")
            else:
                results['failed'].append('Copy versioning: Copy failed')

        except Exception as e:
            results['failed'].append(f'Copy versioning: {str(e)}')

        # Test 13: Versioning with multipart uploads
        print("\nTest 13: Multipart upload versioning")
        try:
            mp_key = 'multipart-versioned'

            # First multipart upload
            upload_id1 = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key
            )['UploadId']

            part_data = b'A' * (5 * 1024 * 1024)  # 5MB
            part1 = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id1,
                PartNumber=1,
                Body=part_data
            )

            complete1 = s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id1,
                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part1['ETag']}]}
            )

            # Second multipart upload (new version)
            upload_id2 = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key
            )['UploadId']

            part_data2 = b'B' * (5 * 1024 * 1024)  # 5MB
            part2 = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id2,
                PartNumber=1,
                Body=part_data2
            )

            complete2 = s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id2,
                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part2['ETag']}]}
            )

            # Verify both versions exist
            v1_data = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=mp_key,
                VersionId=complete1['VersionId']
            )['Body'].read()

            v2_data = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=mp_key,
                VersionId=complete2['VersionId']
            )['Body'].read()

            if v1_data == part_data and v2_data == part_data2:
                results['passed'].append('Multipart versioning')
                print("✓ Multipart versioning: Both versions preserved")
            else:
                results['failed'].append('Multipart versioning: Version content mismatch')

        except Exception as e:
            results['failed'].append(f'Multipart versioning: {str(e)}')

        # Summary
        print(f"\n=== Bucket Versioning Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

        return len(results['failed']) == 0

    except Exception as e:
        print(f"Critical error in versioning test setup: {str(e)}")
        return False

    finally:
        # Cleanup
        try:
            # Delete all object versions and delete markers
            response = s3_client.client.list_object_versions(Bucket=bucket_name)

            # Delete all versions
            if 'Versions' in response:
                for version in response['Versions']:
                    try:
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=version['Key'],
                            VersionId=version['VersionId']
                        )
                    except:
                        pass

            # Delete all delete markers
            if 'DeleteMarkers' in response:
                for marker in response['DeleteMarkers']:
                    try:
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=marker['Key'],
                            VersionId=marker['VersionId']
                        )
                    except:
                        pass

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
    test_bucket_versioning(s3)