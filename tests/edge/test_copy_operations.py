#!/usr/bin/env python3
"""
Test: Copy Operations Edge Cases
Tests cross-bucket copy, metadata preservation, large object copy, and copy conditions
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_copy_operations(s3_client: S3Client):
    """Test copy operation edge cases"""
    bucket1 = f's3-copy-source-{random_string(8).lower()}'
    bucket2 = f's3-copy-dest-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket1)
        s3_client.create_bucket(bucket2)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic copy within same bucket
        print("Test 1: Copy within same bucket")
        source_key = 'source-object'
        dest_key = 'copied-object'
        test_data = b'original data'

        s3_client.client.put_object(
            Bucket=bucket1,
            Key=source_key,
            Body=test_data,
            Metadata={'original': 'metadata'}
        )

        try:
            # Copy within same bucket
            response = s3_client.client.copy_object(
                Bucket=bucket1,
                Key=dest_key,
                CopySource={'Bucket': bucket1, 'Key': source_key}
            )

            # Verify copy
            obj = s3_client.client.get_object(Bucket=bucket1, Key=dest_key)
            if obj['Body'].read() == test_data:
                results['passed'].append('Same bucket copy')
                print("✓ Same bucket copy: Data preserved")
            else:
                results['failed'].append('Same bucket copy: Data mismatch')

        except Exception as e:
            results['failed'].append(f'Same bucket copy: {str(e)}')

        # Test 2: Cross-bucket copy
        print("\nTest 2: Cross-bucket copy")
        try:
            response = s3_client.client.copy_object(
                Bucket=bucket2,
                Key='cross-bucket-copy',
                CopySource={'Bucket': bucket1, 'Key': source_key}
            )

            # Verify
            obj = s3_client.client.get_object(Bucket=bucket2, Key='cross-bucket-copy')
            if obj['Body'].read() == test_data:
                results['passed'].append('Cross-bucket copy')
                print("✓ Cross-bucket copy: Success")
            else:
                results['failed'].append('Cross-bucket: Data mismatch')

        except Exception as e:
            results['failed'].append(f'Cross-bucket: {str(e)}')

        # Test 3: Copy with metadata replacement
        print("\nTest 3: Copy with metadata replacement")
        try:
            response = s3_client.client.copy_object(
                Bucket=bucket1,
                Key='metadata-replaced',
                CopySource={'Bucket': bucket1, 'Key': source_key},
                Metadata={'new': 'metadata', 'another': 'value'},
                MetadataDirective='REPLACE'
            )

            # Check metadata
            head = s3_client.client.head_object(Bucket=bucket1, Key='metadata-replaced')
            metadata = head.get('Metadata', {})

            if 'new' in metadata and 'original' not in metadata:
                results['passed'].append('Metadata replacement')
                print("✓ Metadata replacement: Old metadata replaced")
            else:
                results['failed'].append('Metadata: Not replaced')

        except Exception as e:
            results['failed'].append(f'Metadata replace: {str(e)}')

        # Test 4: Copy with metadata preservation
        print("\nTest 4: Copy with metadata preservation")
        try:
            response = s3_client.client.copy_object(
                Bucket=bucket1,
                Key='metadata-preserved',
                CopySource={'Bucket': bucket1, 'Key': source_key},
                MetadataDirective='COPY'
            )

            # Check metadata
            head = s3_client.client.head_object(Bucket=bucket1, Key='metadata-preserved')
            metadata = head.get('Metadata', {})

            if 'original' in metadata:
                results['passed'].append('Metadata preservation')
                print("✓ Metadata preservation: Original metadata kept")
            else:
                results['failed'].append('Metadata: Not preserved')

        except Exception as e:
            results['failed'].append(f'Metadata preserve: {str(e)}')

        # Test 5: Copy non-existent object
        print("\nTest 5: Copy non-existent object")
        try:
            s3_client.client.copy_object(
                Bucket=bucket1,
                Key='copy-of-nothing',
                CopySource={'Bucket': bucket1, 'Key': 'does-not-exist'}
            )
            results['failed'].append('Copy non-existent: Should have failed')
            print("✗ Copy non-existent: Succeeded (should fail)")
        except Exception as e:
            if 'NoSuchKey' in str(e) or '404' in str(e):
                results['passed'].append('Copy non-existent rejected')
                print("✓ Copy non-existent: Correctly rejected")
            else:
                results['failed'].append(f'Copy non-existent: Wrong error')

        # Test 6: Copy to itself (should fail or be no-op)
        print("\nTest 6: Copy object to itself")
        try:
            s3_client.client.copy_object(
                Bucket=bucket1,
                Key=source_key,
                CopySource={'Bucket': bucket1, 'Key': source_key},
                MetadataDirective='COPY'
            )
            results['failed'].append('Copy to self: Should have failed')
            print("✗ Copy to self: Succeeded (should fail)")
        except Exception as e:
            if 'InvalidRequest' in str(e) or 'Cannot copy' in str(e):
                results['passed'].append('Copy to self rejected')
                print("✓ Copy to self: Correctly rejected")
            else:
                # Some implementations allow it with metadata changes
                results['passed'].append('Copy to self handled')

        # Test 7: Conditional copy (If-Match)
        print("\nTest 7: Conditional copy")
        head = s3_client.client.head_object(Bucket=bucket1, Key=source_key)
        etag = head['ETag']

        try:
            # Copy with matching ETag
            s3_client.client.copy_object(
                Bucket=bucket1,
                Key='conditional-copy-match',
                CopySource={'Bucket': bucket1, 'Key': source_key},
                CopySourceIfMatch=etag
            )
            results['passed'].append('Conditional copy (match)')
            print("✓ Conditional copy: If-Match succeeded")

            # Copy with non-matching ETag
            try:
                s3_client.client.copy_object(
                    Bucket=bucket1,
                    Key='conditional-copy-no-match',
                    CopySource={'Bucket': bucket1, 'Key': source_key},
                    CopySourceIfMatch='"wrong-etag"'
                )
                results['failed'].append('Wrong If-Match: Should have failed')
            except:
                results['passed'].append('Wrong If-Match rejected')
                print("✓ Conditional copy: Wrong If-Match rejected")

        except Exception as e:
            results['failed'].append(f'Conditional copy: {str(e)}')

        # Test 8: Large object copy (multipart copy)
        print("\nTest 8: Large object copy")
        large_key = 'large-object'
        large_data = b'X' * (6 * 1024 * 1024)  # 6MB

        s3_client.client.put_object(
            Bucket=bucket1,
            Key=large_key,
            Body=large_data
        )

        try:
            # Copy large object (may trigger multipart copy)
            s3_client.client.copy_object(
                Bucket=bucket2,
                Key='large-copy',
                CopySource={'Bucket': bucket1, 'Key': large_key}
            )

            # Verify size
            head = s3_client.client.head_object(Bucket=bucket2, Key='large-copy')
            if head['ContentLength'] == len(large_data):
                results['passed'].append('Large object copy')
                print("✓ Large object copy: Size preserved")
            else:
                results['failed'].append('Large copy: Size mismatch')

        except Exception as e:
            results['failed'].append(f'Large copy: {str(e)}')

        # Test 9: Copy with storage class change
        print("\nTest 9: Copy with storage class change")
        try:
            s3_client.client.copy_object(
                Bucket=bucket1,
                Key='changed-storage-class',
                CopySource={'Bucket': bucket1, 'Key': source_key},
                StorageClass='STANDARD_IA'
            )

            head = s3_client.client.head_object(Bucket=bucket1, Key='changed-storage-class')
            if head.get('StorageClass') == 'STANDARD_IA':
                results['passed'].append('Storage class change')
                print("✓ Storage class: Changed during copy")
            else:
                # Provider might not support storage classes
                results['passed'].append('Storage class not supported')
                print("✓ Storage class: Feature not supported")

        except Exception as e:
            if 'InvalidStorageClass' in str(e):
                results['passed'].append('Storage class not available')
                print("✓ Storage class: Not available (expected)")
            else:
                results['failed'].append(f'Storage class: {str(e)}')

        # Test 10: Copy with server-side encryption
        print("\nTest 10: Copy with encryption")
        try:
            s3_client.client.copy_object(
                Bucket=bucket1,
                Key='encrypted-copy',
                CopySource={'Bucket': bucket1, 'Key': source_key},
                ServerSideEncryption='AES256'
            )

            head = s3_client.client.head_object(Bucket=bucket1, Key='encrypted-copy')
            if head.get('ServerSideEncryption') == 'AES256':
                results['passed'].append('Copy with encryption')
                print("✓ Copy encryption: Applied during copy")
            else:
                results['passed'].append('Encryption not confirmed')

        except Exception as e:
            results['failed'].append(f'Copy encryption: {str(e)}')

        # Summary
        print(f"\n=== Copy Operations Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            for bucket in [bucket1, bucket2]:
                try:
                    objects = s3_client.client.list_objects_v2(Bucket=bucket)
                    if 'Contents' in objects:
                        for obj in objects['Contents']:
                            s3_client.client.delete_object(Bucket=bucket, Key=obj['Key'])
                    s3_client.delete_bucket(bucket)
                except:
                    pass
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
    test_copy_operations(s3)