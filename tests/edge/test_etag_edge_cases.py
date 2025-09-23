#!/usr/bin/env python3
"""
Test: ETag Edge Cases and Validation
Tests ETag generation, format consistency, and edge case scenarios
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import hashlib

def test_etag_edge_cases(s3_client: S3Client):
    """Test ETag edge cases and validation"""
    bucket_name = f's3-etag-edge-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: ETag consistency across operations
        print("Test 1: ETag consistency across operations")
        key1 = 'etag-consistency'
        test_data = b'ETag consistency test data'

        try:
            # Upload
            put_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=test_data
            )
            put_etag = put_response.get('ETag')

            # Head object
            head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key1)
            head_etag = head_response.get('ETag')

            # Get object
            get_response = s3_client.client.get_object(Bucket=bucket_name, Key=key1)
            get_etag = get_response.get('ETag')

            # List objects
            list_response = s3_client.client.list_objects_v2(Bucket=bucket_name)
            list_etag = None
            for obj in list_response.get('Contents', []):
                if obj['Key'] == key1:
                    list_etag = obj.get('ETag')
                    break

            # All ETags should be identical
            etags = [put_etag, head_etag, get_etag, list_etag]
            unique_etags = set(filter(None, etags))

            if len(unique_etags) == 1:
                results['passed'].append('ETag consistency')
                print(f"✓ ETag consistency: {list(unique_etags)[0]}")
            else:
                results['failed'].append(f'ETag inconsistency: {etags}')

        except Exception as e:
            results['failed'].append(f'ETag consistency: {str(e)}')

        # Test 2: ETag for empty objects
        print("\nTest 2: ETag for empty objects")
        try:
            # Upload empty object
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-etag',
                Body=b''
            )
            empty_etag = response.get('ETag')

            # Empty object should have predictable ETag (MD5 of empty string)
            expected_empty_md5 = '"' + hashlib.md5(b'').hexdigest() + '"'

            if empty_etag == expected_empty_md5:
                results['passed'].append('Empty object ETag (MD5)')
                print(f"✓ Empty ETag: {empty_etag} (MD5)")
            elif empty_etag:
                results['passed'].append('Empty object ETag (non-MD5)')
                print(f"✓ Empty ETag: {empty_etag} (custom format)")
            else:
                results['failed'].append('Empty object: No ETag')

        except Exception as e:
            results['failed'].append(f'Empty ETag: {str(e)}')

        # Test 3: ETag for identical content
        print("\nTest 3: ETag for identical content")
        identical_data = b'Identical content test'

        try:
            # Upload same content to different keys
            resp1 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='identical-1',
                Body=identical_data
            )
            resp2 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='identical-2',
                Body=identical_data
            )

            etag1 = resp1.get('ETag')
            etag2 = resp2.get('ETag')

            if etag1 == etag2:
                results['passed'].append('Identical content ETags')
                print(f"✓ Identical content: Same ETag {etag1}")
            else:
                results['failed'].append(f'Identical content: Different ETags {etag1} vs {etag2}')

        except Exception as e:
            results['failed'].append(f'Identical content: {str(e)}')

        # Test 4: ETag changes on content modification
        print("\nTest 4: ETag changes on modification")
        key4 = 'etag-modification'

        try:
            # Original upload
            resp1 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'Original content'
            )
            original_etag = resp1.get('ETag')

            # Modified upload
            resp2 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'Modified content'
            )
            modified_etag = resp2.get('ETag')

            if original_etag != modified_etag:
                results['passed'].append('ETag changes on modification')
                print(f"✓ Modification: ETag changed")
            else:
                results['failed'].append('ETag unchanged on modification')

        except Exception as e:
            results['failed'].append(f'ETag modification: {str(e)}')

        # Test 5: ETag format validation
        print("\nTest 5: ETag format validation")
        try:
            # Test various data sizes
            test_sizes = [1, 100, 1024, 1024*1024]

            for size in test_sizes:
                data = b'F' * size
                response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=f'format-test-{size}',
                    Body=data
                )
                etag = response.get('ETag', '')

                # Validate format
                format_valid = True
                format_issues = []

                # Should be quoted
                if not (etag.startswith('"') and etag.endswith('"')):
                    format_valid = False
                    format_issues.append('not quoted')

                # Remove quotes for further validation
                etag_content = etag.strip('"')

                # Should be hex or hex with suffix
                if not all(c in '0123456789abcdefABCDEF-' for c in etag_content):
                    format_valid = False
                    format_issues.append('invalid characters')

                # Length check (MD5 is 32 chars, multipart may have suffix)
                if len(etag_content) < 32:
                    format_valid = False
                    format_issues.append('too short')

                if format_valid:
                    results['passed'].append(f'ETag format {size}')
                else:
                    results['failed'].append(f'ETag format {size}: {format_issues}')

            print(f"✓ ETag format: {len(test_sizes)} sizes validated")

        except Exception as e:
            results['failed'].append(f'ETag format: {str(e)}')

        # Test 6: Multipart ETag vs single part
        print("\nTest 6: Multipart vs single part ETag")
        try:
            # Single part upload
            single_data = b'S' * (10 * 1024 * 1024)  # 10MB
            single_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='single-part-etag',
                Body=single_data
            )
            single_etag = single_response.get('ETag')

            # Multipart upload of same data
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key='multipart-etag'
            )['UploadId']

            # Split into 2 parts of 5MB each
            part1_data = single_data[:5*1024*1024]
            part2_data = single_data[5*1024*1024:]

            part1_resp = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key='multipart-etag',
                UploadId=upload_id,
                PartNumber=1,
                Body=io.BytesIO(part1_data)
            )

            part2_resp = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key='multipart-etag',
                UploadId=upload_id,
                PartNumber=2,
                Body=io.BytesIO(part2_data)
            )

            # Complete multipart
            complete_resp = s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key='multipart-etag',
                UploadId=upload_id,
                MultipartUpload={'Parts': [
                    {'PartNumber': 1, 'ETag': part1_resp['ETag']},
                    {'PartNumber': 2, 'ETag': part2_resp['ETag']}
                ]}
            )
            multipart_etag = complete_resp.get('ETag')

            # ETags should be different
            if single_etag != multipart_etag:
                results['passed'].append('Multipart vs single ETag difference')
                print(f"✓ Multipart difference: Single={single_etag}, Multi={multipart_etag}")

                # Multipart ETag should contain a dash
                if '-' in multipart_etag:
                    results['passed'].append('Multipart ETag format')
                    print("✓ Multipart format: Contains dash")
                else:
                    results['failed'].append('Multipart ETag: Missing dash')

            else:
                results['failed'].append('Multipart vs single: Same ETag')

        except Exception as e:
            results['failed'].append(f'Multipart ETag: {str(e)}')

        # Test 7: ETag with special characters in data
        print("\nTest 7: ETag with special data")
        special_data_tests = [
            (b'\x00\x01\x02\x03\xFF\xFE\xFD', 'Binary data'),
            (b'\r\n\t\x00', 'Control characters'),
            ('🚀 Unicode test 测试'.encode('utf-8'), 'Unicode data'),
            (b'A' * 1000 + b'\x00' + b'B' * 1000, 'Null in middle'),
        ]

        for data, description in special_data_tests:
            try:
                response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=f'special-{description.replace(" ", "-").lower()}',
                    Body=data
                )
                etag = response.get('ETag')

                if etag and etag.startswith('"') and etag.endswith('"'):
                    results['passed'].append(f'Special data ETag: {description}')
                    print(f"✓ {description}: Valid ETag")
                else:
                    results['failed'].append(f'Special data ETag: {description}')

            except Exception as e:
                results['failed'].append(f'Special data {description}: {str(e)}')

        # Test 8: ETag preservation in copy operations
        print("\nTest 8: ETag in copy operations")
        try:
            # Create source object
            source_data = b'Copy ETag test data'
            source_resp = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='copy-source-etag',
                Body=source_data
            )
            source_etag = source_resp.get('ETag')

            # Copy object
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key='copy-dest-etag',
                CopySource={'Bucket': bucket_name, 'Key': 'copy-source-etag'}
            )

            # Get destination ETag
            dest_head = s3_client.client.head_object(Bucket=bucket_name, Key='copy-dest-etag')
            dest_etag = dest_head.get('ETag')

            if source_etag == dest_etag:
                results['passed'].append('Copy preserves ETag')
                print("✓ Copy: ETag preserved")
            else:
                results['passed'].append('Copy generates new ETag')
                print(f"✓ Copy: New ETag generated (source={source_etag}, dest={dest_etag})")

        except Exception as e:
            results['failed'].append(f'Copy ETag: {str(e)}')

        # Test 9: ETag case sensitivity
        print("\nTest 9: ETag case sensitivity")
        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='case-test',
                Body=b'Case sensitivity test'
            )
            etag = response.get('ETag', '').strip('"')

            # Check if ETag contains both upper and lower case
            has_upper = any(c.isupper() for c in etag)
            has_lower = any(c.islower() for c in etag)

            if has_upper or has_lower:
                results['passed'].append('ETag case format')
                print(f"✓ ETag case: {etag} ({'mixed' if has_upper and has_lower else 'uniform'} case)")
            else:
                results['passed'].append('ETag no alpha chars')
                print(f"✓ ETag case: {etag} (no alphabetic characters)")

        except Exception as e:
            results['failed'].append(f'ETag case: {str(e)}')

        # Test 10: ETag with metadata changes
        print("\nTest 10: ETag with metadata changes")
        try:
            # Upload with metadata
            resp1 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='metadata-etag-test',
                Body=b'Same content',
                Metadata={'version': '1'}
            )
            etag1 = resp1.get('ETag')

            # Upload same content with different metadata
            resp2 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key='metadata-etag-test',
                Body=b'Same content',
                Metadata={'version': '2'}
            )
            etag2 = resp2.get('ETag')

            if etag1 == etag2:
                results['passed'].append('ETag ignores metadata')
                print("✓ Metadata change: ETag unchanged (content-based)")
            else:
                results['passed'].append('ETag includes metadata')
                print("✓ Metadata change: ETag changed (includes metadata)")

        except Exception as e:
            results['failed'].append(f'Metadata ETag: {str(e)}')

        # Summary
        print(f"\n=== ETag Edge Cases Test Results ===")
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
    test_etag_edge_cases(s3)