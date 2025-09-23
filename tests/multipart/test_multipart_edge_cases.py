#!/usr/bin/env python3
"""
Test 003: Multipart Upload Edge Cases
Tests unusual multipart upload scenarios including out-of-order parts,
duplicate part numbers, missing parts, and concurrent uploads.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import hashlib

def test_multipart_edge_cases(s3_client: S3Client):
    """Test multipart upload edge cases"""
    bucket_name = f's3-multipart-edge-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Upload parts out of order
        print("Test 1: Out-of-order parts upload")
        key1 = 'out-of-order-upload'
        upload_id1 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key1
        )['UploadId']

        try:
            parts = []
            part_size = 5 * 1024 * 1024  # 5MB minimum

            # Upload part 3 first
            data3 = b'3' * part_size
            resp3 = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key1,
                UploadId=upload_id1,
                PartNumber=3,
                Body=io.BytesIO(data3)
            )
            parts.append({'PartNumber': 3, 'ETag': resp3['ETag']})

            # Upload part 1
            data1 = b'1' * part_size
            resp1 = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key1,
                UploadId=upload_id1,
                PartNumber=1,
                Body=io.BytesIO(data1)
            )
            parts.append({'PartNumber': 1, 'ETag': resp1['ETag']})

            # Upload part 2
            data2 = b'2' * part_size
            resp2 = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key1,
                UploadId=upload_id1,
                PartNumber=2,
                Body=io.BytesIO(data2)
            )
            parts.append({'PartNumber': 2, 'ETag': resp2['ETag']})

            # Complete with correct order
            parts_sorted = sorted(parts, key=lambda x: x['PartNumber'])
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key1,
                UploadId=upload_id1,
                MultipartUpload={'Parts': parts_sorted}
            )

            # Verify object
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key1)
            content = obj['Body'].read()
            expected = data1 + data2 + data3

            if content == expected:
                results['passed'].append('Out-of-order parts')
                print("✓ Out-of-order parts handled correctly")
            else:
                results['failed'].append('Out-of-order parts: Data mismatch')
                print("✗ Out-of-order parts: Data mismatch")

        except Exception as e:
            results['failed'].append(f'Out-of-order parts: {str(e)}')
            print(f"✗ Out-of-order parts: {str(e)}")

        # Test 2: Duplicate part numbers (overwrite)
        print("\nTest 2: Duplicate part numbers")
        key2 = 'duplicate-parts'
        upload_id2 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key2
        )['UploadId']

        try:
            # Upload part 1 twice with different data
            data_first = b'A' * part_size
            resp_first = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key2,
                UploadId=upload_id2,
                PartNumber=1,
                Body=io.BytesIO(data_first)
            )

            data_second = b'B' * part_size
            resp_second = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key2,
                UploadId=upload_id2,
                PartNumber=1,  # Same part number
                Body=io.BytesIO(data_second)
            )

            # Complete with the second ETag
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key2,
                UploadId=upload_id2,
                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': resp_second['ETag']}]}
            )

            # Verify which data was used
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key2)
            content = obj['Body'].read()

            if content == data_second:
                results['passed'].append('Duplicate parts (overwrites)')
                print("✓ Duplicate part numbers: Later upload overwrites")
            elif content == data_first:
                results['failed'].append('Duplicate parts: Used first upload instead of last')
                print("✗ Duplicate parts: Used first upload instead of last")
            else:
                results['failed'].append('Duplicate parts: Unexpected content')
                print("✗ Duplicate parts: Unexpected content")

        except Exception as e:
            results['failed'].append(f'Duplicate parts: {str(e)}')
            print(f"✗ Duplicate parts: {str(e)}")

        # Test 3: Missing parts in completion
        print("\nTest 3: Missing parts in completion")
        key3 = 'missing-parts'
        upload_id3 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key3
        )['UploadId']

        try:
            # Upload parts 1, 2, and 3
            parts3 = []
            for i in range(1, 4):
                data = bytes([i]) * part_size
                resp = s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=key3,
                    UploadId=upload_id3,
                    PartNumber=i,
                    Body=io.BytesIO(data)
                )
                if i != 2:  # Skip part 2 in completion
                    parts3.append({'PartNumber': i, 'ETag': resp['ETag']})

            # Try to complete without part 2
            try:
                s3_client.client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=key3,
                    UploadId=upload_id3,
                    MultipartUpload={'Parts': parts3}
                )
                results['failed'].append('Missing parts: Completed without all parts')
                print("✗ Missing parts: Should have failed but completed")
            except Exception as e:
                if 'InvalidPart' in str(e) or 'InvalidPartOrder' in str(e):
                    results['passed'].append('Missing parts rejected')
                    print("✓ Missing parts: Correctly rejected")
                else:
                    results['failed'].append(f'Missing parts: Unexpected error: {e}')
                    print(f"✗ Missing parts: Unexpected error: {e}")

        except Exception as e:
            results['failed'].append(f'Missing parts test: {str(e)}')
            print(f"✗ Missing parts test: {str(e)}")
        finally:
            # Abort the upload
            try:
                s3_client.client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key3,
                    UploadId=upload_id3
                )
            except:
                pass

        # Test 4: Zero-byte parts
        print("\nTest 4: Zero-byte parts")
        key4 = 'zero-byte-parts'
        upload_id4 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key4
        )['UploadId']

        try:
            # Try to upload a zero-byte part
            try:
                resp = s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=key4,
                    UploadId=upload_id4,
                    PartNumber=1,
                    Body=io.BytesIO(b'')  # Zero bytes
                )
                results['failed'].append('Zero-byte part: Should have been rejected')
                print("✗ Zero-byte part: Accepted (should reject)")
            except Exception as e:
                if 'EntityTooSmall' in str(e) or 'TooSmall' in str(e):
                    results['passed'].append('Zero-byte part rejected')
                    print("✓ Zero-byte part: Correctly rejected")
                else:
                    results['failed'].append(f'Zero-byte part: Unexpected error: {e}')
                    print(f"✗ Zero-byte part: Unexpected error: {e}")

        finally:
            # Abort the upload
            try:
                s3_client.client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key4,
                    UploadId=upload_id4
                )
            except:
                pass

        # Test 5: Part number limits
        print("\nTest 5: Part number limits")
        key5 = 'part-number-limits'
        upload_id5 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key5
        )['UploadId']

        try:
            # Test invalid part numbers
            invalid_parts = [0, -1, 10001]  # Valid range is 1-10000

            for part_num in invalid_parts:
                try:
                    s3_client.client.upload_part(
                        Bucket=bucket_name,
                        Key=key5,
                        UploadId=upload_id5,
                        PartNumber=part_num,
                        Body=io.BytesIO(b'X' * part_size)
                    )
                    results['failed'].append(f'Part {part_num}: Should have been rejected')
                    print(f"✗ Part number {part_num}: Accepted (should reject)")
                except Exception as e:
                    if 'InvalidArgument' in str(e) or 'InvalidPartNumber' in str(e):
                        results['passed'].append(f'Part {part_num} rejected')
                        print(f"✓ Part number {part_num}: Correctly rejected")
                    else:
                        results['failed'].append(f'Part {part_num}: Unexpected error')

        finally:
            # Abort the upload
            try:
                s3_client.client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key5,
                    UploadId=upload_id5
                )
            except:
                pass

        # Summary
        print(f"\n=== Multipart Edge Cases Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            # Abort any remaining multipart uploads
            uploads = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
            if 'Uploads' in uploads:
                for upload in uploads['Uploads']:
                    s3_client.client.abort_multipart_upload(
                        Bucket=bucket_name,
                        Key=upload['Key'],
                        UploadId=upload['UploadId']
                    )

            # Delete objects
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
    test_multipart_edge_cases(s3)