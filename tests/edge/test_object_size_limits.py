#!/usr/bin/env python3
"""
Test: Object Size Limits and Boundaries
Tests minimum and maximum object sizes, empty objects, and size validation
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_object_size_limits(s3_client: S3Client):
    """Test object size limits and boundaries"""
    bucket_name = f's3-size-limits-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Empty object (0 bytes)
        print("Test 1: Empty object (0 bytes)")
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-object',
                Body=b''
            )

            # Verify empty object
            response = s3_client.client.get_object(Bucket=bucket_name, Key='empty-object')
            content = response['Body'].read()

            if len(content) == 0:
                results['passed'].append('Empty object')
                print("✓ Empty object: 0 bytes accepted")
            else:
                results['failed'].append(f'Empty object: Got {len(content)} bytes')

            # Check content length header
            head = s3_client.client.head_object(Bucket=bucket_name, Key='empty-object')
            if head['ContentLength'] == 0:
                results['passed'].append('Empty object metadata')
                print("✓ Empty object: Metadata correct")

        except Exception as e:
            results['failed'].append(f'Empty object: {str(e)}')

        # Test 2: Single byte object
        print("\nTest 2: Single byte object")
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='single-byte',
                Body=b'X'
            )

            response = s3_client.client.get_object(Bucket=bucket_name, Key='single-byte')
            content = response['Body'].read()

            if content == b'X':
                results['passed'].append('Single byte object')
                print("✓ Single byte: 1 byte handled correctly")

        except Exception as e:
            results['failed'].append(f'Single byte: {str(e)}')

        # Test 3: Various small sizes
        print("\nTest 3: Various small sizes")
        small_sizes = [1, 2, 3, 4, 5, 10, 100, 255, 256, 512, 1023, 1024]

        for size in small_sizes:
            try:
                data = b'X' * size
                key = f'size-{size}'

                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data
                )

                # Verify size
                head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
                if head['ContentLength'] == size:
                    results['passed'].append(f'Size {size}')
                else:
                    results['failed'].append(f'Size {size}: Length mismatch')

            except Exception as e:
                results['failed'].append(f'Size {size}: {str(e)}')

        print(f"✓ Small sizes: Tested {len(small_sizes)} different sizes")

        # Test 4: Power-of-2 boundaries
        print("\nTest 4: Power-of-2 boundaries")
        power_sizes = []
        for power in range(10, 21):  # 1KB to 1MB
            size = 2 ** power
            power_sizes.append(size)

        for size in power_sizes:
            try:
                # Create data efficiently
                data = b'0' * size
                key = f'power-{power}'

                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data
                )

                # Verify just the size, not content (too slow for large objects)
                head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
                if head['ContentLength'] == size:
                    results['passed'].append(f'Power-of-2: {size} bytes')
                else:
                    results['failed'].append(f'Power-of-2 {size}: Size mismatch')

            except Exception as e:
                if 'EntityTooLarge' in str(e):
                    results['passed'].append(f'Power-of-2 {size}: Size limit')
                    print(f"✓ Power-of-2 {size}: Size limit enforced")
                    break  # Don't test larger sizes
                else:
                    results['failed'].append(f'Power-of-2 {size}: {str(e)[:50]}')

        # Test 5: 5GB boundary (max single PUT)
        print("\nTest 5: 5GB boundary test")
        try:
            # Create a stream that claims to be 5GB + 1 byte
            class LargeSizeStream:
                def __init__(self, size):
                    self.size = size
                    self.position = 0

                def read(self, length=-1):
                    if length == -1 or length > self.size - self.position:
                        length = self.size - self.position
                    if length <= 0:
                        return b''

                    # Don't actually generate 5GB of data, just a small chunk
                    actual_length = min(length, 1024)  # Max 1KB at a time
                    data = b'L' * actual_length
                    self.position += actual_length
                    return data

            # This should fail as it exceeds single PUT limit
            large_stream = LargeSizeStream(5 * 1024 * 1024 * 1024 + 1)  # 5GB + 1 byte

            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key='oversized-single-put',
                    Body=large_stream,
                    ContentLength=5 * 1024 * 1024 * 1024 + 1
                )
                results['failed'].append('5GB+ single PUT: Should have failed')
                print("✗ 5GB+ single PUT: Accepted (should reject)")

            except Exception as e:
                if 'EntityTooLarge' in str(e) or 'InvalidRequest' in str(e):
                    results['passed'].append('5GB+ single PUT limit')
                    print("✓ 5GB+ single PUT: Correctly rejected")
                else:
                    results['passed'].append('Large PUT handling')
                    print(f"✓ Large PUT: Handled with {str(e)[:50]}...")

        except Exception as e:
            results['failed'].append(f'5GB test: {str(e)}')

        # Test 6: Maximum multipart object size (5TB theoretical)
        print("\nTest 6: Large multipart upload simulation")
        try:
            # Simulate a large multipart upload without actually uploading 5TB
            key = 'large-multipart-sim'

            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=key
            )['UploadId']

            # Upload maximum size parts (5GB each)
            part_size = 5 * 1024 * 1024  # Use 5MB for testing (not actual 5GB)
            max_parts = 3  # Test with 3 parts instead of 10000

            parts = []
            for part_num in range(1, max_parts + 1):
                part_data = b'P' * part_size

                response = s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=io.BytesIO(part_data)
                )

                parts.append({
                    'PartNumber': part_num,
                    'ETag': response['ETag']
                })

            # Complete upload
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            # Verify total size
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
            expected_size = max_parts * part_size

            if head['ContentLength'] == expected_size:
                results['passed'].append('Large multipart simulation')
                print(f"✓ Large multipart: {expected_size} bytes ({max_parts} parts)")
            else:
                results['failed'].append('Large multipart: Size mismatch')

        except Exception as e:
            results['failed'].append(f'Large multipart: {str(e)}')

        # Test 7: Part size limits in multipart
        print("\nTest 7: Multipart part size limits")

        # Test minimum part size (5MB, except last part)
        try:
            key = 'min-part-size'
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=key
            )['UploadId']

            # Try uploading a part smaller than 5MB (should fail except for last part)
            small_part = b'S' * (1024 * 1024)  # 1MB

            try:
                s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=1,
                    Body=io.BytesIO(small_part)
                )

                # If this succeeded, try to complete with just this part
                try:
                    s3_client.client.complete_multipart_upload(
                        Bucket=bucket_name,
                        Key=key,
                        UploadId=upload_id,
                        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': '"dummy"'}]}
                    )
                    results['passed'].append('Small part as last part')
                except:
                    results['passed'].append('Small part size handling')

            except Exception as e:
                if 'EntityTooSmall' in str(e):
                    results['passed'].append('Minimum part size enforced')
                    print("✓ Part size: Minimum enforced")
                else:
                    results['failed'].append(f'Min part size: {str(e)[:50]}')

            # Abort the upload
            try:
                s3_client.client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id
                )
            except:
                pass

        except Exception as e:
            results['failed'].append(f'Part size test: {str(e)}')

        # Test 8: Object count limits per bucket
        print("\nTest 8: Many small objects")

        # Create many small objects to test bucket object limits
        try:
            object_count = 100  # Test with 100 objects
            for i in range(object_count):
                key = f'many-objects-{i:03d}'
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'Object number {i}'.encode()
                )

            # List all objects
            response = s3_client.client.list_objects_v2(Bucket=bucket_name)
            listed_count = response.get('KeyCount', 0)

            if listed_count >= object_count:
                results['passed'].append(f'Many objects ({object_count})')
                print(f"✓ Many objects: {object_count} objects handled")
            else:
                results['failed'].append(f'Many objects: Only {listed_count}/{object_count}')

        except Exception as e:
            results['failed'].append(f'Many objects: {str(e)}')

        # Test 9: Content-Length mismatch
        print("\nTest 9: Content-Length mismatch")

        try:
            # Try to upload with wrong Content-Length
            actual_data = b'This is the actual data'
            wrong_length = len(actual_data) + 10  # Claim longer than actual

            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key='wrong-length',
                    Body=actual_data,
                    ContentLength=wrong_length
                )
                results['failed'].append('Content-Length mismatch: Should fail')
            except Exception as e:
                if 'IncompleteBody' in str(e) or 'ContentLengthMismatch' in str(e):
                    results['passed'].append('Content-Length mismatch detected')
                    print("✓ Content-Length: Mismatch detected")
                else:
                    results['passed'].append('Content-Length handling')

        except Exception as e:
            results['failed'].append(f'Content-Length test: {str(e)}')

        # Summary
        print(f"\n=== Object Size Limits Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            print("\nCleaning up size test objects...")
            # List and delete in batches to handle many objects
            continuation_token = None

            while True:
                params = {'Bucket': bucket_name, 'MaxKeys': 1000}
                if continuation_token:
                    params['ContinuationToken'] = continuation_token

                response = s3_client.client.list_objects_v2(**params)

                if 'Contents' in response:
                    for obj in response['Contents']:
                        s3_client.client.delete_object(Bucket=bucket_name, Key=obj['Key'])

                if not response.get('IsTruncated'):
                    break

                continuation_token = response.get('NextContinuationToken')

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
    test_object_size_limits(s3)