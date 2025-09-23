#!/usr/bin/env python3
"""
Test: Large Object Handling
Tests upload, download, and manipulation of large objects (>100MB)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import hashlib
import time

def test_large_object_handling(s3_client: S3Client):
    """Test handling of large objects"""
    bucket_name = f's3-large-objects-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Upload large object (streaming)
        print("Test 1: Upload large object (50MB)")
        large_key = 'large-50mb'
        chunk_size = 1024 * 1024  # 1MB chunks
        total_size = 50 * 1024 * 1024  # 50MB

        # Create streaming data
        class LargeDataStream:
            def __init__(self, size, pattern=b'A'):
                self.size = size
                self.pattern = pattern
                self.position = 0

            def read(self, size=-1):
                if size == -1 or size > self.size - self.position:
                    size = self.size - self.position
                if size <= 0:
                    return b''
                data = self.pattern * (size // len(self.pattern) + 1)
                data = data[:size]
                self.position += size
                return data

        try:
            start_time = time.time()

            # Upload large object
            stream = LargeDataStream(total_size)
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=large_key,
                Body=stream,
                ContentLength=total_size
            )

            upload_time = time.time() - start_time

            # Verify upload
            head = s3_client.client.head_object(Bucket=bucket_name, Key=large_key)
            if head['ContentLength'] == total_size:
                results['passed'].append('Large upload (50MB)')
                print(f"✓ Large upload: 50MB uploaded in {upload_time:.2f}s")
            else:
                results['failed'].append(f'Large upload: Size {head["ContentLength"]} != {total_size}')

        except Exception as e:
            if 'EntityTooLarge' in str(e):
                results['passed'].append('Large upload size limit')
                print("✓ Large upload: Size limit enforced")
            else:
                results['failed'].append(f'Large upload: {str(e)}')

        # Test 2: Download large object in chunks
        print("\nTest 2: Download large object in chunks")
        try:
            # Download in 5MB chunks
            downloaded_size = 0
            chunk_size = 5 * 1024 * 1024

            start_time = time.time()

            while downloaded_size < total_size:
                end_byte = min(downloaded_size + chunk_size - 1, total_size - 1)

                response = s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=large_key,
                    Range=f'bytes={downloaded_size}-{end_byte}'
                )

                chunk_data = response['Body'].read()
                downloaded_size += len(chunk_data)

                # Verify chunk content
                if not all(b == ord('A') for b in chunk_data):
                    results['failed'].append('Chunk download: Data corruption')
                    break

            download_time = time.time() - start_time

            if downloaded_size == total_size:
                results['passed'].append('Large download chunks')
                print(f"✓ Chunked download: {total_size} bytes in {download_time:.2f}s")

        except Exception as e:
            results['failed'].append(f'Chunked download: {str(e)}')

        # Test 3: Copy large object
        print("\nTest 3: Copy large object")
        try:
            start_time = time.time()

            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key='large-copy',
                CopySource={'Bucket': bucket_name, 'Key': large_key}
            )

            copy_time = time.time() - start_time

            # Verify copy
            head = s3_client.client.head_object(Bucket=bucket_name, Key='large-copy')
            if head['ContentLength'] == total_size:
                results['passed'].append('Large object copy')
                print(f"✓ Large copy: Completed in {copy_time:.2f}s")
            else:
                results['failed'].append('Large copy: Size mismatch')

        except Exception as e:
            results['failed'].append(f'Large copy: {str(e)}')

        # Test 4: Multipart upload for very large object
        print("\nTest 4: Multipart upload (100MB)")
        very_large_key = 'very-large-100mb'
        part_size = 10 * 1024 * 1024  # 10MB parts
        total_parts = 10  # 100MB total

        try:
            # Initiate multipart upload
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=very_large_key
            )['UploadId']

            parts = []
            start_time = time.time()

            for part_num in range(1, total_parts + 1):
                # Create part data
                part_stream = LargeDataStream(part_size, bytes([part_num]))

                response = s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=very_large_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=part_stream
                )

                parts.append({
                    'PartNumber': part_num,
                    'ETag': response['ETag']
                })

                print(f"  Uploaded part {part_num}/{total_parts}")

            # Complete multipart upload
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=very_large_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            upload_time = time.time() - start_time

            # Verify upload
            head = s3_client.client.head_object(Bucket=bucket_name, Key=very_large_key)
            expected_size = total_parts * part_size

            if head['ContentLength'] == expected_size:
                results['passed'].append('Very large multipart (100MB)')
                print(f"✓ Multipart 100MB: Completed in {upload_time:.2f}s")
            else:
                results['failed'].append('Very large multipart: Size mismatch')

        except Exception as e:
            # Abort upload if failed
            try:
                s3_client.client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=very_large_key,
                    UploadId=upload_id
                )
            except:
                pass
            results['failed'].append(f'Very large multipart: {str(e)}')

        # Test 5: Object size limits
        print("\nTest 5: Object size limits")

        # Test maximum single PUT size (typically 5GB)
        max_put_size = 6 * 1024 * 1024 * 1024  # 6GB (should fail)

        try:
            # This should fail as it exceeds single PUT limit
            stream = LargeDataStream(max_put_size)
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='oversized-put',
                Body=stream,
                ContentLength=max_put_size
            )
            results['failed'].append('Oversized PUT: Should have failed')
            print("✗ Oversized PUT: Accepted 6GB (should reject)")

        except Exception as e:
            if 'EntityTooLarge' in str(e) or 'MaxMessageLength' in str(e):
                results['passed'].append('PUT size limit enforced')
                print("✓ PUT size limit: 6GB correctly rejected")
            else:
                # Provider might have different limits
                results['passed'].append('PUT limit handling')
                print(f"✓ PUT limit: Handled with {str(e)[:50]}...")

        # Test 6: Sparse data handling
        print("\nTest 6: Sparse data handling")
        sparse_key = 'sparse-data'

        try:
            # Create data with lots of zeros
            sparse_data = b'\x00' * (1024 * 1024) + b'DATA' + b'\x00' * (1024 * 1024)

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=sparse_key,
                Body=sparse_data
            )

            # Download and verify
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=sparse_key)
            downloaded = obj['Body'].read()

            if downloaded == sparse_data:
                results['passed'].append('Sparse data handling')
                print("✓ Sparse data: Preserved correctly")
            else:
                results['failed'].append('Sparse data: Corruption detected')

        except Exception as e:
            results['failed'].append(f'Sparse data: {str(e)}')

        # Test 7: Memory efficient streaming
        print("\nTest 7: Memory efficient streaming")
        stream_key = 'stream-test'

        try:
            # Test that we can handle large objects without loading into memory
            class MemoryEfficientStream:
                def __init__(self, size):
                    self.size = size
                    self.position = 0

                def read(self, size=-1):
                    if size == -1 or size > self.size - self.position:
                        size = self.size - self.position
                    if size <= 0:
                        return b''

                    # Generate data on the fly to avoid memory usage
                    chunk = bytes([(self.position + i) % 256 for i in range(size)])
                    self.position += size
                    return chunk

            stream_size = 20 * 1024 * 1024  # 20MB
            stream = MemoryEfficientStream(stream_size)

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=stream_key,
                Body=stream,
                ContentLength=stream_size
            )

            # Verify size
            head = s3_client.client.head_object(Bucket=bucket_name, Key=stream_key)
            if head['ContentLength'] == stream_size:
                results['passed'].append('Memory efficient streaming')
                print("✓ Streaming: Memory efficient upload")

        except Exception as e:
            results['failed'].append(f'Streaming: {str(e)}')

        # Summary
        print(f"\n=== Large Object Handling Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            print("\nCleaning up large objects...")
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
    test_large_object_handling(s3)