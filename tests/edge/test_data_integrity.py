#!/usr/bin/env python3
"""
Test: Data Integrity and Checksums
Tests data integrity verification, ETag validation, and corruption detection
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import hashlib
import random

def test_data_integrity(s3_client: S3Client):
    """Test data integrity and checksums"""
    bucket_name = f's3-integrity-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic data integrity
        print("Test 1: Basic data integrity")
        key1 = 'integrity-basic'
        test_data = b'This is test data for integrity verification' * 100

        try:
            # Upload data
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=test_data
            )

            upload_etag = response.get('ETag', '').strip('"')

            # Download and verify
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key1)
            downloaded_data = obj['Body'].read()
            download_etag = obj.get('ETag', '').strip('"')

            if downloaded_data == test_data:
                results['passed'].append('Basic data integrity')
                print("✓ Basic integrity: Data matches exactly")
            else:
                results['failed'].append('Basic integrity: Data mismatch')

            if upload_etag == download_etag:
                results['passed'].append('ETag consistency')
                print(f"✓ ETag consistency: {upload_etag}")
            else:
                results['failed'].append('ETag inconsistency')

        except Exception as e:
            results['failed'].append(f'Basic integrity: {str(e)}')

        # Test 2: Large data integrity
        print("\nTest 2: Large data integrity")
        key2 = 'integrity-large'

        try:
            # Generate 10MB of pseudo-random data
            random.seed(42)  # Reproducible random data
            large_data = bytes([random.randint(0, 255) for _ in range(10 * 1024 * 1024)])

            # Calculate MD5 for verification
            original_md5 = hashlib.md5(large_data).hexdigest()

            # Upload
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key2,
                Body=large_data
            )

            # Download
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key2)
            downloaded_data = obj['Body'].read()

            # Verify size
            if len(downloaded_data) == len(large_data):
                results['passed'].append('Large data size integrity')
                print(f"✓ Large size: {len(downloaded_data)} bytes")

            # Verify MD5
            downloaded_md5 = hashlib.md5(downloaded_data).hexdigest()
            if original_md5 == downloaded_md5:
                results['passed'].append('Large data MD5 integrity')
                print(f"✓ Large MD5: {original_md5}")
            else:
                results['failed'].append('Large data MD5 mismatch')

        except Exception as e:
            results['failed'].append(f'Large integrity: {str(e)}')

        # Test 3: ETag validation for different data
        print("\nTest 3: ETag validation for different data")

        test_cases = [
            (b'identical data', b'identical data', True),
            (b'different data 1', b'different data 2', False),
            (b'case sensitive', b'Case Sensitive', False),
            (b'trailing space', b'trailing space ', False),
            (b'', b'', True),  # Empty data
        ]

        for i, (data1, data2, should_match) in enumerate(test_cases):
            try:
                key_a = f'etag-test-{i}-a'
                key_b = f'etag-test-{i}-b'

                # Upload both data sets
                resp_a = s3_client.client.put_object(Bucket=bucket_name, Key=key_a, Body=data1)
                resp_b = s3_client.client.put_object(Bucket=bucket_name, Key=key_b, Body=data2)

                etag_a = resp_a.get('ETag', '').strip('"')
                etag_b = resp_b.get('ETag', '').strip('"')

                etags_match = (etag_a == etag_b)

                if etags_match == should_match:
                    results['passed'].append(f'ETag test case {i}')
                    print(f"✓ ETag case {i}: {'Match' if etags_match else 'Different'} (expected)")
                else:
                    results['failed'].append(f'ETag test case {i}: Unexpected result')

            except Exception as e:
                results['failed'].append(f'ETag case {i}: {str(e)}')

        # Test 4: Multipart upload integrity
        print("\nTest 4: Multipart upload integrity")
        key4 = 'multipart-integrity'

        try:
            # Create test data for multipart upload
            part_size = 5 * 1024 * 1024  # 5MB per part
            num_parts = 3
            parts_data = []

            for i in range(num_parts):
                # Create unique data for each part
                part_data = f'Part {i} data: '.encode() + bytes([i] * (part_size - 20))
                parts_data.append(part_data)

            # Calculate expected final data
            expected_data = b''.join(parts_data)
            expected_md5 = hashlib.md5(expected_data).hexdigest()

            # Initiate multipart upload
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=key4
            )['UploadId']

            # Upload parts
            parts = []
            for i, part_data in enumerate(parts_data, 1):
                response = s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=key4,
                    UploadId=upload_id,
                    PartNumber=i,
                    Body=io.BytesIO(part_data)
                )
                parts.append({'PartNumber': i, 'ETag': response['ETag']})

            # Complete upload
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key4,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            # Download and verify
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key4)
            final_data = obj['Body'].read()
            final_md5 = hashlib.md5(final_data).hexdigest()

            if len(final_data) == len(expected_data):
                results['passed'].append('Multipart size integrity')
                print(f"✓ Multipart size: {len(final_data)} bytes")

            if final_md5 == expected_md5:
                results['passed'].append('Multipart MD5 integrity')
                print(f"✓ Multipart MD5: {final_md5}")
            else:
                results['failed'].append('Multipart MD5 mismatch')

        except Exception as e:
            results['failed'].append(f'Multipart integrity: {str(e)}')

        # Test 5: Range request integrity
        print("\nTest 5: Range request integrity")
        key5 = 'range-integrity'

        try:
            # Create test data with pattern
            pattern_data = b'0123456789' * 1000  # 10KB with repeating pattern
            s3_client.client.put_object(Bucket=bucket_name, Key=key5, Body=pattern_data)

            # Test various ranges
            range_tests = [
                (0, 9, b'0123456789'),
                (100, 109, b'0123456789'),
                (500, 519, b'01234567890123456789'),
                (9990, 9999, b'0123456789'),
            ]

            for start, end, expected in range_tests:
                response = s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=key5,
                    Range=f'bytes={start}-{end}'
                )
                range_data = response['Body'].read()

                if range_data == expected:
                    results['passed'].append(f'Range {start}-{end}')
                else:
                    results['failed'].append(f'Range {start}-{end}: Data mismatch')

            print(f"✓ Range integrity: {len(range_tests)} ranges tested")

        except Exception as e:
            results['failed'].append(f'Range integrity: {str(e)}')

        # Test 6: Copy integrity
        print("\nTest 6: Copy operation integrity")
        source_key = 'copy-source'
        dest_key = 'copy-dest'

        try:
            # Create source with known data
            source_data = b'Copy integrity test data with special chars: \x00\xFF\x7F' * 1000
            source_md5 = hashlib.md5(source_data).hexdigest()

            s3_client.client.put_object(Bucket=bucket_name, Key=source_key, Body=source_data)

            # Copy object
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={'Bucket': bucket_name, 'Key': source_key}
            )

            # Verify copy
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=dest_key)
            copied_data = obj['Body'].read()
            copied_md5 = hashlib.md5(copied_data).hexdigest()

            if copied_md5 == source_md5:
                results['passed'].append('Copy integrity')
                print(f"✓ Copy integrity: MD5 {copied_md5}")
            else:
                results['failed'].append('Copy integrity: MD5 mismatch')

        except Exception as e:
            results['failed'].append(f'Copy integrity: {str(e)}')

        # Test 7: Binary data integrity
        print("\nTest 7: Binary data integrity")
        key7 = 'binary-integrity'

        try:
            # Create binary data with all byte values
            binary_data = bytes(range(256)) * 1000  # All possible byte values
            binary_md5 = hashlib.md5(binary_data).hexdigest()

            s3_client.client.put_object(Bucket=bucket_name, Key=key7, Body=binary_data)

            # Download and verify
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key7)
            downloaded_binary = obj['Body'].read()
            downloaded_md5 = hashlib.md5(downloaded_binary).hexdigest()

            if downloaded_md5 == binary_md5:
                results['passed'].append('Binary data integrity')
                print(f"✓ Binary integrity: All 256 byte values preserved")
            else:
                results['failed'].append('Binary data integrity: Corruption detected')

        except Exception as e:
            results['failed'].append(f'Binary integrity: {str(e)}')

        # Test 8: Streaming integrity
        print("\nTest 8: Streaming upload/download integrity")
        key8 = 'streaming-integrity'

        try:
            # Create streaming data source
            class ChecksumStream:
                def __init__(self, size):
                    self.size = size
                    self.position = 0
                    self.checksum = hashlib.md5()

                def read(self, length=-1):
                    if length == -1 or length > self.size - self.position:
                        length = self.size - self.position
                    if length <= 0:
                        return b''

                    # Generate predictable data
                    data = bytes([(self.position + i) % 256 for i in range(length)])
                    self.checksum.update(data)
                    self.position += length
                    return data

                def get_checksum(self):
                    return self.checksum.hexdigest()

            stream_size = 1024 * 1024  # 1MB
            stream = ChecksumStream(stream_size)

            # Upload from stream
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key8,
                Body=stream,
                ContentLength=stream_size
            )

            upload_checksum = stream.get_checksum()

            # Download and verify
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key8)
            downloaded_data = obj['Body'].read()
            download_checksum = hashlib.md5(downloaded_data).hexdigest()

            if upload_checksum == download_checksum:
                results['passed'].append('Streaming integrity')
                print(f"✓ Streaming integrity: {upload_checksum}")
            else:
                results['failed'].append('Streaming integrity: Checksum mismatch')

        except Exception as e:
            results['failed'].append(f'Streaming integrity: {str(e)}')

        # Test 9: ETag format validation
        print("\nTest 9: ETag format validation")

        try:
            # Test various object sizes to see ETag format
            sizes = [0, 1, 1024, 1024*1024, 5*1024*1024]

            for size in sizes:
                key = f'etag-format-{size}'
                data = b'E' * size

                response = s3_client.client.put_object(Bucket=bucket_name, Key=key, Body=data)
                etag = response.get('ETag', '')

                # ETag should be quoted hex string
                if etag.startswith('"') and etag.endswith('"'):
                    hex_part = etag.strip('"')
                    if all(c in '0123456789abcdefABCDEF-' for c in hex_part):
                        results['passed'].append(f'ETag format {size}')
                    else:
                        results['failed'].append(f'ETag format {size}: Invalid hex')
                else:
                    results['failed'].append(f'ETag format {size}: Not quoted')

            print(f"✓ ETag format: {len(sizes)} sizes validated")

        except Exception as e:
            results['failed'].append(f'ETag format: {str(e)}')

        # Summary
        print(f"\n=== Data Integrity Test Results ===")
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
    test_data_integrity(s3)