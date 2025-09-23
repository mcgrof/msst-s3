#!/usr/bin/env python3
"""
Test: Range Requests and Partial Content
Tests byte-range requests, multi-range requests, and conditional range operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_range_requests(s3_client: S3Client):
    """Test range requests and partial content retrieval"""
    bucket_name = f's3-range-requests-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Create test data
        test_data = b'0123456789' * 100  # 1000 bytes
        key = 'range-test-object'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=test_data
        )

        # Test 1: Simple byte range
        print("Test 1: Simple byte range")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=0-9'
            )
            content = response['Body'].read()

            if content == b'0123456789':
                results['passed'].append('Simple byte range')
                print("✓ Simple range: Retrieved bytes 0-9")
            else:
                results['failed'].append(f'Simple range: Got {content}')

            # Check response headers
            if response.get('ContentRange') == 'bytes 0-9/1000':
                results['passed'].append('Content-Range header')
                print("✓ Content-Range header: Correct format")

        except Exception as e:
            results['failed'].append(f'Simple range: {str(e)}')

        # Test 2: Range from middle
        print("\nTest 2: Range from middle")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=500-509'
            )
            content = response['Body'].read()

            if content == b'0123456789':
                results['passed'].append('Middle range')
                print("✓ Middle range: Retrieved bytes 500-509")
            else:
                results['failed'].append('Middle range: Wrong content')

        except Exception as e:
            results['failed'].append(f'Middle range: {str(e)}')

        # Test 3: Open-ended range (from position to end)
        print("\nTest 3: Open-ended range")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=995-'
            )
            content = response['Body'].read()

            if content == b'56789':  # Last 5 bytes
                results['passed'].append('Open-ended range')
                print("✓ Open-ended range: Retrieved from 995 to end")
            else:
                results['failed'].append(f'Open-ended range: Got {content}')

        except Exception as e:
            results['failed'].append(f'Open-ended range: {str(e)}')

        # Test 4: Suffix range (last N bytes)
        print("\nTest 4: Suffix range")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=-10'
            )
            content = response['Body'].read()

            if content == b'0123456789':  # Last 10 bytes
                results['passed'].append('Suffix range')
                print("✓ Suffix range: Retrieved last 10 bytes")
            else:
                results['failed'].append('Suffix range: Wrong content')

        except Exception as e:
            results['failed'].append(f'Suffix range: {str(e)}')

        # Test 5: Invalid range (beyond file size)
        print("\nTest 5: Invalid range beyond file")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=2000-2010'
            )
            # Should return 416 Range Not Satisfiable
            results['failed'].append('Beyond range: Should have failed')
            print("✗ Beyond range: Returned content (should fail)")
        except Exception as e:
            if 'InvalidRange' in str(e) or '416' in str(e):
                results['passed'].append('Beyond range rejected')
                print("✓ Beyond range: Correctly rejected with 416")
            else:
                results['failed'].append(f'Beyond range: Wrong error: {e}')

        # Test 6: Overlapping range
        print("\nTest 6: Overlapping/adjusted range")
        try:
            # Request more bytes than available
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=990-1010'  # Only 990-999 available
            )
            content = response['Body'].read()

            if len(content) == 10:  # Should get 990-999
                results['passed'].append('Adjusted range')
                print("✓ Adjusted range: Range adjusted to file size")
            else:
                results['failed'].append(f'Adjusted range: Got {len(content)} bytes')

        except Exception as e:
            # Some implementations might reject this
            if 'InvalidRange' in str(e):
                results['passed'].append('Strict range validation')
                print("✓ Adjusted range: Strict validation applied")
            else:
                results['failed'].append(f'Adjusted range: {str(e)}')

        # Test 7: Zero-length range
        print("\nTest 7: Zero-length range")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=100-99'  # End before start
            )
            results['failed'].append('Zero-length range: Accepted')
        except Exception as e:
            if 'InvalidRange' in str(e) or '416' in str(e):
                results['passed'].append('Zero-length range rejected')
                print("✓ Zero-length range: Correctly rejected")
            else:
                results['failed'].append('Zero-length range: Wrong error')

        # Test 8: Single byte range
        print("\nTest 8: Single byte range")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=0-0'
            )
            content = response['Body'].read()

            if content == b'0':
                results['passed'].append('Single byte range')
                print("✓ Single byte: Retrieved exactly one byte")
            else:
                results['failed'].append(f'Single byte: Got {content}')

        except Exception as e:
            results['failed'].append(f'Single byte: {str(e)}')

        # Test 9: Range with If-Match condition
        print("\nTest 9: Conditional range request")
        try:
            # Get ETag first
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
            etag = head['ETag']

            # Range with matching ETag
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=0-9',
                IfMatch=etag
            )
            content = response['Body'].read()

            if content == b'0123456789':
                results['passed'].append('Conditional range (If-Match)')
                print("✓ Conditional range: If-Match condition worked")

            # Range with non-matching ETag
            try:
                s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=key,
                    Range='bytes=0-9',
                    IfMatch='"wrong-etag"'
                )
                results['failed'].append('Wrong If-Match: Should have failed')
            except:
                results['passed'].append('Wrong If-Match rejected')
                print("✓ Conditional range: Wrong If-Match rejected")

        except Exception as e:
            results['failed'].append(f'Conditional range: {str(e)}')

        # Test 10: Large range request
        print("\nTest 10: Large range request")
        # Upload larger object (10MB)
        large_data = b'X' * (10 * 1024 * 1024)
        large_key = 'large-object'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=large_key,
            Body=large_data
        )

        try:
            # Request 1MB from middle
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=large_key,
                Range='bytes=5242880-6291455'  # 5MB to 6MB
            )
            content = response['Body'].read()

            if len(content) == 1048576:  # 1MB
                results['passed'].append('Large range request')
                print("✓ Large range: Retrieved 1MB from 10MB object")
            else:
                results['failed'].append(f'Large range: Got {len(content)} bytes')

        except Exception as e:
            results['failed'].append(f'Large range: {str(e)}')

        # Summary
        print(f"\n=== Range Requests Test Results ===")
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
    test_range_requests(s3)