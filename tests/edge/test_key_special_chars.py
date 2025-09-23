#!/usr/bin/env python3
"""
Test: Key Special Characters and Reserved Names
Tests S3's handling of special characters, reserved names, and path traversal attempts
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_key_special_chars(s3_client: S3Client):
    """Test special characters and reserved names in keys"""
    bucket_name = f's3-special-keys-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test cases with special characters
        test_cases = [
            # URL encoding challenges
            ('test%20space.txt', b'percent encoded space', 'Percent encoded space'),
            ('test+plus.txt', b'plus sign', 'Plus sign in key'),
            ('test?query=param', b'query string', 'Query string characters'),
            ('test#fragment', b'fragment', 'Fragment identifier'),

            # Path traversal attempts
            ('../../../etc/passwd', b'path traversal', 'Path traversal attempt'),
            ('..\\..\\..\\windows\\system32', b'windows path', 'Windows path traversal'),
            ('./././test.txt', b'dot slash', 'Dot slash sequences'),

            # Reserved names
            ('CON', b'windows reserved', 'Windows reserved name CON'),
            ('PRN', b'printer reserved', 'Windows reserved name PRN'),
            ('AUX', b'auxiliary reserved', 'Windows reserved name AUX'),
            ('NUL', b'null device', 'Windows reserved name NUL'),

            # Control characters
            ('test\x00null.txt', b'null byte', 'Null byte in key'),
            ('test\r\nCRLF.txt', b'crlf', 'CRLF in key'),
            ('test\x1bescape.txt', b'escape char', 'Escape character'),

            # Extreme lengths
            ('a' * 1024, b'max key length', 'Maximum key length (1024)'),
            ('/', b'single slash', 'Single slash key'),
            ('//', b'double slash', 'Double slash key'),
            ('///', b'triple slash', 'Triple slash key'),

            # Mixed encodings
            ('test%2F%2Fencoded.txt', b'double encoded', 'Double URL encoding'),
            ('test%252Ftriple.txt', b'triple encoded', 'Triple URL encoding'),
        ]

        for key, data, description in test_cases:
            try:
                # Test upload
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=io.BytesIO(data)
                )

                # Test retrieval with exact key
                response = s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=key
                )
                retrieved = response['Body'].read()

                if retrieved == data:
                    results['passed'].append(description)
                    print(f"✓ {description}: PASSED")
                else:
                    results['failed'].append(f"{description}: Data mismatch")
                    print(f"✗ {description}: Data mismatch")

            except Exception as e:
                # Some keys might be rejected - that's also a valid behavior
                if 'InvalidArgument' in str(e) or 'InvalidRequest' in str(e):
                    results['passed'].append(f"{description} (rejected)")
                    print(f"✓ {description}: Correctly rejected")
                else:
                    results['failed'].append(f"{description}: {str(e)}")
                    print(f"✗ {description}: {str(e)}")

        # Test key length limit
        try:
            too_long_key = 'x' * 1025  # Over 1024 limit
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=too_long_key,
                Body=b'test'
            )
            results['failed'].append('Key length limit: Should have been rejected')
            print("✗ Key length limit: Accepted key over 1024 chars")
        except Exception as e:
            if 'KeyTooLong' in str(e) or 'InvalidRequest' in str(e):
                results['passed'].append('Key length limit enforced')
                print("✓ Key length limit: Correctly enforced")
            else:
                results['failed'].append(f'Key length limit: Unexpected error: {e}')

        # Summary
        print(f"\n=== Key Special Characters Test Results ===")
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
    test_key_special_chars(s3)