#!/usr/bin/env python3
"""
Test: URL Encoding and Path Handling
Tests URL encoding, path separators, and special characters in S3 operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import urllib.parse

def test_url_encoding(s3_client: S3Client):
    """Test URL encoding and path handling"""
    bucket_name = f's3-url-encoding-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic URL encoding characters
        print("Test 1: Basic URL encoding characters")
        encoding_tests = [
            ('space file.txt', 'File with space'),
            ('plus+file.txt', 'File with plus'),
            ('percent%file.txt', 'File with percent'),
            ('question?file.txt', 'File with question mark'),
            ('hash#file.txt', 'File with hash'),
            ('ampersand&file.txt', 'File with ampersand'),
            ('equals=file.txt', 'File with equals'),
            ('at@file.txt', 'File with at symbol'),
        ]

        for key, description in encoding_tests:
            try:
                # Upload
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'{description} content'.encode()
                )

                # Retrieve
                response = s3_client.client.get_object(Bucket=bucket_name, Key=key)
                content = response['Body'].read().decode()

                if f'{description} content' in content:
                    results['passed'].append(f'URL encoding: {description}')
                    print(f"✓ {description}: Handled correctly")
                else:
                    results['failed'].append(f'URL encoding: {description}')

            except Exception as e:
                # Some characters might be rejected
                if 'InvalidArgument' in str(e) or 'InvalidKeyName' in str(e):
                    results['passed'].append(f'{description} rejected')
                    print(f"✓ {description}: Appropriately rejected")
                else:
                    results['failed'].append(f'{description}: {str(e)[:50]}')

        # Test 2: Path separator variations
        print("\nTest 2: Path separator variations")
        path_tests = [
            ('normal/path/file.txt', 'Normal forward slash'),
            ('path//double//slash.txt', 'Double forward slash'),
            ('path///triple///slash.txt', 'Triple forward slash'),
            ('/leading/slash.txt', 'Leading slash'),
            ('trailing/slash/.txt', 'Trailing slash in dir'),
            ('./relative/path.txt', 'Relative path'),
            ('../parent/path.txt', 'Parent directory'),
        ]

        for key, description in path_tests:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'{description} data'.encode()
                )

                # List to see how path is stored
                response = s3_client.client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=key[:5] if len(key) > 5 else key
                )

                found = any(obj['Key'] == key for obj in response.get('Contents', []))
                if found:
                    results['passed'].append(f'Path: {description}')
                    print(f"✓ {description}: Stored as-is")
                else:
                    results['failed'].append(f'Path: {description}')

            except Exception as e:
                results['failed'].append(f'Path {description}: {str(e)[:50]}')

        # Test 3: Pre-encoded vs raw characters
        print("\nTest 3: Pre-encoded vs raw characters")

        # Test space character
        raw_space_key = 'test space.txt'
        encoded_space_key = 'test%20space.txt'

        try:
            # Upload with raw space
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=raw_space_key,
                Body=b'raw space content'
            )

            # Upload with encoded space
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=encoded_space_key,
                Body=b'encoded space content'
            )

            # List objects to see how they're stored
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='test'
            )

            keys = [obj['Key'] for obj in response.get('Contents', [])]

            if raw_space_key in keys and encoded_space_key in keys:
                results['passed'].append('Raw vs encoded: Different objects')
                print("✓ Raw vs encoded: Treated as different keys")
            elif raw_space_key in keys or encoded_space_key in keys:
                results['passed'].append('Raw vs encoded: One form')
                print("✓ Raw vs encoded: Normalized to one form")
            else:
                results['failed'].append('Raw vs encoded: Both missing')

        except Exception as e:
            results['failed'].append(f'Raw vs encoded: {str(e)}')

        # Test 4: International characters
        print("\nTest 4: International characters")
        intl_tests = [
            ('café.txt', 'French accents'),
            ('naïve.txt', 'Diaeresis'),
            ('résumé.txt', 'Mixed accents'),
            ('файл.txt', 'Cyrillic'),
            ('文件.txt', 'Chinese'),
            ('ファイル.txt', 'Japanese'),
            ('파일.txt', 'Korean'),
        ]

        for key, description in intl_tests:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'{description} content'.encode()
                )

                # Retrieve by exact key
                response = s3_client.client.get_object(Bucket=bucket_name, Key=key)
                content = response['Body'].read().decode()

                if description in content:
                    results['passed'].append(f'International: {description}')
                    print(f"✓ {description}: UTF-8 handled")
                else:
                    results['failed'].append(f'International: {description}')

            except Exception as e:
                if 'InvalidArgument' in str(e):
                    results['passed'].append(f'{description} restricted')
                    print(f"✓ {description}: Appropriately restricted")
                else:
                    results['failed'].append(f'{description}: {str(e)[:50]}')

        # Test 5: Reserved characters in different contexts
        print("\nTest 5: Reserved characters in contexts")

        # Test in different parts of the path
        reserved_tests = [
            ('dir with spaces/file.txt', 'Space in directory'),
            ('dir%20encoded/file.txt', 'Encoded in directory'),
            ('normal/file with spaces.txt', 'Space in filename'),
            ('normal/file%20encoded.txt', 'Encoded in filename'),
            ('dir/subdir/file.ext', 'Multiple levels'),
        ]

        for key, description in reserved_tests:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'{description} content'.encode()
                )

                # Test listing with prefix
                dir_part = key.split('/')[0]
                response = s3_client.client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=dir_part,
                    Delimiter='/'
                )

                # Should appear in listing
                found_in_listing = any(
                    obj['Key'].startswith(dir_part)
                    for obj in response.get('Contents', [])
                )

                if found_in_listing:
                    results['passed'].append(f'Context: {description}')
                    print(f"✓ {description}: Listed correctly")

            except Exception as e:
                results['failed'].append(f'Context {description}: {str(e)[:50]}')

        # Test 6: Query parameter characters
        print("\nTest 6: Query parameter characters")

        query_char_tests = [
            ('file?param=value.txt', 'Query-like syntax'),
            ('file&param1=val&param2=val.txt', 'Multiple parameters'),
            ('file.txt?v=1.0', 'Version parameter'),
            ('file.txt#section', 'Fragment identifier'),
        ]

        for key, description in query_char_tests:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=b'query test content'
                )

                # Try to retrieve by exact key
                response = s3_client.client.get_object(Bucket=bucket_name, Key=key)

                results['passed'].append(f'Query chars: {description}')
                print(f"✓ {description}: Accepted")

            except Exception as e:
                if 'InvalidArgument' in str(e):
                    results['passed'].append(f'{description} rejected')
                    print(f"✓ {description}: Appropriately rejected")
                else:
                    results['failed'].append(f'{description}: {str(e)[:50]}')

        # Test 7: Case sensitivity in keys
        print("\nTest 7: Case sensitivity in keys")

        case_tests = [
            ('CamelCase.txt', 'Camel case'),
            ('camelcase.txt', 'Lower case'),
            ('UPPERCASE.TXT', 'Upper case'),
            ('MiXeD-CaSe.TxT', 'Mixed case'),
        ]

        for key, description in case_tests:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'{description} content'.encode()
                )

                # Try to retrieve with different case
                alt_key = key.swapcase()
                try:
                    s3_client.client.get_object(Bucket=bucket_name, Key=alt_key)
                    results['failed'].append(f'Case insensitive: {description}')
                except:
                    results['passed'].append(f'Case sensitive: {description}')
                    print(f"✓ {description}: Case sensitive")

            except Exception as e:
                results['failed'].append(f'Case test {description}: {str(e)[:50]}')

        # Test 8: Maximum URL length
        print("\nTest 8: Maximum URL length")

        try:
            # Create very long key (approaching URL limits)
            long_key = 'very/long/path/' + 'x' * 800 + '.txt'

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=long_key,
                Body=b'long path content'
            )

            # Try to retrieve
            response = s3_client.client.get_object(Bucket=bucket_name, Key=long_key)

            if response['Body'].read() == b'long path content':
                results['passed'].append('Long URL handling')
                print("✓ Long URL: Handled successfully")

        except Exception as e:
            if 'KeyTooLong' in str(e) or 'RequestURITooLong' in str(e):
                results['passed'].append('Long URL limit enforced')
                print("✓ Long URL: Limit enforced")
            else:
                results['failed'].append(f'Long URL: {str(e)[:50]}')

        # Summary
        print(f"\n=== URL Encoding Test Results ===")
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
    test_url_encoding(s3)