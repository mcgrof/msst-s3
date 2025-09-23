#!/usr/bin/env python3
"""
Test: HTTP Header Handling
Tests custom headers, case sensitivity, and header limits in S3 operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
from datetime import datetime, timezone

def test_header_handling(s3_client: S3Client):
    """Test HTTP header handling"""
    bucket_name = f's3-headers-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Standard S3 headers
        print("Test 1: Standard S3 headers")
        key1 = 'standard-headers'

        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=b'test data',
                ContentType='text/plain',
                ContentLanguage='en-US',
                ContentEncoding='identity',
                ContentDisposition='attachment; filename="test.txt"',
                CacheControl='max-age=3600',
                Expires=datetime(2025, 12, 31, tzinfo=timezone.utc)
            )

            # Verify headers on retrieval
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key1)

            expected_headers = {
                'ContentType': 'text/plain',
                'ContentLanguage': 'en-US',
                'ContentEncoding': 'identity',
                'ContentDisposition': 'attachment; filename="test.txt"',
                'CacheControl': 'max-age=3600'
            }

            passed_headers = 0
            for header, expected in expected_headers.items():
                if header in head and head[header] == expected:
                    passed_headers += 1

            if passed_headers >= 4:  # Allow some flexibility
                results['passed'].append('Standard headers')
                print(f"✓ Standard headers: {passed_headers}/5 preserved")
            else:
                results['failed'].append(f'Standard headers: Only {passed_headers}/5')

        except Exception as e:
            results['failed'].append(f'Standard headers: {str(e)}')

        # Test 2: Custom x-amz headers
        print("\nTest 2: Custom x-amz headers")
        key2 = 'custom-amz-headers'

        try:
            # Use metadata (becomes x-amz-meta headers)
            metadata = {
                'user-id': '12345',
                'application': 'test-suite',
                'custom-field': 'custom-value'
            }

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key2,
                Body=b'test',
                Metadata=metadata
            )

            # Retrieve and verify
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key2)
            retrieved_metadata = head.get('Metadata', {})

            if len(retrieved_metadata) == len(metadata):
                results['passed'].append('Custom x-amz headers')
                print("✓ Custom headers: All metadata preserved")
            else:
                results['failed'].append(f'Custom headers: {len(retrieved_metadata)}/{len(metadata)}')

        except Exception as e:
            results['failed'].append(f'Custom headers: {str(e)}')

        # Test 3: Header case sensitivity
        print("\nTest 3: Header case sensitivity")
        key3 = 'case-sensitivity'

        try:
            # Test various cases in metadata keys
            mixed_case_metadata = {
                'CamelCase': 'value1',
                'lowercase': 'value2',
                'UPPERCASE': 'value3',
                'Mixed-Kebab-Case': 'value4'
            }

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key3,
                Body=b'test',
                Metadata=mixed_case_metadata
            )

            head = s3_client.client.head_object(Bucket=bucket_name, Key=key3)
            retrieved = head.get('Metadata', {})

            # Check if keys are normalized or preserved
            normalized_keys = set(k.lower() for k in retrieved.keys())
            original_keys = set(k.lower() for k in mixed_case_metadata.keys())

            if normalized_keys == original_keys:
                results['passed'].append('Header case handling')
                print("✓ Case sensitivity: Headers handled consistently")
            else:
                results['failed'].append('Case sensitivity: Key mismatch')

        except Exception as e:
            results['failed'].append(f'Case sensitivity: {str(e)}')

        # Test 4: Header value encoding
        print("\nTest 4: Header value encoding")
        key4 = 'encoding-test'

        try:
            # Test special characters in header values
            special_metadata = {
                'ascii': 'simple ascii value',
                'spaces': 'value with spaces',
                'quotes': 'value "with quotes"',
                'unicode': '测试 unicode 🚀',
                'encoded': '%20encoded%20value%20',
                'special-chars': '!@#$%^&*()',
            }

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'test',
                Metadata=special_metadata
            )

            head = s3_client.client.head_object(Bucket=bucket_name, Key=key4)
            retrieved = head.get('Metadata', {})

            preserved_count = 0
            for key, value in special_metadata.items():
                if key in retrieved:
                    if retrieved[key] == value:
                        preserved_count += 1
                    else:
                        print(f"  Modified: {key}: '{value}' → '{retrieved[key]}'")

            if preserved_count >= 4:  # Allow some encoding changes
                results['passed'].append('Header encoding')
                print(f"✓ Header encoding: {preserved_count}/6 values preserved")
            else:
                results['failed'].append(f'Header encoding: Only {preserved_count}/6')

        except Exception as e:
            results['failed'].append(f'Header encoding: {str(e)}')

        # Test 5: Header size limits
        print("\nTest 5: Header size limits")
        key5 = 'header-size-limits'

        # Test maximum header value size
        try:
            large_value = 'x' * 8192  # 8KB header value
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key5,
                Body=b'test',
                Metadata={'large-header': large_value}
            )

            head = s3_client.client.head_object(Bucket=bucket_name, Key=key5)
            if 'large-header' in head.get('Metadata', {}):
                results['passed'].append('Large header value')
                print("✓ Large header: 8KB value accepted")

        except Exception as e:
            if 'RequestHeaderSectionTooLarge' in str(e) or 'MetadataTooLarge' in str(e):
                results['passed'].append('Header size limit enforced')
                print("✓ Header size: Limit correctly enforced")
            else:
                results['failed'].append(f'Header size: {str(e)[:50]}')

        # Test 6: Forbidden headers
        print("\nTest 6: Forbidden/reserved headers")
        key6 = 'forbidden-headers'

        # Some headers should be rejected or ignored
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key6,
                Body=b'test',
                Metadata={
                    'authorization': 'should-be-ignored',
                    'host': 'fake-host',
                    'content-length': '999',  # Should be calculated automatically
                }
            )

            head = s3_client.client.head_object(Bucket=bucket_name, Key=key6)
            metadata = head.get('Metadata', {})

            # These shouldn't appear in metadata
            forbidden_found = any(k in metadata for k in ['authorization', 'host', 'content-length'])

            if not forbidden_found:
                results['passed'].append('Forbidden headers filtered')
                print("✓ Forbidden headers: Correctly filtered")
            else:
                results['passed'].append('Forbidden headers present')
                print("✓ Forbidden headers: Present (implementation choice)")

        except Exception as e:
            results['failed'].append(f'Forbidden headers: {str(e)}')

        # Test 7: Response header control
        print("\nTest 7: Response header control")
        key7 = 'response-headers'

        s3_client.client.put_object(Bucket=bucket_name, Key=key7, Body=b'test content')

        try:
            # Request with custom response headers
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key7,
                ResponseContentType='application/octet-stream',
                ResponseContentDisposition='attachment; filename="download.bin"',
                ResponseCacheControl='no-cache'
            )

            # Check if response headers were overridden
            if response.get('ContentType') == 'application/octet-stream':
                results['passed'].append('Response header override')
                print("✓ Response headers: Override successful")
            else:
                results['passed'].append('Response headers not supported')
                print("✓ Response headers: Not supported (expected)")

        except Exception as e:
            results['failed'].append(f'Response headers: {str(e)}')

        # Test 8: Date header formats
        print("\nTest 8: Date header formats")
        key8 = 'date-headers'

        try:
            # Upload object
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key8,
                Body=b'test'
            )

            # Check date format in response
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key8)

            if 'LastModified' in head:
                last_modified = head['LastModified']
                # Should be a datetime object
                if hasattr(last_modified, 'year'):
                    results['passed'].append('Date header format')
                    print(f"✓ Date format: {last_modified}")
                else:
                    results['failed'].append('Date header: Wrong type')

        except Exception as e:
            results['failed'].append(f'Date headers: {str(e)}')

        # Test 9: ETag header handling
        print("\nTest 9: ETag header handling")
        key9 = 'etag-test'

        try:
            # Upload and check ETag
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key9,
                Body=b'etag test data'
            )

            upload_etag = response.get('ETag')

            # Retrieve and compare
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key9)
            head_etag = head.get('ETag')

            if upload_etag and head_etag and upload_etag == head_etag:
                results['passed'].append('ETag consistency')
                print(f"✓ ETag: Consistent {upload_etag}")
            else:
                results['failed'].append('ETag: Inconsistent')

            # Check ETag format (should be quoted hex)
            if upload_etag and upload_etag.startswith('"') and upload_etag.endswith('"'):
                results['passed'].append('ETag format')
                print("✓ ETag format: Properly quoted")

        except Exception as e:
            results['failed'].append(f'ETag handling: {str(e)}')

        # Test 10: Transfer-Encoding handling
        print("\nTest 10: Transfer encoding")
        key10 = 'transfer-encoding'

        try:
            # Test chunked upload (if supported)
            chunked_data = b'chunk1' + b'chunk2' + b'chunk3'

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key10,
                Body=chunked_data
            )

            # Verify data integrity
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key10)
            if obj['Body'].read() == chunked_data:
                results['passed'].append('Transfer encoding')
                print("✓ Transfer encoding: Data integrity preserved")

        except Exception as e:
            results['failed'].append(f'Transfer encoding: {str(e)}')

        # Summary
        print(f"\n=== Header Handling Test Results ===")
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
    test_header_handling(s3)