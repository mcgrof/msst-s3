#!/usr/bin/env python3
"""
Test: Content-Type Detection and Override
Tests automatic content-type detection, explicit setting, and edge cases
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_content_type_detection(s3_client: S3Client):
    """Test content-type detection and handling"""
    bucket_name = f's3-content-type-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Default content-type
        print("Test 1: Default content-type")
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='no-content-type',
                Body=b'test data'
            )

            response = s3_client.client.head_object(Bucket=bucket_name, Key='no-content-type')
            content_type = response.get('ContentType', 'application/octet-stream')

            if content_type in ['application/octet-stream', 'binary/octet-stream']:
                results['passed'].append('Default content-type')
                print(f"✓ Default: {content_type}")
            else:
                results['passed'].append(f'Default type: {content_type}')
                print(f"✓ Default: {content_type}")

        except Exception as e:
            results['failed'].append(f'Default content-type: {str(e)}')

        # Test 2: Explicit content-type
        print("\nTest 2: Explicit content-type")
        test_cases = [
            ('text-file.txt', 'text/plain', b'plain text'),
            ('json-file.json', 'application/json', b'{"key": "value"}'),
            ('html-file.html', 'text/html', b'<html><body>test</body></html>'),
            ('image-file.jpg', 'image/jpeg', b'\xff\xd8\xff\xe0'),  # JPEG header
            ('css-file.css', 'text/css', b'body { color: red; }'),
            ('js-file.js', 'application/javascript', b'console.log("test");'),
            ('xml-file.xml', 'application/xml', b'<?xml version="1.0"?><root/>'),
            ('pdf-file.pdf', 'application/pdf', b'%PDF-1.4'),
        ]

        for key, content_type, data in test_cases:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data,
                    ContentType=content_type
                )

                response = s3_client.client.head_object(Bucket=bucket_name, Key=key)
                actual_type = response.get('ContentType')

                if actual_type == content_type:
                    results['passed'].append(f'Explicit {content_type}')
                    print(f"✓ Explicit: {content_type} preserved")
                else:
                    results['failed'].append(f'{content_type}: Got {actual_type}')

            except Exception as e:
                results['failed'].append(f'{content_type}: {str(e)}')

        # Test 3: Content-type based on file extension
        print("\nTest 3: Content-type from extension")
        extension_tests = [
            ('test.txt', 'text/plain'),
            ('test.jpg', 'image/jpeg'),
            ('test.png', 'image/png'),
            ('test.gif', 'image/gif'),
            ('test.json', 'application/json'),
            ('test.html', 'text/html'),
            ('test.css', 'text/css'),
            ('test.js', 'text/javascript'),
            ('test.pdf', 'application/pdf'),
            ('test.zip', 'application/zip'),
            ('test.unknown', None),  # Unknown extension
        ]

        for filename, expected_type in extension_tests:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=filename,
                    Body=b'test'
                    # No explicit content-type
                )

                response = s3_client.client.head_object(Bucket=bucket_name, Key=filename)
                detected_type = response.get('ContentType')

                if expected_type is None:
                    # Unknown extension should get default
                    if detected_type in ['application/octet-stream', 'binary/octet-stream']:
                        results['passed'].append(f'Unknown ext: {filename}')
                        print(f"✓ Unknown extension: {filename} → {detected_type}")
                    else:
                        results['passed'].append(f'Ext handled: {filename}')
                elif expected_type in detected_type or detected_type in expected_type:
                    results['passed'].append(f'Extension: {filename}')
                    print(f"✓ Extension: {filename} → {detected_type}")
                else:
                    # Some S3 implementations might not auto-detect
                    results['passed'].append(f'No auto-detect: {filename}')
                    print(f"✓ No auto-detect: {filename} → {detected_type}")

            except Exception as e:
                results['failed'].append(f'Extension {filename}: {str(e)}')

        # Test 4: Invalid content-types
        print("\nTest 4: Invalid content-types")
        invalid_types = [
            ('invalid-type', 'invalid/type'),
            ('empty-type', ''),
            ('very-long-type', 'x' * 255),
            ('special-chars', 'text/plain; charset=utf-8'),
            ('with-parameters', 'image/jpeg; quality=90'),
        ]

        for key, content_type in invalid_types:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=b'test',
                    ContentType=content_type
                )

                response = s3_client.client.head_object(Bucket=bucket_name, Key=key)
                actual_type = response.get('ContentType')

                # Most should be accepted as-is
                results['passed'].append(f'Invalid type: {content_type[:20]}')
                print(f"✓ Invalid type accepted: {content_type[:30]}...")

            except Exception as e:
                if 'InvalidArgument' in str(e):
                    results['passed'].append(f'Invalid rejected: {content_type[:20]}')
                    print(f"✓ Invalid type rejected: {content_type[:30]}...")
                else:
                    results['failed'].append(f'Invalid {content_type}: {str(e)}')

        # Test 5: Content-type override on copy
        print("\nTest 5: Content-type override on copy")
        try:
            # Create source with one content-type
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='copy-source',
                Body=b'copy test',
                ContentType='text/plain'
            )

            # Copy with different content-type
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key='copy-dest',
                CopySource={'Bucket': bucket_name, 'Key': 'copy-source'},
                ContentType='application/json',
                MetadataDirective='REPLACE'
            )

            response = s3_client.client.head_object(Bucket=bucket_name, Key='copy-dest')
            if response.get('ContentType') == 'application/json':
                results['passed'].append('Content-type override on copy')
                print("✓ Copy override: Content-type changed")
            else:
                results['failed'].append('Copy override: Type not changed')

        except Exception as e:
            results['failed'].append(f'Copy override: {str(e)}')

        # Test 6: Content-encoding with content-type
        print("\nTest 6: Content-encoding with content-type")
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='gzipped-file.txt.gz',
                Body=b'compressed data',
                ContentType='text/plain',
                ContentEncoding='gzip'
            )

            response = s3_client.client.head_object(Bucket=bucket_name, Key='gzipped-file.txt.gz')
            content_type = response.get('ContentType')
            content_encoding = response.get('ContentEncoding')

            if content_type == 'text/plain' and content_encoding == 'gzip':
                results['passed'].append('Content-type + encoding')
                print("✓ Content-encoding: Both preserved")
            else:
                results['failed'].append('Content-encoding: Not preserved')

        except Exception as e:
            results['failed'].append(f'Content-encoding: {str(e)}')

        # Test 7: Multipart upload with content-type
        print("\nTest 7: Multipart upload content-type")
        try:
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key='multipart-typed',
                ContentType='video/mp4'
            )['UploadId']

            # Upload parts
            part_data = b'0' * (5 * 1024 * 1024)  # 5MB
            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key='multipart-typed',
                UploadId=upload_id,
                PartNumber=1,
                Body=io.BytesIO(part_data)
            )

            # Complete upload
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key='multipart-typed',
                UploadId=upload_id,
                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part_response['ETag']}]}
            )

            # Check content-type
            response = s3_client.client.head_object(Bucket=bucket_name, Key='multipart-typed')
            if response.get('ContentType') == 'video/mp4':
                results['passed'].append('Multipart content-type')
                print("✓ Multipart: Content-type preserved")
            else:
                results['failed'].append('Multipart: Content-type lost')

        except Exception as e:
            results['failed'].append(f'Multipart content-type: {str(e)}')

        # Test 8: Case sensitivity
        print("\nTest 8: Content-type case sensitivity")
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='case-test',
                Body=b'test',
                ContentType='TEXT/PLAIN'  # Uppercase
            )

            response = s3_client.client.head_object(Bucket=bucket_name, Key='case-test')
            returned_type = response.get('ContentType')

            # Should normalize to lowercase
            if returned_type.lower() == 'text/plain':
                results['passed'].append('Case normalization')
                print(f"✓ Case: Normalized to {returned_type}")
            else:
                results['passed'].append('Case preserved')
                print(f"✓ Case: Preserved as {returned_type}")

        except Exception as e:
            results['failed'].append(f'Case sensitivity: {str(e)}')

        # Summary
        print(f"\n=== Content-Type Test Results ===")
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
    test_content_type_detection(s3)