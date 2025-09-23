#!/usr/bin/env python3
"""
Test: Empty and Null Operations
Tests edge cases with empty values, null parameters, and minimal data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_empty_operations(s3_client: S3Client):
    """Test empty and null operations"""
    bucket_name = f's3-empty-ops-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Empty object operations
        print("Test 1: Empty object operations")
        try:
            # Create empty object
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-object',
                Body=b''
            )

            # Verify empty object exists
            head = s3_client.client.head_object(Bucket=bucket_name, Key='empty-object')
            if head['ContentLength'] == 0:
                results['passed'].append('Empty object creation')
                print("✓ Empty object: Created with 0 bytes")

            # Download empty object
            obj = s3_client.client.get_object(Bucket=bucket_name, Key='empty-object')
            content = obj['Body'].read()
            if len(content) == 0:
                results['passed'].append('Empty object download')
                print("✓ Empty object: Downloaded 0 bytes")

            # Copy empty object
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key='copied-empty',
                CopySource={'Bucket': bucket_name, 'Key': 'empty-object'}
            )

            copied_head = s3_client.client.head_object(Bucket=bucket_name, Key='copied-empty')
            if copied_head['ContentLength'] == 0:
                results['passed'].append('Empty object copy')
                print("✓ Empty object: Copy preserves zero length")

        except Exception as e:
            results['failed'].append(f'Empty object: {str(e)}')

        # Test 2: Empty strings in parameters
        print("\nTest 2: Empty strings in parameters")

        # Empty prefix in listing
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=''
            )
            results['passed'].append('Empty prefix listing')
            print("✓ Empty prefix: Listing accepted")
        except Exception as e:
            results['failed'].append(f'Empty prefix: {str(e)}')

        # Empty delimiter
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Delimiter=''
            )
            results['passed'].append('Empty delimiter')
            print("✓ Empty delimiter: Accepted")
        except Exception as e:
            results['failed'].append(f'Empty delimiter: {str(e)}')

        # Test 3: Empty metadata
        print("\nTest 3: Empty metadata")
        try:
            # Upload with empty metadata dict
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-metadata-dict',
                Body=b'test',
                Metadata={}
            )

            # Upload with empty metadata values
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-metadata-values',
                Body=b'test',
                Metadata={'empty-key': '', 'another-empty': ''}
            )

            # Verify empty metadata
            head = s3_client.client.head_object(Bucket=bucket_name, Key='empty-metadata-values')
            metadata = head.get('Metadata', {})

            if 'empty-key' in metadata and metadata['empty-key'] == '':
                results['passed'].append('Empty metadata values')
                print("✓ Empty metadata: Values preserved")

            results['passed'].append('Empty metadata dict')
            print("✓ Empty metadata: Dict accepted")

        except Exception as e:
            results['failed'].append(f'Empty metadata: {str(e)}')

        # Test 4: Empty key edge cases
        print("\nTest 4: Empty and minimal keys")

        # Test minimal key (single character)
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='a',
                Body=b'single char key'
            )
            results['passed'].append('Single character key')
            print("✓ Single char key: 'a' accepted")
        except Exception as e:
            results['failed'].append(f'Single char key: {str(e)}')

        # Test key with just forward slash
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='/',
                Body=b'slash key'
            )
            results['passed'].append('Slash key')
            print("✓ Slash key: '/' accepted")
        except Exception as e:
            if 'InvalidArgument' in str(e):
                results['passed'].append('Slash key rejected')
                print("✓ Slash key: Appropriately rejected")
            else:
                results['failed'].append(f'Slash key: {str(e)}')

        # Test 5: Empty content types and headers
        print("\nTest 5: Empty content types and headers")
        try:
            # Empty content type
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-content-type',
                Body=b'test',
                ContentType=''
            )

            head = s3_client.client.head_object(Bucket=bucket_name, Key='empty-content-type')
            content_type = head.get('ContentType', 'default')

            if content_type == '':
                results['passed'].append('Empty content type preserved')
                print("✓ Empty content type: Preserved as empty")
            else:
                results['passed'].append('Empty content type defaulted')
                print(f"✓ Empty content type: Defaulted to {content_type}")

        except Exception as e:
            results['failed'].append(f'Empty content type: {str(e)}')

        # Test 6: Empty ranges
        print("\nTest 6: Empty range requests")
        test_key = 'range-test'
        s3_client.client.put_object(Bucket=bucket_name, Key=test_key, Body=b'0123456789')

        # Invalid empty range
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=test_key,
                Range='bytes='
            )
            results['failed'].append('Empty range: Should have failed')
        except Exception as e:
            if 'InvalidRange' in str(e) or 'InvalidArgument' in str(e):
                results['passed'].append('Empty range rejected')
                print("✓ Empty range: Correctly rejected")
            else:
                results['failed'].append(f'Empty range: Wrong error')

        # Test 7: Empty conditional headers
        print("\nTest 7: Empty conditional headers")
        try:
            # Empty If-Match
            try:
                s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=test_key,
                    IfMatch=''
                )
                results['failed'].append('Empty If-Match: Should fail')
            except Exception as e:
                if 'InvalidArgument' in str(e) or 'PreconditionFailed' in str(e):
                    results['passed'].append('Empty If-Match rejected')
                    print("✓ Empty If-Match: Correctly rejected")

        except Exception as e:
            results['failed'].append(f'Empty conditionals: {str(e)}')

        # Test 8: Empty multipart operations
        print("\nTest 8: Empty multipart operations")
        try:
            # Create multipart upload
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key='empty-multipart'
            )['UploadId']

            # Try to complete with empty parts list
            try:
                s3_client.client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key='empty-multipart',
                    UploadId=upload_id,
                    MultipartUpload={'Parts': []}
                )
                results['failed'].append('Empty parts list: Should fail')
            except Exception as e:
                if 'InvalidRequest' in str(e) or 'InvalidPart' in str(e):
                    results['passed'].append('Empty parts list rejected')
                    print("✓ Empty parts list: Correctly rejected")

            # Clean up
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name,
                Key='empty-multipart',
                UploadId=upload_id
            )

        except Exception as e:
            results['failed'].append(f'Empty multipart: {str(e)}')

        # Test 9: Empty bucket operations
        print("\nTest 9: Empty bucket operations")

        # Create empty bucket and test operations
        empty_bucket = f's3-empty-bucket-{random_string(8).lower()}'
        try:
            s3_client.create_bucket(empty_bucket)

            # List empty bucket
            response = s3_client.client.list_objects_v2(Bucket=empty_bucket)
            if response.get('KeyCount', 0) == 0:
                results['passed'].append('Empty bucket listing')
                print("✓ Empty bucket: Lists correctly")

            # Delete empty bucket
            s3_client.delete_bucket(empty_bucket)
            results['passed'].append('Empty bucket deletion')
            print("✓ Empty bucket: Deletes successfully")

        except Exception as e:
            results['failed'].append(f'Empty bucket ops: {str(e)}')

        # Test 10: Null-like values
        print("\nTest 10: Null-like values")

        # Test with None-equivalent scenarios
        try:
            # Upload without optional parameters
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='minimal-params',
                Body=b'minimal test'
                # No ContentType, Metadata, etc.
            )

            head = s3_client.client.head_object(Bucket=bucket_name, Key='minimal-params')

            # Should have default values
            if 'ContentType' in head:
                results['passed'].append('Default content type assigned')
                print(f"✓ Default content type: {head['ContentType']}")

            results['passed'].append('Minimal parameters')
            print("✓ Minimal parameters: Upload successful")

        except Exception as e:
            results['failed'].append(f'Minimal params: {str(e)}')

        # Test 11: Empty file-like operations
        print("\nTest 11: Empty file-like operations")

        try:
            # Empty BytesIO stream
            empty_stream = io.BytesIO(b'')
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='empty-stream',
                Body=empty_stream
            )

            # Verify
            obj = s3_client.client.get_object(Bucket=bucket_name, Key='empty-stream')
            if len(obj['Body'].read()) == 0:
                results['passed'].append('Empty stream upload')
                print("✓ Empty stream: Handled correctly")

        except Exception as e:
            results['failed'].append(f'Empty stream: {str(e)}')

        # Test 12: Empty response scenarios
        print("\nTest 12: Empty response scenarios")

        try:
            # HEAD request on empty object
            head = s3_client.client.head_object(Bucket=bucket_name, Key='empty-object')

            # Should have headers but no body
            if 'ContentLength' in head and head['ContentLength'] == 0:
                results['passed'].append('Empty object HEAD response')
                print("✓ Empty object HEAD: Correct response")

            # GET request on empty object
            obj = s3_client.client.get_object(Bucket=bucket_name, Key='empty-object')
            content = obj['Body'].read()

            if len(content) == 0 and 'ContentLength' in obj:
                results['passed'].append('Empty object GET response')
                print("✓ Empty object GET: Correct response")

        except Exception as e:
            results['failed'].append(f'Empty responses: {str(e)}')

        # Summary
        print(f"\n=== Empty Operations Test Results ===")
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
    test_empty_operations(s3)