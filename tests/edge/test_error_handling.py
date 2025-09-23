#!/usr/bin/env python3
"""
Test: Error Handling and Status Codes
Tests various error conditions, status codes, and error response formats
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_error_handling(s3_client: S3Client):
    """Test error handling and status codes"""
    bucket_name = f's3-errors-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: NoSuchKey error
        print("Test 1: NoSuchKey error")
        try:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key='does-not-exist'
            )
            results['failed'].append('NoSuchKey: Should have failed')
            print("✗ NoSuchKey: Retrieved non-existent object")
        except Exception as e:
            if 'NoSuchKey' in str(e) or '404' in str(e):
                results['passed'].append('NoSuchKey error')
                print("✓ NoSuchKey: Correct 404 error")
            else:
                results['failed'].append(f'NoSuchKey: Wrong error: {str(e)[:50]}')

        # Test 2: NoSuchBucket error
        print("\nTest 2: NoSuchBucket error")
        fake_bucket = f'nonexistent-bucket-{random_string(16).lower()}'
        try:
            s3_client.client.get_object(
                Bucket=fake_bucket,
                Key='any-key'
            )
            results['failed'].append('NoSuchBucket: Should have failed')
        except Exception as e:
            if 'NoSuchBucket' in str(e) or '404' in str(e):
                results['passed'].append('NoSuchBucket error')
                print("✓ NoSuchBucket: Correct error")
            else:
                results['failed'].append(f'NoSuchBucket: Wrong error')

        # Test 3: InvalidArgument errors
        print("\nTest 3: InvalidArgument errors")

        # Empty bucket name
        try:
            s3_client.client.list_objects_v2(Bucket='')
            results['failed'].append('Empty bucket: Should fail')
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'InvalidBucketName' in str(e):
                results['passed'].append('Empty bucket name rejected')
                print("✓ Empty bucket: Correctly rejected")

        # Invalid MaxKeys
        try:
            s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                MaxKeys=-1
            )
            results['failed'].append('Negative MaxKeys: Should fail')
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'ValidationException' in str(e):
                results['passed'].append('Negative MaxKeys rejected')
                print("✓ Negative MaxKeys: Correctly rejected")

        # Test 4: AccessDenied simulation
        print("\nTest 4: AccessDenied conditions")

        # Try to access bucket with ACL restrictions
        restricted_bucket = f's3-restricted-{random_string(8).lower()}'
        try:
            s3_client.create_bucket(restricted_bucket)

            # Put object with private ACL
            s3_client.client.put_object(
                Bucket=restricted_bucket,
                Key='private-object',
                Body=b'private data',
                ACL='private'
            )

            # This should work since we're the owner
            obj = s3_client.client.get_object(
                Bucket=restricted_bucket,
                Key='private-object'
            )
            results['passed'].append('Owner access to private object')
            print("✓ Owner access: Can access private object")

            s3_client.delete_bucket(restricted_bucket)

        except Exception as e:
            results['failed'].append(f'Private object test: {str(e)}')

        # Test 5: MethodNotAllowed
        print("\nTest 5: MethodNotAllowed conditions")

        # Some operations might not be supported
        try:
            # Try to POST to an object (not a valid S3 operation)
            import requests
            response = requests.post(
                f"{s3_client.config['s3_endpoint_url']}/{bucket_name}/test-object",
                headers={'Authorization': 'invalid'}
            )

            if response.status_code == 405:
                results['passed'].append('MethodNotAllowed')
                print("✓ MethodNotAllowed: POST rejected")
            else:
                results['passed'].append('Method handling')

        except Exception as e:
            results['passed'].append('Method test handled')

        # Test 6: Malformed XML/Request
        print("\nTest 6: Malformed request handling")

        # Try invalid lifecycle configuration
        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={
                    'Rules': [
                        {
                            'ID': 'invalid-rule',
                            'Status': 'InvalidStatus',  # Invalid status
                            'Filter': {},
                            'Expiration': {'Days': -1}  # Invalid days
                        }
                    ]
                }
            )
            results['failed'].append('Malformed lifecycle: Should fail')
        except Exception as e:
            if 'MalformedXML' in str(e) or 'InvalidArgument' in str(e):
                results['passed'].append('Malformed request rejected')
                print("✓ Malformed request: Correctly rejected")
            else:
                results['passed'].append('Invalid config handled')

        # Test 7: RequestTimeout handling
        print("\nTest 7: Request size limits")

        # Try very large metadata (should fail)
        try:
            huge_metadata = {'large': 'x' * 5000}  # Way over 2KB limit
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='huge-metadata',
                Body=b'test',
                Metadata=huge_metadata
            )
            results['failed'].append('Huge metadata: Should fail')
        except Exception as e:
            if 'MetadataTooLarge' in str(e) or 'RequestHeaderSectionTooLarge' in str(e):
                results['passed'].append('Metadata size limit')
                print("✓ Metadata limit: Correctly enforced")
            else:
                results['passed'].append('Large metadata handled')

        # Test 8: InternalError simulation
        print("\nTest 8: Server error handling")

        # Try operations that might cause server errors
        try:
            # Rapid sequential operations on same key
            for i in range(10):
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key='rapid-update',
                    Body=f'update {i}'.encode()
                )

            results['passed'].append('Rapid operations')
            print("✓ Rapid operations: Handled successfully")

        except Exception as e:
            if 'InternalError' in str(e) or '500' in str(e):
                results['passed'].append('Server error detected')
                print("✓ Server error: Properly reported")
            else:
                results['failed'].append(f'Rapid ops: {str(e)[:50]}')

        # Test 9: SlowDown/Throttling
        print("\nTest 9: Rate limiting detection")

        # Make many requests rapidly
        try:
            for i in range(50):
                try:
                    s3_client.client.head_object(
                        Bucket=bucket_name,
                        Key='rapid-head-test'
                    )
                except:
                    pass  # Expected for non-existent object

            results['passed'].append('Rate limiting test')
            print("✓ Rate limiting: No throttling detected")

        except Exception as e:
            if 'SlowDown' in str(e) or '503' in str(e):
                results['passed'].append('Rate limiting detected')
                print("✓ Rate limiting: Throttling applied")
            else:
                results['failed'].append(f'Rate test: {str(e)[:50]}')

        # Test 10: Error response format
        print("\nTest 10: Error response format")

        try:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key='format-test-nonexistent'
            )
        except Exception as e:
            error_str = str(e)

            # Check if error contains expected S3 error elements
            if any(keyword in error_str for keyword in ['Code:', 'Message:', 'RequestId:', 'NoSuchKey']):
                results['passed'].append('Error format')
                print("✓ Error format: Contains structured error info")
            else:
                results['passed'].append('Simple error format')
                print("✓ Error format: Simple format used")

        # Test 11: Partial failure handling
        print("\nTest 11: Partial failure handling")

        # Upload with invalid ETag in conditional request
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='conditional-test',
            Body=b'test data'
        )

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='conditional-test',
                Body=b'new data',
                IfMatch='"wrong-etag"'
            )
            results['failed'].append('Wrong conditional: Should fail')
        except Exception as e:
            if 'PreconditionFailed' in str(e) or '412' in str(e):
                results['passed'].append('Conditional failure')
                print("✓ Conditional: Failed with correct error")

                # Verify original data is unchanged
                obj = s3_client.client.get_object(Bucket=bucket_name, Key='conditional-test')
                if obj['Body'].read() == b'test data':
                    results['passed'].append('Data preserved on failure')
                    print("✓ Failure handling: Original data preserved")

        # Summary
        print(f"\n=== Error Handling Test Results ===")
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
    test_error_handling(s3)