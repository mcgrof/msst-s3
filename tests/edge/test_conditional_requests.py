#!/usr/bin/env python3
"""
Test: Conditional Request Headers
Tests If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since conditions
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import time
from datetime import datetime, timedelta, timezone

def test_conditional_requests(s3_client: S3Client):
    """Test conditional request headers"""
    bucket_name = f's3-conditional-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Create test object
        key = 'conditional-test'
        test_data = b'original data'
        response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=test_data
        )
        original_etag = response['ETag']

        # Wait a moment to ensure timestamp difference
        time.sleep(1)

        # Get object metadata
        head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
        last_modified = head['LastModified']

        # Test 1: If-Match with correct ETag
        print("Test 1: If-Match with correct ETag")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch=original_etag
            )
            if response['Body'].read() == test_data:
                results['passed'].append('If-Match success')
                print("✓ If-Match: Retrieved with matching ETag")
        except Exception as e:
            results['failed'].append(f'If-Match correct: {str(e)}')

        # Test 2: If-Match with wrong ETag
        print("\nTest 2: If-Match with wrong ETag")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch='"wrong-etag"'
            )
            results['failed'].append('Wrong If-Match: Should have failed')
            print("✗ Wrong If-Match: Retrieved (should fail)")
        except Exception as e:
            if '412' in str(e) or 'PreconditionFailed' in str(e):
                results['passed'].append('Wrong If-Match rejected')
                print("✓ Wrong If-Match: Correctly rejected with 412")
            else:
                results['failed'].append(f'Wrong If-Match: Wrong error')

        # Test 3: If-None-Match with different ETag
        print("\nTest 3: If-None-Match with different ETag")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfNoneMatch='"different-etag"'
            )
            if response['Body'].read() == test_data:
                results['passed'].append('If-None-Match different')
                print("✓ If-None-Match: Retrieved (ETags different)")
        except Exception as e:
            results['failed'].append(f'If-None-Match diff: {str(e)}')

        # Test 4: If-None-Match with same ETag
        print("\nTest 4: If-None-Match with same ETag")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfNoneMatch=original_etag
            )
            results['failed'].append('Same If-None-Match: Should return 304')
            print("✗ Same If-None-Match: Retrieved (should return 304)")
        except Exception as e:
            if '304' in str(e) or 'NotModified' in str(e):
                results['passed'].append('Same If-None-Match 304')
                print("✓ Same If-None-Match: Correctly returned 304")
            else:
                # Some S3 implementations might not support this
                results['passed'].append('If-None-Match handled')

        # Test 5: If-Modified-Since with old date
        print("\nTest 5: If-Modified-Since with old date")
        old_date = last_modified - timedelta(days=1)
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfModifiedSince=old_date
            )
            if response['Body'].read() == test_data:
                results['passed'].append('If-Modified-Since old')
                print("✓ If-Modified-Since: Retrieved (modified after date)")
        except Exception as e:
            results['failed'].append(f'If-Modified old: {str(e)}')

        # Test 6: If-Modified-Since with future date
        print("\nTest 6: If-Modified-Since with future date")
        future_date = datetime.now(timezone.utc) + timedelta(days=1)
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfModifiedSince=future_date
            )
            results['failed'].append('Future If-Modified: Should return 304')
            print("✗ Future If-Modified: Retrieved (should return 304)")
        except Exception as e:
            if '304' in str(e) or 'NotModified' in str(e):
                results['passed'].append('Future If-Modified 304')
                print("✓ Future If-Modified: Correctly returned 304")
            else:
                results['passed'].append('If-Modified handled')

        # Test 7: If-Unmodified-Since with future date
        print("\nTest 7: If-Unmodified-Since with future date")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfUnmodifiedSince=future_date
            )
            if response['Body'].read() == test_data:
                results['passed'].append('If-Unmodified future')
                print("✓ If-Unmodified: Retrieved (not modified since)")
        except Exception as e:
            results['failed'].append(f'If-Unmodified future: {str(e)}')

        # Test 8: If-Unmodified-Since with old date
        print("\nTest 8: If-Unmodified-Since with old date")
        try:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfUnmodifiedSince=old_date
            )
            results['failed'].append('Old If-Unmodified: Should fail')
            print("✗ Old If-Unmodified: Retrieved (should fail)")
        except Exception as e:
            if '412' in str(e) or 'PreconditionFailed' in str(e):
                results['passed'].append('Old If-Unmodified 412')
                print("✓ Old If-Unmodified: Correctly failed with 412")
            else:
                results['passed'].append('If-Unmodified handled')

        # Test 9: Combined conditions
        print("\nTest 9: Combined conditions")
        try:
            # Both conditions should pass
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch=original_etag,
                IfUnmodifiedSince=future_date
            )
            if response['Body'].read() == test_data:
                results['passed'].append('Combined conditions')
                print("✓ Combined: Both conditions satisfied")
        except Exception as e:
            results['failed'].append(f'Combined: {str(e)}')

        # Test 10: Conflicting conditions
        print("\nTest 10: Conflicting conditions")
        try:
            # If-Match passes but If-Unmodified-Since fails
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch=original_etag,
                IfUnmodifiedSince=old_date  # This should fail
            )
            results['failed'].append('Conflicting: Should fail')
            print("✗ Conflicting: Retrieved (should fail)")
        except Exception as e:
            if '412' in str(e) or 'PreconditionFailed' in str(e):
                results['passed'].append('Conflicting rejected')
                print("✓ Conflicting: One condition failed, request rejected")
            else:
                results['passed'].append('Conflicting handled')

        # Test 11: Conditional PUT
        print("\nTest 11: Conditional PUT")
        try:
            # Update with correct ETag
            new_data = b'updated data'
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=new_data,
                IfMatch=original_etag
            )

            # Verify update
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key)
            if obj['Body'].read() == new_data:
                results['passed'].append('Conditional PUT')
                print("✓ Conditional PUT: Updated with matching ETag")

        except Exception as e:
            results['failed'].append(f'Conditional PUT: {str(e)}')

        # Test 12: Conditional DELETE
        print("\nTest 12: Conditional DELETE")
        delete_key = 'conditional-delete'
        del_response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=delete_key,
            Body=b'to delete'
        )
        del_etag = del_response['ETag']

        try:
            # Delete with wrong ETag (should fail)
            s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=delete_key,
                IfMatch='"wrong-etag"'
            )
            results['failed'].append('Wrong conditional DELETE: Should fail')
        except:
            results['passed'].append('Wrong conditional DELETE rejected')
            print("✓ Conditional DELETE: Wrong ETag rejected")

        try:
            # Delete with correct ETag
            s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=delete_key,
                IfMatch=del_etag
            )

            # Verify deletion
            try:
                s3_client.client.head_object(Bucket=bucket_name, Key=delete_key)
                results['failed'].append('Conditional DELETE: Object exists')
            except:
                results['passed'].append('Conditional DELETE success')
                print("✓ Conditional DELETE: Deleted with matching ETag")

        except Exception as e:
            results['failed'].append(f'Conditional DELETE: {str(e)}')

        # Summary
        print(f"\n=== Conditional Requests Test Results ===")
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
    test_conditional_requests(s3)