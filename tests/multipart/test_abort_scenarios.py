#!/usr/bin/env python3
"""
Test: Multipart Upload Abort Scenarios
Tests various abort scenarios, cleanup behavior, and resource management
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import time
import threading

def test_abort_scenarios(s3_client: S3Client):
    """Test multipart upload abort scenarios"""
    bucket_name = f's3-abort-scenarios-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}
        part_size = 5 * 1024 * 1024  # 5MB minimum

        # Test 1: Basic abort functionality
        print("Test 1: Basic abort functionality")
        key1 = 'basic-abort'
        upload_id1 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key1
        )['UploadId']

        # Upload some parts
        for i in range(1, 4):
            s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key1,
                UploadId=upload_id1,
                PartNumber=i,
                Body=io.BytesIO(b'X' * part_size)
            )

        # Abort the upload
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key1,
            UploadId=upload_id1
        )

        # Verify upload is gone
        uploads = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        active_uploads = [u for u in uploads.get('Uploads', []) if u['UploadId'] == upload_id1]

        if len(active_uploads) == 0:
            results['passed'].append('Basic abort cleanup')
            print("✓ Basic abort: Upload cleaned up")
        else:
            results['failed'].append('Basic abort: Upload still listed')
            print("✗ Basic abort: Upload still listed")

        # Test 2: Double abort (abort already aborted upload)
        print("\nTest 2: Double abort")
        try:
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key1,
                UploadId=upload_id1
            )
            results['failed'].append('Double abort: Should have failed')
            print("✗ Double abort: Second abort succeeded (should fail)")
        except Exception as e:
            if 'NoSuchUpload' in str(e) or '404' in str(e):
                results['passed'].append('Double abort rejected')
                print("✓ Double abort: Correctly rejected")
            else:
                results['failed'].append(f'Double abort: Unexpected error: {e}')

        # Test 3: Abort with invalid upload ID
        print("\nTest 3: Abort with invalid upload ID")
        try:
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name,
                Key='nonexistent',
                UploadId='invalid-upload-id-12345'
            )
            results['failed'].append('Invalid upload ID: Should have failed')
            print("✗ Invalid upload ID: Abort succeeded (should fail)")
        except Exception as e:
            if 'NoSuchUpload' in str(e) or '404' in str(e):
                results['passed'].append('Invalid upload ID rejected')
                print("✓ Invalid upload ID: Correctly rejected")
            else:
                results['failed'].append(f'Invalid upload ID: Unexpected error: {e}')

        # Test 4: Concurrent abort attempts
        print("\nTest 4: Concurrent abort attempts")
        key4 = 'concurrent-abort'
        upload_id4 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key4
        )['UploadId']

        # Upload a part
        s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key4,
            UploadId=upload_id4,
            PartNumber=1,
            Body=io.BytesIO(b'Y' * part_size)
        )

        abort_results = {'success': 0, 'failed': 0}
        lock = threading.Lock()

        def abort_upload():
            try:
                s3_client.client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key4,
                    UploadId=upload_id4
                )
                with lock:
                    abort_results['success'] += 1
            except:
                with lock:
                    abort_results['failed'] += 1

        # Try to abort from multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=abort_upload)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Only one should succeed
        if abort_results['success'] == 1 and abort_results['failed'] == 4:
            results['passed'].append('Concurrent abort handled correctly')
            print(f"✓ Concurrent abort: 1 success, 4 failures (correct)")
        else:
            results['failed'].append(f"Concurrent abort: {abort_results['success']} success, {abort_results['failed']} failures")
            print(f"✗ Concurrent abort: {abort_results['success']} success, {abort_results['failed']} failures")

        # Test 5: Abort after partial complete attempt
        print("\nTest 5: Abort after failed complete")
        key5 = 'abort-after-failed-complete'
        upload_id5 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key5
        )['UploadId']

        # Upload only part 1 and 3 (missing part 2)
        resp1 = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key5,
            UploadId=upload_id5,
            PartNumber=1,
            Body=io.BytesIO(b'1' * part_size)
        )
        resp3 = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key5,
            UploadId=upload_id5,
            PartNumber=3,
            Body=io.BytesIO(b'3' * part_size)
        )

        # Try to complete with missing part 2
        try:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key5,
                UploadId=upload_id5,
                MultipartUpload={'Parts': [
                    {'PartNumber': 1, 'ETag': resp1['ETag']},
                    {'PartNumber': 3, 'ETag': resp3['ETag']}
                ]}
            )
        except:
            pass  # Expected to fail

        # Now abort the upload
        try:
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key5,
                UploadId=upload_id5
            )
            results['passed'].append('Abort after failed complete')
            print("✓ Abort after failed complete: Success")
        except Exception as e:
            results['failed'].append(f'Abort after failed complete: {str(e)}')
            print(f"✗ Abort after failed complete: {str(e)}")

        # Test 6: List parts after abort
        print("\nTest 6: List parts after abort")
        key6 = 'list-after-abort'
        upload_id6 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key6
        )['UploadId']

        # Upload a part
        s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key6,
            UploadId=upload_id6,
            PartNumber=1,
            Body=io.BytesIO(b'Z' * part_size)
        )

        # Abort
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key6,
            UploadId=upload_id6
        )

        # Try to list parts of aborted upload
        try:
            parts = s3_client.client.list_parts(
                Bucket=bucket_name,
                Key=key6,
                UploadId=upload_id6
            )
            results['failed'].append('List parts after abort: Should have failed')
            print("✗ List parts after abort: Succeeded (should fail)")
        except Exception as e:
            if 'NoSuchUpload' in str(e) or '404' in str(e):
                results['passed'].append('List parts after abort rejected')
                print("✓ List parts after abort: Correctly rejected")
            else:
                results['failed'].append(f'List parts after abort: Unexpected error: {e}')

        # Summary
        print(f"\n=== Abort Scenarios Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            # Abort any remaining uploads
            uploads = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
            if 'Uploads' in uploads:
                for upload in uploads['Uploads']:
                    try:
                        s3_client.client.abort_multipart_upload(
                            Bucket=bucket_name,
                            Key=upload['Key'],
                            UploadId=upload['UploadId']
                        )
                    except:
                        pass

            # Delete any objects
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
    test_abort_scenarios(s3)