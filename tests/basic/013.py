#!/usr/bin/env python3
"""
Test 013: Partial failure recovery

Tests system behavior and recovery mechanisms when operations partially fail.
Validates that partial failures are handled gracefully and can be recovered.
"""

import io
import time
import threading
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_013(s3_client, config):
    """Partial failure recovery test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    multipart_upload_ids = []

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-013')
        s3_client.create_bucket(bucket_name)

        # Test 1: Partial multipart upload failure and recovery
        object_key = 'partial-multipart.bin'
        part_size = 5 * 1024 * 1024  # 5MB
        total_parts = 5

        # Generate test data
        parts_data = []
        for i in range(total_parts):
            parts_data.append(b'P' * part_size + str(i).encode())

        # Start multipart upload
        upload_id = s3_client.create_multipart_upload(bucket_name, object_key)
        multipart_upload_ids.append(upload_id)

        # Upload some parts successfully
        uploaded_parts = []
        for part_num in [1, 2, 4]:  # Skip part 3 intentionally
            response = s3_client.upload_part(
                bucket_name,
                object_key,
                upload_id,
                part_num,
                io.BytesIO(parts_data[part_num - 1])
            )
            uploaded_parts.append({
                'PartNumber': part_num,
                'ETag': response['ETag']
            })

        # Verify we can list the parts already uploaded
        response = s3_client.client.list_parts(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id
        )
        existing_parts = response.get('Parts', [])
        assert len(existing_parts) == 3, f"Expected 3 parts, found {len(existing_parts)}"

        # Try to complete with missing parts
        # Note: Some S3 implementations (like MinIO) may allow completion with gaps
        try:
            response = s3_client.complete_multipart_upload(
                bucket_name,
                object_key,
                upload_id,
                uploaded_parts
            )
            # MinIO allows completion with missing parts
            # Clean up the object for next test
            s3_client.delete_object(bucket_name, object_key)
            multipart_upload_ids.remove(upload_id)

            # Start a new upload for the recovery test
            upload_id = s3_client.create_multipart_upload(bucket_name, object_key)
            multipart_upload_ids.append(upload_id)
            uploaded_parts = []

            # Upload all parts for the new upload
            for part_num in range(1, total_parts + 1):
                response = s3_client.upload_part(
                    bucket_name,
                    object_key,
                    upload_id,
                    part_num,
                    io.BytesIO(parts_data[part_num - 1])
                )
                uploaded_parts.append({
                    'PartNumber': part_num,
                    'ETag': response['ETag']
                })
        except ClientError as e:
            error_code = e.response['Error']['Code']
            # AWS S3 enforces stricter validation
            assert error_code in ['InvalidPart', 'InvalidPartOrder', 'InvalidRequest'], \
                f"Unexpected error code: {error_code}"

            # Recover by uploading missing parts
            for part_num in [3, 5]:
                response = s3_client.upload_part(
                    bucket_name,
                    object_key,
                    upload_id,
                    part_num,
                    io.BytesIO(parts_data[part_num - 1])
                )
                uploaded_parts.append({
                    'PartNumber': part_num,
                    'ETag': response['ETag']
                })

        # Sort parts by part number
        uploaded_parts.sort(key=lambda x: x['PartNumber'])

        # Now complete should succeed
        s3_client.complete_multipart_upload(
            bucket_name,
            object_key,
            upload_id,
            uploaded_parts
        )
        multipart_upload_ids.remove(upload_id)  # Successfully completed

        # Verify the object exists and has correct size
        response = s3_client.head_object(bucket_name, object_key)
        expected_size = sum(len(part) for part in parts_data)
        actual_size = response['ContentLength']
        assert actual_size == expected_size, \
            f"Size mismatch after recovery: expected {expected_size}, got {actual_size}"

        # Test 2: Batch operation with partial failures
        batch_objects = []
        success_count = 0
        failure_count = 0

        # Create mix of valid and invalid operations
        operations = [
            {'key': 'batch/valid1.txt', 'data': b'Valid data 1', 'valid': True},
            {'key': 'batch/valid2.txt', 'data': b'Valid data 2', 'valid': True},
            {'key': 'batch/invalid/\x00null.txt', 'data': b'Invalid key', 'valid': False},  # Invalid character
            {'key': 'batch/valid3.txt', 'data': b'Valid data 3', 'valid': True},
            {'key': 'batch/../escaping.txt', 'data': b'Path traversal', 'valid': False},
            {'key': 'batch/valid4.txt', 'data': b'Valid data 4', 'valid': True},
        ]

        # Execute batch operations
        for op in operations:
            try:
                # Try to upload each object
                s3_client.put_object(
                    bucket_name,
                    op['key'],
                    io.BytesIO(op['data'])
                )
                success_count += 1
                if op['valid']:
                    batch_objects.append(op['key'])
                else:
                    # Some S3 implementations might accept these keys
                    batch_objects.append(op['key'])
            except (ClientError, ValueError, Exception) as e:
                failure_count += 1
                if op['valid']:
                    raise Exception(f"Valid operation failed: {op['key']}")

        # Verify at least some operations succeeded
        assert success_count >= 4, f"Too few successful operations: {success_count}"

        # List objects to verify successful uploads
        objects = s3_client.list_objects(bucket_name, prefix='batch/')
        object_keys = [obj['Key'] for obj in objects]

        # Verify expected valid objects exist
        for op in operations:
            if op['valid'] and op['key'] in batch_objects:
                assert op['key'] in object_keys, f"Missing valid object: {op['key']}"

        # Test 3: Concurrent operations with partial failures
        concurrent_results = {'success': 0, 'failure': 0, 'errors': []}
        lock = threading.Lock()

        def concurrent_upload(index):
            """Upload with simulated random failures"""
            key = f'concurrent/object-{index}.txt'
            data = f'Concurrent data {index}'.encode()

            try:
                # Simulate failure for some operations
                if index % 7 == 0:  # Simulate ~14% failure rate
                    # Try to create object with invalid metadata
                    s3_client.put_object(
                        bucket_name,
                        key,
                        io.BytesIO(data),
                        Metadata={'invalid\x00key': 'value'}  # Invalid metadata key
                    )
                else:
                    # Normal upload
                    s3_client.put_object(
                        bucket_name,
                        key,
                        io.BytesIO(data)
                    )

                with lock:
                    concurrent_results['success'] += 1

            except Exception as e:
                with lock:
                    concurrent_results['failure'] += 1
                    concurrent_results['errors'].append(str(e))

        # Run concurrent uploads
        threads = []
        num_operations = 20

        for i in range(num_operations):
            thread = threading.Thread(target=concurrent_upload, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join(timeout=10)

        # Verify partial success
        assert concurrent_results['success'] > 0, "No operations succeeded"
        assert concurrent_results['success'] >= num_operations * 0.75, \
            f"Too many failures: {concurrent_results['failure']}/{num_operations}"

        # Test 4: Recovery from incomplete multipart uploads
        # Create multiple incomplete uploads
        incomplete_uploads = []
        for i in range(3):
            key = f'incomplete-{i}.bin'
            upload_id = s3_client.create_multipart_upload(bucket_name, key)
            multipart_upload_ids.append(upload_id)
            incomplete_uploads.append({'Key': key, 'UploadId': upload_id})

            # Upload only one part (incomplete)
            s3_client.upload_part(
                bucket_name,
                key,
                upload_id,
                1,
                io.BytesIO(b'Incomplete data')
            )

        # List incomplete uploads
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        uploads = response.get('Uploads', [])

        # Should have at least our incomplete uploads
        assert len(uploads) >= len(incomplete_uploads), \
            f"Expected at least {len(incomplete_uploads)} incomplete uploads"

        # Clean up incomplete uploads (recovery action)
        for upload in incomplete_uploads:
            s3_client.abort_multipart_upload(
                bucket_name,
                upload['Key'],
                upload['UploadId']
            )
            multipart_upload_ids.remove(upload['UploadId'])

        # Verify cleanup
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        remaining_uploads = response.get('Uploads', [])

        # Our uploads should be gone
        for upload in incomplete_uploads:
            for remaining in remaining_uploads:
                assert remaining['UploadId'] != upload['UploadId'], \
                    f"Upload {upload['UploadId']} not cleaned up"

        print(f"Partial failure recovery test completed:")
        print(f"- Multipart recovery: ✓")
        print(f"- Batch operations: {success_count} succeeded")
        print(f"- Concurrent operations: {concurrent_results['success']}/{num_operations} succeeded")
        print(f"- Incomplete upload cleanup: {len(incomplete_uploads)} cleaned")

    finally:
        # Cleanup any remaining multipart uploads
        for upload_id in multipart_upload_ids:
            try:
                s3_client.abort_multipart_upload(bucket_name, object_key, upload_id)
            except:
                pass

        # Cleanup bucket
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass