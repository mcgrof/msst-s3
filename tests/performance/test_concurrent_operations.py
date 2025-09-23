#!/usr/bin/env python3
"""
Test: Concurrent Operations Stress Test
Tests multiple simultaneous operations for race conditions and performance
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import threading
import time
import random

def test_concurrent_operations(s3_client: S3Client):
    """Test concurrent operations and race conditions"""
    bucket_name = f's3-concurrent-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': [], 'errors': []}
        results_lock = threading.Lock()

        # Test 1: Concurrent uploads to different keys
        print("Test 1: Concurrent uploads (different keys)")

        def upload_worker(worker_id, count):
            """Worker function for concurrent uploads"""
            local_errors = []
            local_success = 0

            for i in range(count):
                try:
                    key = f'worker-{worker_id}-object-{i}'
                    data = f'Worker {worker_id} object {i} data'.encode()

                    s3_client.client.put_object(
                        Bucket=bucket_name,
                        Key=key,
                        Body=data
                    )
                    local_success += 1

                except Exception as e:
                    local_errors.append(str(e))

            with results_lock:
                if local_success == count:
                    results['passed'].append(f'Worker {worker_id} uploads')
                else:
                    results['failed'].append(f'Worker {worker_id}: {local_success}/{count}')

                results['errors'].extend(local_errors)

        # Start multiple upload workers
        upload_threads = []
        num_workers = 5
        objects_per_worker = 10

        start_time = time.time()

        for worker_id in range(num_workers):
            thread = threading.Thread(
                target=upload_worker,
                args=(worker_id, objects_per_worker)
            )
            upload_threads.append(thread)
            thread.start()

        # Wait for all uploads to complete
        for thread in upload_threads:
            thread.join()

        upload_time = time.time() - start_time
        print(f"✓ Concurrent uploads: {num_workers} workers completed in {upload_time:.2f}s")

        # Test 2: Concurrent reads of same object
        print("\nTest 2: Concurrent reads (same object)")

        # Create a test object first
        test_key = 'concurrent-read-test'
        test_data = b'This is test data for concurrent reads' * 100
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_data
        )

        read_results = {'success': 0, 'failed': 0, 'corrupted': 0}
        read_lock = threading.Lock()

        def read_worker(worker_id, count):
            """Worker function for concurrent reads"""
            for i in range(count):
                try:
                    response = s3_client.client.get_object(
                        Bucket=bucket_name,
                        Key=test_key
                    )
                    content = response['Body'].read()

                    with read_lock:
                        if content == test_data:
                            read_results['success'] += 1
                        else:
                            read_results['corrupted'] += 1

                except Exception as e:
                    with read_lock:
                        read_results['failed'] += 1

        # Start concurrent readers
        read_threads = []
        num_readers = 10
        reads_per_worker = 5

        for worker_id in range(num_readers):
            thread = threading.Thread(
                target=read_worker,
                args=(worker_id, reads_per_worker)
            )
            read_threads.append(thread)
            thread.start()

        for thread in read_threads:
            thread.join()

        total_reads = num_readers * reads_per_worker
        if read_results['success'] == total_reads:
            results['passed'].append('Concurrent reads')
            print(f"✓ Concurrent reads: {read_results['success']}/{total_reads} successful")
        else:
            results['failed'].append(f"Concurrent reads: {read_results}")

        # Test 3: Mixed operations (read/write/delete)
        print("\nTest 3: Mixed concurrent operations")

        mixed_results = {'creates': 0, 'reads': 0, 'updates': 0, 'deletes': 0, 'errors': 0}
        mixed_lock = threading.Lock()

        def mixed_worker(worker_id, operations):
            """Worker performing mixed operations"""
            for op_num in range(operations):
                try:
                    key = f'mixed-{worker_id}-{op_num}'
                    operation = random.choice(['create', 'read', 'update', 'delete'])

                    if operation == 'create':
                        s3_client.client.put_object(
                            Bucket=bucket_name,
                            Key=key,
                            Body=f'Mixed operation data {worker_id}-{op_num}'.encode()
                        )
                        with mixed_lock:
                            mixed_results['creates'] += 1

                    elif operation == 'read':
                        # Try to read existing object
                        try:
                            s3_client.client.get_object(Bucket=bucket_name, Key=test_key)
                            with mixed_lock:
                                mixed_results['reads'] += 1
                        except:
                            pass  # Object might not exist

                    elif operation == 'update':
                        # Update if exists, create if not
                        s3_client.client.put_object(
                            Bucket=bucket_name,
                            Key=key,
                            Body=f'Updated data {worker_id}-{op_num}'.encode()
                        )
                        with mixed_lock:
                            mixed_results['updates'] += 1

                    elif operation == 'delete':
                        # Try to delete
                        try:
                            s3_client.client.delete_object(Bucket=bucket_name, Key=key)
                            with mixed_lock:
                                mixed_results['deletes'] += 1
                        except:
                            pass  # Object might not exist

                except Exception as e:
                    with mixed_lock:
                        mixed_results['errors'] += 1

        # Start mixed operation workers
        mixed_threads = []
        num_mixed_workers = 5
        ops_per_worker = 20

        for worker_id in range(num_mixed_workers):
            thread = threading.Thread(
                target=mixed_worker,
                args=(worker_id, ops_per_worker)
            )
            mixed_threads.append(thread)
            thread.start()

        for thread in mixed_threads:
            thread.join()

        total_ops = sum(mixed_results.values())
        if mixed_results['errors'] < total_ops * 0.1:  # Less than 10% errors
            results['passed'].append('Mixed operations')
            print(f"✓ Mixed operations: {total_ops} ops, {mixed_results['errors']} errors")
        else:
            results['failed'].append(f"Mixed operations: Too many errors {mixed_results}")

        # Test 4: Concurrent multipart uploads
        print("\nTest 4: Concurrent multipart uploads")

        def multipart_worker(worker_id):
            """Worker performing multipart upload"""
            try:
                key = f'multipart-{worker_id}'

                # Initiate multipart upload
                upload_id = s3_client.client.create_multipart_upload(
                    Bucket=bucket_name,
                    Key=key
                )['UploadId']

                # Upload parts
                parts = []
                part_size = 5 * 1024 * 1024  # 5MB

                for part_num in range(1, 4):  # 3 parts
                    part_data = f'Worker {worker_id} part {part_num} '.encode() * (part_size // 30)
                    part_data = part_data[:part_size]  # Ensure exact size

                    response = s3_client.client.upload_part(
                        Bucket=bucket_name,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_num,
                        Body=io.BytesIO(part_data)
                    )
                    parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

                # Complete multipart upload
                s3_client.client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )

                with results_lock:
                    results['passed'].append(f'Multipart worker {worker_id}')

            except Exception as e:
                with results_lock:
                    results['failed'].append(f'Multipart worker {worker_id}: {str(e)[:50]}')

        # Start concurrent multipart uploads
        multipart_threads = []
        num_multipart_workers = 3

        for worker_id in range(num_multipart_workers):
            thread = threading.Thread(target=multipart_worker, args=(worker_id,))
            multipart_threads.append(thread)
            thread.start()

        for thread in multipart_threads:
            thread.join()

        print(f"✓ Concurrent multipart: {num_multipart_workers} uploads attempted")

        # Test 5: Rapid bucket operations
        print("\nTest 5: Rapid bucket operations")

        bucket_ops_results = {'success': 0, 'failed': 0}
        bucket_ops_lock = threading.Lock()

        def bucket_ops_worker(worker_id):
            """Worker performing bucket operations"""
            test_bucket = f'rapid-ops-{worker_id}-{random_string(8).lower()}'

            try:
                # Create bucket
                s3_client.create_bucket(test_bucket)

                # Put some objects
                for i in range(5):
                    s3_client.client.put_object(
                        Bucket=test_bucket,
                        Key=f'rapid-object-{i}',
                        Body=f'Rapid test data {i}'.encode()
                    )

                # List objects
                s3_client.client.list_objects_v2(Bucket=test_bucket)

                # Delete objects
                objects = s3_client.client.list_objects_v2(Bucket=test_bucket)
                if 'Contents' in objects:
                    for obj in objects['Contents']:
                        s3_client.client.delete_object(
                            Bucket=test_bucket,
                            Key=obj['Key']
                        )

                # Delete bucket
                s3_client.delete_bucket(test_bucket)

                with bucket_ops_lock:
                    bucket_ops_results['success'] += 1

            except Exception as e:
                with bucket_ops_lock:
                    bucket_ops_results['failed'] += 1

        # Start rapid bucket operation workers
        bucket_threads = []
        num_bucket_workers = 3

        for worker_id in range(num_bucket_workers):
            thread = threading.Thread(target=bucket_ops_worker, args=(worker_id,))
            bucket_threads.append(thread)
            thread.start()

        for thread in bucket_threads:
            thread.join()

        if bucket_ops_results['success'] >= bucket_ops_results['failed']:
            results['passed'].append('Rapid bucket operations')
            print(f"✓ Rapid bucket ops: {bucket_ops_results['success']}/{num_bucket_workers} successful")
        else:
            results['failed'].append(f"Rapid bucket ops: {bucket_ops_results}")

        # Summary
        print(f"\n=== Concurrent Operations Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['errors']:
            print(f"Errors encountered: {len(results['errors'])}")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"  - {error[:80]}...")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            print("\nCleaning up concurrent test objects...")
            objects = s3_client.client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                # Delete in batches to avoid overwhelming the server
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
    test_concurrent_operations(s3)