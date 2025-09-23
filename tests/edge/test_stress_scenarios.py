#!/usr/bin/env python3
"""
Test: Stress Test Scenarios
Tests S3 under various stress conditions including rapid operations and resource limits
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

def test_stress_scenarios(s3_client: S3Client):
    """Test various stress scenarios"""
    bucket_name = f's3-stress-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': [], 'errors': []}

        # Test 1: Rapid sequential operations
        print("Test 1: Rapid sequential operations")
        try:
            start_time = time.time()
            operations_count = 100

            for i in range(operations_count):
                key = f'rapid-{i:03d}'

                # Upload
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'Rapid test data {i}'.encode()
                )

                # Immediate read
                obj = s3_client.client.get_object(Bucket=bucket_name, Key=key)
                content = obj['Body'].read()

                # Verify data
                if content != f'Rapid test data {i}'.encode():
                    results['failed'].append(f'Rapid operation {i}: Data mismatch')
                    break

            end_time = time.time()
            duration = end_time - start_time

            if i == operations_count - 1:  # All operations completed
                results['passed'].append(f'Rapid operations ({operations_count})')
                print(f"✓ Rapid operations: {operations_count} ops in {duration:.2f}s ({operations_count/duration:.1f} ops/s)")

        except Exception as e:
            results['failed'].append(f'Rapid operations: {str(e)}')

        # Test 2: Many small objects
        print("\nTest 2: Many small objects")
        try:
            num_objects = 500
            start_time = time.time()

            # Create many small objects
            for i in range(num_objects):
                key = f'small-{i:04d}'
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'Small object {i}'.encode()
                )

            # List all objects
            response = s3_client.client.list_objects_v2(Bucket=bucket_name)
            listed_count = response.get('KeyCount', 0)

            end_time = time.time()
            duration = end_time - start_time

            if listed_count >= num_objects:
                results['passed'].append(f'Many small objects ({num_objects})')
                print(f"✓ Many objects: {num_objects} created in {duration:.2f}s")
            else:
                results['failed'].append(f'Many objects: Only {listed_count}/{num_objects} listed')

        except Exception as e:
            results['failed'].append(f'Many objects: {str(e)}')

        # Test 3: Rapid key name variations
        print("\nTest 3: Rapid key name variations")
        try:
            variations = [
                'test/file.txt',
                'test-file.txt',
                'test_file.txt',
                'test.file.txt',
                'TEST/FILE.TXT',
                'test/sub/file.txt',
                'test//double//slash.txt',
                'test/.hidden/file.txt',
                'test/123/file.txt',
                '123/test/file.txt',
            ]

            for i, key_pattern in enumerate(variations * 10):  # Repeat each pattern 10 times
                key = f'{key_pattern}-{i}'
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'Variation test {i}'.encode()
                )

            results['passed'].append('Key name variations')
            print(f"✓ Key variations: {len(variations) * 10} variations handled")

        except Exception as e:
            results['failed'].append(f'Key variations: {str(e)}')

        # Test 4: Rapid metadata operations
        print("\nTest 4: Rapid metadata operations")
        try:
            base_key = 'metadata-stress'

            # Rapid metadata updates via copy
            for i in range(50):
                metadata = {
                    'iteration': str(i),
                    'timestamp': str(time.time()),
                    'random': str(random.randint(1000, 9999))
                }

                if i == 0:
                    # Initial upload
                    s3_client.client.put_object(
                        Bucket=bucket_name,
                        Key=base_key,
                        Body=b'metadata stress test',
                        Metadata=metadata
                    )
                else:
                    # Update metadata via copy
                    s3_client.client.copy_object(
                        Bucket=bucket_name,
                        Key=base_key,
                        CopySource={'Bucket': bucket_name, 'Key': base_key},
                        Metadata=metadata,
                        MetadataDirective='REPLACE'
                    )

            # Verify final metadata
            head = s3_client.client.head_object(Bucket=bucket_name, Key=base_key)
            final_metadata = head.get('Metadata', {})

            if 'iteration' in final_metadata and final_metadata['iteration'] == '49':
                results['passed'].append('Rapid metadata updates')
                print("✓ Metadata stress: 50 updates completed")
            else:
                results['failed'].append('Metadata stress: Final state incorrect')

        except Exception as e:
            results['failed'].append(f'Metadata stress: {str(e)}')

        # Test 5: Concurrent bucket listing
        print("\nTest 5: Concurrent bucket listing")
        list_results = {'success': 0, 'failed': 0}
        list_lock = threading.Lock()

        def list_worker(worker_id, iterations):
            """Worker that repeatedly lists bucket contents"""
            for i in range(iterations):
                try:
                    response = s3_client.client.list_objects_v2(
                        Bucket=bucket_name,
                        MaxKeys=100
                    )

                    with list_lock:
                        list_results['success'] += 1

                except Exception as e:
                    with list_lock:
                        list_results['failed'] += 1

        # Start concurrent listers
        list_threads = []
        num_listers = 10
        lists_per_worker = 20

        for worker_id in range(num_listers):
            thread = threading.Thread(target=list_worker, args=(worker_id, lists_per_worker))
            list_threads.append(thread)
            thread.start()

        for thread in list_threads:
            thread.join()

        total_lists = num_listers * lists_per_worker
        success_rate = list_results['success'] / total_lists

        if success_rate >= 0.95:  # 95% success rate
            results['passed'].append('Concurrent listing stress')
            print(f"✓ Concurrent listing: {list_results['success']}/{total_lists} successful ({success_rate:.1%})")
        else:
            results['failed'].append(f"Concurrent listing: Low success rate {success_rate:.1%}")

        # Test 6: Rapid object size variations
        print("\nTest 6: Rapid object size variations")
        try:
            sizes = [0, 1, 10, 100, 1000, 10000, 100000, 500000]  # Various sizes

            for i, size in enumerate(sizes * 5):  # Repeat each size 5 times
                key = f'size-stress-{i}-{size}'
                data = b'X' * size

                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data
                )

                # Verify size immediately
                head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
                if head['ContentLength'] != size:
                    results['failed'].append(f'Size stress {size}: Length mismatch')
                    break

            results['passed'].append('Object size variations')
            print(f"✓ Size variations: {len(sizes) * 5} objects with varying sizes")

        except Exception as e:
            results['failed'].append(f'Size variations: {str(e)}')

        # Test 7: Error recovery stress
        print("\nTest 7: Error recovery stress")
        try:
            error_recovery_count = 0

            for i in range(100):
                try:
                    # Mix valid and invalid operations
                    if i % 10 == 0:
                        # Invalid operation (non-existent key)
                        try:
                            s3_client.client.get_object(
                                Bucket=bucket_name,
                                Key=f'non-existent-{i}'
                            )
                        except:
                            error_recovery_count += 1
                    else:
                        # Valid operation
                        key = f'recovery-test-{i}'
                        s3_client.client.put_object(
                            Bucket=bucket_name,
                            Key=key,
                            Body=f'Recovery test {i}'.encode()
                        )

                except Exception as e:
                    # Should recover from errors
                    error_recovery_count += 1

            if error_recovery_count >= 10:  # Should have handled the errors
                results['passed'].append('Error recovery stress')
                print(f"✓ Error recovery: Handled {error_recovery_count} error scenarios")
            else:
                results['failed'].append('Error recovery: Insufficient error handling')

        except Exception as e:
            results['failed'].append(f'Error recovery: {str(e)}')

        # Test 8: Memory pressure simulation
        print("\nTest 8: Memory pressure simulation")
        try:
            # Upload objects that could stress memory if not handled properly
            large_keys = []

            for i in range(20):
                key = f'memory-test-{i}'
                # Create 1MB objects
                data = bytes([i % 256]) * (1024 * 1024)

                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data
                )
                large_keys.append(key)

            # Rapidly download multiple large objects
            download_count = 0
            for key in large_keys[:10]:  # Download 10 objects
                obj = s3_client.client.get_object(Bucket=bucket_name, Key=key)
                content = obj['Body'].read()
                if len(content) == 1024 * 1024:
                    download_count += 1

            if download_count == 10:
                results['passed'].append('Memory pressure handling')
                print(f"✓ Memory pressure: Handled {download_count} large downloads")
            else:
                results['failed'].append(f'Memory pressure: Only {download_count}/10 downloads')

        except Exception as e:
            results['failed'].append(f'Memory pressure: {str(e)}')

        # Test 9: Rapid delete operations
        print("\nTest 9: Rapid delete operations")
        try:
            # Create objects to delete
            delete_keys = []
            for i in range(100):
                key = f'delete-me-{i:03d}'
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'Delete test {i}'.encode()
                )
                delete_keys.append(key)

            # Rapidly delete them
            start_time = time.time()
            deleted_count = 0

            for key in delete_keys:
                try:
                    s3_client.client.delete_object(Bucket=bucket_name, Key=key)
                    deleted_count += 1
                except:
                    pass

            end_time = time.time()
            duration = end_time - start_time

            if deleted_count == len(delete_keys):
                results['passed'].append('Rapid delete operations')
                print(f"✓ Rapid deletes: {deleted_count} objects in {duration:.2f}s")
            else:
                results['failed'].append(f'Rapid deletes: Only {deleted_count}/{len(delete_keys)}')

        except Exception as e:
            results['failed'].append(f'Rapid deletes: {str(e)}')

        # Test 10: Connection stress
        print("\nTest 10: Connection stress")
        try:
            # Perform many operations that could stress connections
            connection_ops = 0

            for i in range(200):
                try:
                    # Mix of operations
                    op_type = i % 4

                    if op_type == 0:
                        # HEAD operation
                        s3_client.client.head_object(Bucket=bucket_name, Key='rapid-001')
                    elif op_type == 1:
                        # LIST operation
                        s3_client.client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                    elif op_type == 2:
                        # PUT operation
                        s3_client.client.put_object(
                            Bucket=bucket_name,
                            Key=f'conn-stress-{i}',
                            Body=f'Connection stress {i}'.encode()
                        )
                    else:
                        # GET operation
                        try:
                            s3_client.client.get_object(Bucket=bucket_name, Key='rapid-001')
                        except:
                            pass  # May not exist

                    connection_ops += 1

                except Exception as e:
                    # Some failures are expected under stress
                    pass

            if connection_ops >= 150:  # At least 75% success
                results['passed'].append('Connection stress')
                print(f"✓ Connection stress: {connection_ops}/200 operations successful")
            else:
                results['failed'].append(f'Connection stress: Only {connection_ops}/200')

        except Exception as e:
            results['failed'].append(f'Connection stress: {str(e)}')

        # Summary
        print(f"\n=== Stress Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['errors']:
            print(f"Errors: {len(results['errors'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            print("\nCleaning up stress test objects...")

            # Delete in batches to avoid overwhelming
            continuation_token = None
            batch_size = 1000

            while True:
                params = {'Bucket': bucket_name, 'MaxKeys': batch_size}
                if continuation_token:
                    params['ContinuationToken'] = continuation_token

                try:
                    response = s3_client.client.list_objects_v2(**params)

                    if 'Contents' in response:
                        for obj in response['Contents']:
                            try:
                                s3_client.client.delete_object(
                                    Bucket=bucket_name,
                                    Key=obj['Key']
                                )
                            except:
                                pass  # Continue cleanup even if some deletions fail

                    if not response.get('IsTruncated'):
                        break

                    continuation_token = response.get('NextContinuationToken')

                except:
                    break  # Exit cleanup loop on error

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
    test_stress_scenarios(s3)