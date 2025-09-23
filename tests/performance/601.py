#!/usr/bin/env python3
"""
Test 601: Concurrent operations performance

Benchmarks concurrent read/write operations.
Measures system behavior under parallel load.
"""

import time
import os
import io
import threading
import statistics
from common.fixtures import TestFixture

def concurrent_operation(s3_client, bucket, operation, key, data, results, index):
    """Execute a single operation and track timing"""
    start_time = time.time()
    try:
        if operation == 'write':
            s3_client.put_object(bucket, key, io.BytesIO(data))
        elif operation == 'read':
            response = s3_client.get_object(bucket, key)
            _ = response['Body'].read()
        elif operation == 'delete':
            s3_client.delete_object(bucket, key)

        duration = time.time() - start_time
        results[index] = {'success': True, 'duration': duration}
    except Exception as e:
        duration = time.time() - start_time
        results[index] = {'success': False, 'duration': duration, 'error': str(e)}

def test_601(s3_client, config):
    """Concurrent operations performance test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    results = {
        'concurrent_levels': {},
        'summary': {}
    }

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-601')
        s3_client.create_bucket(bucket_name)

        # Test different concurrency levels
        concurrency_levels = [1, 5, 10, 20, 50]
        object_size = 100 * 1024  # 100KB objects
        operations_per_level = 50

        print("\n=== Concurrent Operations Performance ===")

        for concurrent_count in concurrency_levels:
            print(f"\nTesting with {concurrent_count} concurrent operations...")

            # Prepare test data
            test_data = os.urandom(object_size)

            # First, create objects for read tests
            setup_keys = []
            for i in range(operations_per_level):
                key = f'concurrent-{concurrent_count}-{i}.dat'
                s3_client.put_object(bucket_name, key, io.BytesIO(test_data))
                setup_keys.append(key)

            # Test concurrent writes
            write_results = {}
            write_threads = []
            write_start = time.time()

            for i in range(operations_per_level):
                key = f'concurrent-write-{concurrent_count}-{i}.dat'
                thread = threading.Thread(
                    target=concurrent_operation,
                    args=(s3_client, bucket_name, 'write', key,
                          test_data, write_results, i)
                )
                write_threads.append(thread)

                # Start thread first
                thread.start()

                # Control concurrency by waiting if we have too many active threads
                while len([t for t in write_threads if t.is_alive()]) >= concurrent_count:
                    # Wait a bit for threads to complete
                    time.sleep(0.01)

            # Wait for all writes to complete
            for thread in write_threads:
                thread.join(timeout=30)

            write_duration = time.time() - write_start

            # Test concurrent reads
            read_results = {}
            read_threads = []
            read_start = time.time()

            for i in range(min(operations_per_level, len(setup_keys))):
                key = setup_keys[i]
                thread = threading.Thread(
                    target=concurrent_operation,
                    args=(s3_client, bucket_name, 'read', key,
                          None, read_results, i)
                )
                read_threads.append(thread)

                # Start thread first
                thread.start()

                # Control concurrency by waiting if we have too many active threads
                while len([t for t in read_threads if t.is_alive()]) >= concurrent_count:
                    # Wait a bit for threads to complete
                    time.sleep(0.01)

            # Wait for all reads to complete
            for thread in read_threads:
                thread.join(timeout=30)

            read_duration = time.time() - read_start

            # Calculate statistics
            write_times = [r['duration'] for r in write_results.values() if r['success']]
            read_times = [r['duration'] for r in read_results.values() if r['success']]
            write_failures = len([r for r in write_results.values() if not r['success']])
            read_failures = len([r for r in read_results.values() if not r['success']])

            results['concurrent_levels'][concurrent_count] = {
                'write': {
                    'total_ops': len(write_results),
                    'successful_ops': len(write_times),
                    'failed_ops': write_failures,
                    'total_duration': write_duration,
                    'ops_per_second': len(write_times) / write_duration if write_duration > 0 else 0,
                    'avg_latency': statistics.mean(write_times) if write_times else 0,
                    'p50_latency': statistics.median(write_times) if write_times else 0,
                    'p95_latency': sorted(write_times)[int(len(write_times) * 0.95)] if write_times else 0,
                    'throughput_mbps': (len(write_times) * object_size) / (write_duration * 1024 * 1024) if write_duration > 0 else 0
                },
                'read': {
                    'total_ops': len(read_results),
                    'successful_ops': len(read_times),
                    'failed_ops': read_failures,
                    'total_duration': read_duration,
                    'ops_per_second': len(read_times) / read_duration if read_duration > 0 else 0,
                    'avg_latency': statistics.mean(read_times) if read_times else 0,
                    'p50_latency': statistics.median(read_times) if read_times else 0,
                    'p95_latency': sorted(read_times)[int(len(read_times) * 0.95)] if read_times else 0,
                    'throughput_mbps': (len(read_times) * object_size) / (read_duration * 1024 * 1024) if read_duration > 0 else 0
                }
            }

            # Print results for this concurrency level
            level_results = results['concurrent_levels'][concurrent_count]
            print(f"  Write: {level_results['write']['ops_per_second']:.1f} ops/s, "
                  f"p50={level_results['write']['p50_latency']:.3f}s, "
                  f"p95={level_results['write']['p95_latency']:.3f}s, "
                  f"failures={level_results['write']['failed_ops']}")
            print(f"  Read:  {level_results['read']['ops_per_second']:.1f} ops/s, "
                  f"p50={level_results['read']['p50_latency']:.3f}s, "
                  f"p95={level_results['read']['p95_latency']:.3f}s, "
                  f"failures={level_results['read']['failed_ops']}")

        # Find optimal concurrency level
        best_write_ops = max(results['concurrent_levels'].items(),
                            key=lambda x: x[1]['write']['ops_per_second'])
        best_read_ops = max(results['concurrent_levels'].items(),
                           key=lambda x: x[1]['read']['ops_per_second'])

        results['summary'] = {
            'best_write_concurrency': best_write_ops[0],
            'best_write_ops_per_second': best_write_ops[1]['write']['ops_per_second'],
            'best_read_concurrency': best_read_ops[0],
            'best_read_ops_per_second': best_read_ops[1]['read']['ops_per_second']
        }

        print("\n=== Concurrency Summary ===")
        print(f"Best write concurrency: {results['summary']['best_write_concurrency']} "
              f"({results['summary']['best_write_ops_per_second']:.1f} ops/s)")
        print(f"Best read concurrency: {results['summary']['best_read_concurrency']} "
              f"({results['summary']['best_read_ops_per_second']:.1f} ops/s)")

        # Performance assertions
        # System should handle at least 10 concurrent operations
        assert results['concurrent_levels'][10]['write']['failed_ops'] < 5, \
            "Too many failures with 10 concurrent writes"
        assert results['concurrent_levels'][10]['read']['failed_ops'] < 5, \
            "Too many failures with 10 concurrent reads"

        # Should achieve reasonable ops/s with concurrency
        assert results['summary']['best_write_ops_per_second'] > 5, \
            "Should achieve >5 write ops/s with concurrency"
        assert results['summary']['best_read_ops_per_second'] > 10, \
            "Should achieve >10 read ops/s with concurrency"

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Delete all test objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    try:
                        s3_client.delete_object(bucket_name, obj['Key'])
                    except:
                        pass
                s3_client.delete_bucket(bucket_name)
            except:
                pass

    return results