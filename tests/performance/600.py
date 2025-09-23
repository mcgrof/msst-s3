#!/usr/bin/env python3
"""
Test 600: Sequential read/write performance

Benchmarks sequential read and write operations.
Measures throughput and latency for various object sizes.
"""

import time
import os
import io
import statistics
from common.fixtures import TestFixture

def test_600(s3_client, config):
    """Sequential read/write performance test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    results = {
        'write': {},
        'read': {},
        'summary': {}
    }

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-600')
        s3_client.create_bucket(bucket_name)

        # Test different object sizes
        test_sizes = {
            'tiny': 1024,           # 1KB
            'small': 100 * 1024,    # 100KB
            'medium': 1024 * 1024,  # 1MB
            'large': 10 * 1024 * 1024  # 10MB
        }

        # Number of iterations per size
        iterations = 10

        print("\n=== Sequential Performance Test ===")

        for size_name, size_bytes in test_sizes.items():
            write_times = []
            read_times = []
            write_throughput = []
            read_throughput = []

            # Generate test data once for this size
            test_data = os.urandom(size_bytes)

            print(f"\nTesting {size_name} ({size_bytes / 1024:.1f} KB)...")

            # Sequential writes
            for i in range(iterations):
                key = f'perf-{size_name}-{i}.dat'

                # Write performance
                start_time = time.time()
                s3_client.put_object(bucket_name, key, io.BytesIO(test_data))
                write_time = time.time() - start_time

                write_times.append(write_time)
                write_throughput.append(size_bytes / write_time / (1024 * 1024))  # MB/s

                # Read performance
                start_time = time.time()
                response = s3_client.get_object(bucket_name, key)
                data = response['Body'].read()
                read_time = time.time() - start_time

                read_times.append(read_time)
                read_throughput.append(size_bytes / read_time / (1024 * 1024))  # MB/s

                # Verify data integrity
                assert len(data) == size_bytes, f"Size mismatch for {key}"

            # Calculate statistics
            results['write'][size_name] = {
                'avg_latency': statistics.mean(write_times),
                'min_latency': min(write_times),
                'max_latency': max(write_times),
                'p50_latency': statistics.median(write_times),
                'p95_latency': sorted(write_times)[int(len(write_times) * 0.95)],
                'avg_throughput_mbps': statistics.mean(write_throughput),
                'operations': iterations
            }

            results['read'][size_name] = {
                'avg_latency': statistics.mean(read_times),
                'min_latency': min(read_times),
                'max_latency': max(read_times),
                'p50_latency': statistics.median(read_times),
                'p95_latency': sorted(read_times)[int(len(read_times) * 0.95)],
                'avg_throughput_mbps': statistics.mean(read_throughput),
                'operations': iterations
            }

            # Print summary for this size
            print(f"  Write: avg={results['write'][size_name]['avg_latency']:.3f}s, "
                  f"p50={results['write'][size_name]['p50_latency']:.3f}s, "
                  f"p95={results['write'][size_name]['p95_latency']:.3f}s, "
                  f"throughput={results['write'][size_name]['avg_throughput_mbps']:.1f} MB/s")
            print(f"  Read:  avg={results['read'][size_name]['avg_latency']:.3f}s, "
                  f"p50={results['read'][size_name]['p50_latency']:.3f}s, "
                  f"p95={results['read'][size_name]['p95_latency']:.3f}s, "
                  f"throughput={results['read'][size_name]['avg_throughput_mbps']:.1f} MB/s")

        # Overall summary
        total_write_ops = sum(iterations for _ in test_sizes)
        total_read_ops = total_write_ops
        total_data_written = sum(size * iterations for size in test_sizes.values())
        total_data_read = total_data_written

        all_write_times = []
        all_read_times = []
        for size_name in test_sizes:
            all_write_times.extend([results['write'][size_name]['avg_latency']] * iterations)
            all_read_times.extend([results['read'][size_name]['avg_latency']] * iterations)

        results['summary'] = {
            'total_operations': total_write_ops + total_read_ops,
            'total_data_transferred_mb': (total_data_written + total_data_read) / (1024 * 1024),
            'avg_write_latency': statistics.mean(all_write_times) if all_write_times else 0,
            'avg_read_latency': statistics.mean(all_read_times) if all_read_times else 0
        }

        print("\n=== Performance Summary ===")
        print(f"Total operations: {results['summary']['total_operations']}")
        print(f"Total data transferred: {results['summary']['total_data_transferred_mb']:.1f} MB")
        print(f"Average write latency: {results['summary']['avg_write_latency']:.3f}s")
        print(f"Average read latency: {results['summary']['avg_read_latency']:.3f}s")

        # Performance assertions for production readiness
        # Small objects should have low latency
        assert results['write']['tiny']['p95_latency'] < 1.0, \
            "Small object write p95 should be < 1s"
        assert results['read']['tiny']['p95_latency'] < 0.5, \
            "Small object read p95 should be < 0.5s"

        # Large objects should have reasonable throughput
        assert results['write']['large']['avg_throughput_mbps'] > 1.0, \
            "Large object write throughput should be > 1 MB/s"
        assert results['read']['large']['avg_throughput_mbps'] > 2.0, \
            "Large object read throughput should be > 2 MB/s"

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
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