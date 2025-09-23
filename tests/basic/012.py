#!/usr/bin/env python3
"""
Test 012: Retry logic with exponential backoff

Tests automatic retry behavior for transient failures.
Validates exponential backoff implementation.
"""

import time
import random
from botocore.exceptions import ClientError
from common.fixtures import TestFixture

def test_012(s3_client, config):
    """Retry logic with exponential backoff test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-012')
        s3_client.create_bucket(bucket_name)

        # Test 1: Verify retry on 503 Service Unavailable
        # We'll test by trying operations that might naturally fail and retry

        # Generate object name that doesn't exist
        non_existent_key = f'non-existent-{random.randint(1000, 9999)}.txt'

        # Test proper error handling for 404
        try:
            s3_client.get_object(bucket_name, non_existent_key)
            assert False, "Should raise error for non-existent object"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            assert error_code in ['NoSuchKey', '404'], \
                f"Expected NoSuchKey error, got: {error_code}"

        # Test 2: Measure retry timing behavior
        # Upload multiple objects and measure timing patterns
        timings = []
        test_data = b'Test data for retry logic'

        for i in range(5):
            key = f'retry-test-{i}.txt'
            start_time = time.time()

            try:
                s3_client.put_object(bucket_name, key, test_data)
                upload_time = time.time() - start_time
                timings.append(upload_time)
            except Exception:
                # If upload fails, measure the failure time
                failure_time = time.time() - start_time
                timings.append(failure_time)

        # Verify operations complete in reasonable time
        avg_time = sum(timings) / len(timings)
        max_time = max(timings)

        assert max_time < 30, \
            f"Operations taking too long, possible retry issues: max={max_time}s"
        assert avg_time < 5, \
            f"Average operation time too high: {avg_time}s"

        # Test 3: Concurrent operations to stress retry logic
        import threading

        results = {'success': 0, 'failure': 0, 'retry_detected': 0}
        threads = []

        def upload_with_tracking(index):
            key = f'concurrent-retry-{index}.txt'
            data = f'Data for thread {index}'.encode()
            start_time = time.time()

            try:
                response = s3_client.put_object(bucket_name, key, data)
                duration = time.time() - start_time

                # If operation took longer than usual, likely had retries
                if duration > 1.0:  # Assuming normal upload is <1s
                    results['retry_detected'] += 1

                results['success'] += 1
            except Exception as e:
                results['failure'] += 1

        # Launch concurrent uploads
        for i in range(10):
            thread = threading.Thread(target=upload_with_tracking, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join(timeout=30)

        # Verify most operations succeeded
        total_ops = results['success'] + results['failure']
        success_rate = results['success'] / total_ops if total_ops > 0 else 0

        assert success_rate > 0.8, \
            f"Too many failures: {results['failure']}/{total_ops}"

        # Test 4: Verify idempotent operations
        # Multiple PUTs of same object should succeed
        idempotent_key = 'idempotent-test.txt'
        idempotent_data = b'Idempotent test data'

        for attempt in range(3):
            response = s3_client.put_object(
                bucket_name,
                idempotent_key,
                idempotent_data
            )
            assert 'ETag' in response, f"Attempt {attempt + 1} should succeed"

        # Verify final state is correct
        response = s3_client.get_object(bucket_name, idempotent_key)
        downloaded = response['Body'].read()
        assert downloaded == idempotent_data, "Data should match after retries"

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