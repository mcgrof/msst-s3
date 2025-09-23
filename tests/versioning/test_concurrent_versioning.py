#!/usr/bin/env python3
"""
Test 002: Concurrent Versioning Race Conditions
Tests versioning behavior under concurrent writes to the same key
to detect race conditions and ordering issues.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import threading
import time

def test_concurrent_versioning(s3_client: S3Client):
    """Test concurrent writes with versioning enabled"""
    bucket_name = f's3-concurrent-version-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)

        # Enable versioning
        s3_client.client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )

        results = {'versions': [], 'errors': [], 'race_conditions': []}
        lock = threading.Lock()

        def concurrent_write(thread_id, iterations=10):
            """Write objects concurrently"""
            for i in range(iterations):
                try:
                    key = f'concurrent-key-{i % 3}'  # Reuse 3 keys
                    data = f'Thread-{thread_id}-Iteration-{i}'.encode()

                    response = s3_client.client.put_object(
                        Bucket=bucket_name,
                        Key=key,
                        Body=io.BytesIO(data)
                    )

                    with lock:
                        results['versions'].append({
                            'thread': thread_id,
                            'iteration': i,
                            'key': key,
                            'version_id': response.get('VersionId', 'null'),
                            'etag': response['ETag']
                        })

                except Exception as e:
                    with lock:
                        results['errors'].append(f"Thread {thread_id}: {str(e)}")

        # Start concurrent threads
        threads = []
        num_threads = 5

        print(f"Starting {num_threads} concurrent threads...")
        for i in range(num_threads):
            t = threading.Thread(target=concurrent_write, args=(i,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        print(f"Concurrent writes completed: {len(results['versions'])} versions created")

        # Verify versioning integrity
        for key_num in range(3):
            key = f'concurrent-key-{key_num}'

            # List all versions
            versions_response = s3_client.client.list_object_versions(
                Bucket=bucket_name,
                Prefix=key
            )

            versions = versions_response.get('Versions', [])
            print(f"\nKey '{key}': {len(versions)} versions")

            # Check for version ordering issues
            version_times = []
            for v in versions:
                version_times.append(v['LastModified'])

            # Check if versions are properly ordered by time
            if version_times != sorted(version_times, reverse=True):
                results['race_conditions'].append(f"Version ordering issue for {key}")
                print(f"⚠️  Version ordering issue detected for {key}")

            # Try to retrieve each version
            for v in versions[:3]:  # Check first 3 versions
                try:
                    obj = s3_client.client.get_object(
                        Bucket=bucket_name,
                        Key=key,
                        VersionId=v['VersionId']
                    )
                    content = obj['Body'].read().decode()
                    print(f"  Version {v['VersionId'][:8]}: {content[:30]}")
                except Exception as e:
                    results['errors'].append(f"Failed to retrieve version: {e}")

        # Test simultaneous deletion and creation
        print("\nTesting simultaneous delete and create...")

        def delete_create_race(iteration):
            """Test delete/create race condition"""
            key = 'race-condition-key'
            try:
                if iteration % 2 == 0:
                    # Delete
                    s3_client.client.delete_object(
                        Bucket=bucket_name,
                        Key=key
                    )
                else:
                    # Create
                    s3_client.client.put_object(
                        Bucket=bucket_name,
                        Key=key,
                        Body=io.BytesIO(f'Data-{iteration}'.encode())
                    )
            except Exception as e:
                with lock:
                    results['errors'].append(f"Delete/Create race: {str(e)}")

        # Run delete/create race test
        race_threads = []
        for i in range(10):
            t = threading.Thread(target=delete_create_race, args=(i,))
            race_threads.append(t)
            t.start()

        for t in race_threads:
            t.join()

        # Check final state
        try:
            final_versions = s3_client.client.list_object_versions(
                Bucket=bucket_name,
                Prefix='race-condition-key'
            )
            num_versions = len(final_versions.get('Versions', []))
            num_delete_markers = len(final_versions.get('DeleteMarkers', []))

            print(f"\nRace condition key final state:")
            print(f"  Versions: {num_versions}")
            print(f"  Delete markers: {num_delete_markers}")

        except Exception as e:
            results['errors'].append(f"Failed to check final state: {e}")

        # Summary
        print(f"\n=== Concurrent Versioning Test Results ===")
        print(f"Total versions created: {len(results['versions'])}")
        print(f"Errors encountered: {len(results['errors'])}")
        print(f"Race conditions detected: {len(results['race_conditions'])}")

        if results['errors']:
            print("\nErrors:")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")

        if results['race_conditions']:
            print("\nRace conditions:")
            for condition in results['race_conditions']:
                print(f"  - {condition}")

        return len(results['errors']) == 0 and len(results['race_conditions']) == 0

    finally:
        # Cleanup
        try:
            # Delete all versions
            versions = s3_client.client.list_object_versions(Bucket=bucket_name)

            # Delete all object versions
            if 'Versions' in versions:
                for version in versions['Versions']:
                    s3_client.client.delete_object(
                        Bucket=bucket_name,
                        Key=version['Key'],
                        VersionId=version['VersionId']
                    )

            # Delete all delete markers
            if 'DeleteMarkers' in versions:
                for marker in versions['DeleteMarkers']:
                    s3_client.client.delete_object(
                        Bucket=bucket_name,
                        Key=marker['Key'],
                        VersionId=marker['VersionId']
                    )

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
    test_concurrent_versioning(s3)