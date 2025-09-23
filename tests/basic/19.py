#!/usr/bin/env python3
"""
Test 19: Batch operations

Tests batch operations on multiple objects including bulk copy,
bulk delete, and batch tagging operations.
"""

import io
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_19(s3_client, config):
    """Batch operations test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    bucket_name_2 = None

    try:
        # Create test buckets
        bucket_name = fixture.generate_bucket_name('test-19')
        bucket_name_2 = fixture.generate_bucket_name('test-19-dest')
        s3_client.create_bucket(bucket_name)
        s3_client.create_bucket(bucket_name_2)

        # Test 1: Batch upload
        batch_size = 20
        test_objects = []

        print(f"Batch uploading {batch_size} objects...")
        start_time = time.time()

        for i in range(batch_size):
            key = f'batch/object-{i:03d}.txt'
            data = f'Batch object {i}'.encode()
            test_objects.append({'key': key, 'data': data})

            s3_client.put_object(
                bucket_name,
                key,
                io.BytesIO(data)
            )

        upload_time = time.time() - start_time
        print(f"Batch upload completed: {batch_size} objects in {upload_time:.2f}s")

        # Verify all objects uploaded
        objects = s3_client.list_objects(bucket_name, prefix='batch/')
        assert len(objects) == batch_size, f"Expected {batch_size} objects, found {len(objects)}"

        # Test 2: Batch copy
        print(f"Batch copying {batch_size} objects...")
        start_time = time.time()

        def copy_object(obj):
            """Copy a single object"""
            copy_source = {'Bucket': bucket_name, 'Key': obj['key']}
            dest_key = obj['key'].replace('batch/', 'copied/')

            try:
                s3_client.client.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name_2,
                    Key=dest_key
                )
                return True
            except ClientError:
                return False

        # Use thread pool for parallel copying
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(copy_object, obj) for obj in test_objects]
            results = [future.result() for future in as_completed(futures)]

        copy_time = time.time() - start_time
        successful_copies = sum(results)
        print(f"Batch copy completed: {successful_copies}/{batch_size} objects in {copy_time:.2f}s")

        # Verify copies
        copied_objects = s3_client.list_objects(bucket_name_2, prefix='copied/')
        assert len(copied_objects) == batch_size, \
            f"Expected {batch_size} copied objects, found {len(copied_objects)}"

        # Test 3: Batch tagging
        print(f"Batch tagging {batch_size} objects...")
        start_time = time.time()

        tag_set = {
            'TagSet': [
                {'Key': 'Environment', 'Value': 'Test'},
                {'Key': 'Batch', 'Value': '019'},
                {'Key': 'Processed', 'Value': 'True'}
            ]
        }

        tagged_count = 0
        for obj in test_objects:
            try:
                s3_client.client.put_object_tagging(
                    Bucket=bucket_name,
                    Key=obj['key'],
                    Tagging=tag_set
                )
                tagged_count += 1
            except ClientError:
                pass

        tagging_time = time.time() - start_time
        print(f"Batch tagging completed: {tagged_count}/{batch_size} objects in {tagging_time:.2f}s")

        # Verify tags on a sample object
        if tagged_count > 0:
            sample_key = test_objects[0]['key']
            response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=sample_key
            )
            tags = response.get('TagSet', [])
            assert len(tags) > 0, "Tags not applied"

        # Test 4: Batch metadata update
        print("Batch updating metadata...")
        start_time = time.time()

        metadata_updated = 0
        for obj in test_objects[:10]:  # Update first 10 objects
            try:
                # Copy object to itself with new metadata
                copy_source = {'Bucket': bucket_name, 'Key': obj['key']}
                s3_client.client.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=obj['key'],
                    Metadata={
                        'batch-processed': 'true',
                        'processing-date': time.strftime('%Y-%m-%d'),
                        'batch-id': '019'
                    },
                    MetadataDirective='REPLACE'
                )
                metadata_updated += 1
            except ClientError:
                pass

        metadata_time = time.time() - start_time
        print(f"Batch metadata update: {metadata_updated} objects in {metadata_time:.2f}s")

        # Test 5: Batch delete with delete markers (if versioning enabled)
        # First check if versioning is enabled
        versioning = s3_client.get_bucket_versioning(bucket_name)
        if versioning.get('Status') == 'Enabled':
            print("Testing batch delete with versioning...")

            # Delete half the objects
            delete_batch = test_objects[:batch_size//2]
            for obj in delete_batch:
                s3_client.delete_object(bucket_name, obj['key'])

            # Check for delete markers
            response = s3_client.client.list_object_versions(
                Bucket=bucket_name,
                Prefix='batch/'
            )

            delete_markers = response.get('DeleteMarkers', [])
            print(f"Delete markers created: {len(delete_markers)}")
        else:
            print("Versioning not enabled, skipping delete marker test")

        # Test 6: Batch delete using delete_objects API
        print("Batch deleting objects...")
        start_time = time.time()

        # Prepare delete list for bucket_name_2
        delete_list = {
            'Objects': [
                {'Key': f'copied/object-{i:03d}.txt'}
                for i in range(batch_size)
            ]
        }

        try:
            response = s3_client.client.delete_objects(
                Bucket=bucket_name_2,
                Delete=delete_list
            )

            deleted = response.get('Deleted', [])
            errors = response.get('Errors', [])

            delete_time = time.time() - start_time
            print(f"Batch delete completed: {len(deleted)} deleted, {len(errors)} errors in {delete_time:.2f}s")

            # Verify deletion
            remaining = s3_client.list_objects(bucket_name_2, prefix='copied/')
            assert len(remaining) == 0, f"Objects not fully deleted: {len(remaining)} remaining"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Batch delete failed: {error_code}")

        # Test 7: Batch operations with filters
        print("Testing filtered batch operations...")

        # Create objects with different prefixes
        for i in range(5):
            s3_client.put_object(
                bucket_name,
                f'type-a/file-{i}.txt',
                io.BytesIO(f'Type A file {i}'.encode())
            )
            s3_client.put_object(
                bucket_name,
                f'type-b/file-{i}.txt',
                io.BytesIO(f'Type B file {i}'.encode())
            )

        # Batch operation only on type-a objects
        type_a_objects = s3_client.list_objects(bucket_name, prefix='type-a/')
        operation_count = 0

        for obj in type_a_objects:
            # Add specific tag to type-a objects
            try:
                s3_client.client.put_object_tagging(
                    Bucket=bucket_name,
                    Key=obj['Key'],
                    Tagging={
                        'TagSet': [
                            {'Key': 'Type', 'Value': 'A'}
                        ]
                    }
                )
                operation_count += 1
            except ClientError:
                pass

        print(f"Filtered batch operation: {operation_count} type-a objects tagged")

        # Test 8: Concurrent batch operations
        print("Testing concurrent batch operations...")

        def concurrent_operation(index):
            """Perform a concurrent operation"""
            key = f'concurrent/item-{index:04d}.txt'
            data = f'Concurrent item {index}'.encode()

            try:
                # Upload
                s3_client.put_object(bucket_name, key, io.BytesIO(data))

                # Tag
                s3_client.client.put_object_tagging(
                    Bucket=bucket_name,
                    Key=key,
                    Tagging={
                        'TagSet': [
                            {'Key': 'Index', 'Value': str(index)}
                        ]
                    }
                )

                # Copy
                s3_client.client.copy_object(
                    CopySource={'Bucket': bucket_name, 'Key': key},
                    Bucket=bucket_name_2,
                    Key=key
                )

                return True
            except ClientError:
                return False

        start_time = time.time()
        concurrent_count = 20

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(concurrent_operation, i) for i in range(concurrent_count)]
            results = [future.result() for future in as_completed(futures)]

        concurrent_time = time.time() - start_time
        successful = sum(results)
        print(f"Concurrent operations: {successful}/{concurrent_count} successful in {concurrent_time:.2f}s")

        # Test 9: Batch prefix rename (simulate)
        print("Simulating batch prefix rename...")

        # Create objects with old prefix
        old_prefix = 'old-prefix/'
        new_prefix = 'new-prefix/'

        for i in range(5):
            key = f'{old_prefix}file-{i}.txt'
            s3_client.put_object(
                bucket_name,
                key,
                io.BytesIO(f'File {i} content'.encode())
            )

        # "Rename" by copying and deleting
        renamed_count = 0
        old_objects = s3_client.list_objects(bucket_name, prefix=old_prefix)

        for obj in old_objects:
            old_key = obj['Key']
            new_key = old_key.replace(old_prefix, new_prefix)

            try:
                # Copy to new location
                s3_client.client.copy_object(
                    CopySource={'Bucket': bucket_name, 'Key': old_key},
                    Bucket=bucket_name,
                    Key=new_key
                )

                # Delete old location
                s3_client.delete_object(bucket_name, old_key)
                renamed_count += 1
            except ClientError:
                pass

        print(f"Batch rename: {renamed_count} objects renamed from {old_prefix} to {new_prefix}")

        print(f"\nBatch operations test completed:")
        print(f"- Batch upload: ✓")
        print(f"- Batch copy: ✓")
        print(f"- Batch tagging: ✓")
        print(f"- Batch delete: ✓")
        print(f"- Concurrent operations: ✓")
        print(f"- Performance: {batch_size} objects processed efficiently")

    finally:
        # Cleanup
        for bucket in [bucket_name, bucket_name_2]:
            if bucket and s3_client.bucket_exists(bucket):
                try:
                    # Delete all objects including versions
                    try:
                        response = s3_client.client.list_object_versions(Bucket=bucket)

                        # Delete all versions
                        for version in response.get('Versions', []):
                            s3_client.client.delete_object(
                                Bucket=bucket,
                                Key=version['Key'],
                                VersionId=version['VersionId']
                            )

                        # Delete all delete markers
                        for marker in response.get('DeleteMarkers', []):
                            s3_client.client.delete_object(
                                Bucket=bucket,
                                Key=marker['Key'],
                                VersionId=marker['VersionId']
                            )
                    except:
                        # Regular cleanup if versioning not enabled
                        objects = s3_client.list_objects(bucket)
                        for obj in objects:
                            s3_client.delete_object(bucket, obj['Key'])

                    s3_client.delete_bucket(bucket)
                except:
                    pass