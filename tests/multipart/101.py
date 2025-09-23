#!/usr/bin/env python3
"""
Test 101: Parallel part uploads

Tests parallel upload of multipart segments.
Validates concurrent part handling and assembly.
"""

import hashlib
import io
import os
import threading
import time
from common.fixtures import TestFixture

def upload_part_thread(s3_client, bucket, key, upload_id, part_num,
                       data, results, index):
    """Thread function for parallel part upload"""
    try:
        response = s3_client.upload_part(
            bucket, key, upload_id, part_num, io.BytesIO(data)
        )
        results[index] = {
            'success': True,
            'part_number': part_num,
            'etag': response['ETag']
        }
    except Exception as e:
        results[index] = {
            'success': False,
            'part_number': part_num,
            'error': str(e)
        }

def test_101(s3_client, config):
    """Parallel part uploads test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None
    upload_id = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-101')
        s3_client.create_bucket(bucket_name)

        object_key = 'parallel-multipart.bin'

        # Create 20MB file with 4 parts of 5MB each
        part_size = 5 * 1024 * 1024  # 5MB per part
        num_parts = 4
        total_size = part_size * num_parts

        # Initiate multipart upload
        upload_id = s3_client.create_multipart_upload(bucket_name, object_key)

        # Prepare parts data
        parts_data = []
        full_hasher = hashlib.sha256()

        for i in range(num_parts):
            part_data = os.urandom(part_size)
            parts_data.append(part_data)
            full_hasher.update(part_data)

        original_hash = full_hasher.hexdigest()

        # Upload parts in parallel
        threads = []
        results = {}
        start_time = time.time()

        for i in range(num_parts):
            thread = threading.Thread(
                target=upload_part_thread,
                args=(s3_client, bucket_name, object_key, upload_id,
                      i + 1, parts_data[i], results, i)
            )
            threads.append(thread)
            thread.start()

        # Wait for all uploads
        for thread in threads:
            thread.join(timeout=60)

        upload_duration = time.time() - start_time

        # Verify all parts uploaded successfully
        failed_parts = [r for r in results.values() if not r['success']]
        assert not failed_parts, f"Some parts failed: {failed_parts}"

        # Sort parts by part number for completion
        parts = []
        for i in range(num_parts):
            assert i in results, f"Missing result for part {i}"
            parts.append({
                'PartNumber': results[i]['part_number'],
                'ETag': results[i]['etag']
            })

        parts.sort(key=lambda x: x['PartNumber'])

        # Complete multipart upload
        s3_client.complete_multipart_upload(
            bucket_name, object_key, upload_id, parts
        )
        upload_id = None  # Mark as completed

        # Verify object integrity
        response = s3_client.get_object(bucket_name, object_key)
        downloaded_data = response['Body'].read()

        download_hasher = hashlib.sha256()
        download_hasher.update(downloaded_data)
        downloaded_hash = download_hasher.hexdigest()

        assert downloaded_hash == original_hash, \
            f"Data corrupted: expected {original_hash}, got {downloaded_hash}"
        assert len(downloaded_data) == total_size, \
            f"Size mismatch: expected {total_size}, got {len(downloaded_data)}"

        # Performance check - parallel should be reasonably fast
        assert upload_duration < 30, \
            f"Parallel upload too slow: {upload_duration}s for {num_parts} parts"

        # Calculate upload speed
        upload_speed_mbps = (total_size / (1024 * 1024)) / upload_duration
        print(f"Parallel upload speed: {upload_speed_mbps:.2f} MB/s")

    finally:
        # Cleanup
        if upload_id and bucket_name:
            try:
                s3_client.abort_multipart_upload(
                    bucket_name, object_key, upload_id
                )
            except:
                pass

        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                s3_client.delete_object(bucket_name, object_key)
                s3_client.delete_bucket(bucket_name)
            except:
                pass