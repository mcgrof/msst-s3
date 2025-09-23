#!/usr/bin/env python3
"""
Test 2443: RT analytics 2443

Tests real-time analytics buffering
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2443(s3_client, config):
    """RT analytics 2443"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2443')
        s3_client.create_bucket(bucket_name)

        # Test real-time analytics buffer
        window_start = int(time.time()) - (i % 60)
        window_end = window_start + 60  # 1-minute windows

        # Aggregated analytics data
        analytics = {
            'window': {
                'start': window_start,
                'end': window_end,
                'duration_seconds': 60
            },
            'metrics': {
                'event_count': random.randint(1000, 10000),
                'unique_users': random.randint(100, 1000),
                'avg_response_time_ms': random.uniform(10, 100),
                'error_rate': random.uniform(0, 0.05),
                'throughput_rps': random.randint(100, 1000)
            },
            'top_events': [
                {'event': f'event_{j}', 'count': random.randint(100, 1000)}
                for j in range(5)
            ]
        }

        # Store in analytics buffer
        key = f'analytics/realtime/window-{window_start}-{window_end}.json'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(analytics).encode()),
            Metadata={
                'window-start': str(window_start),
                'window-end': str(window_end),
                'processing-time': str(int(time.time())),
                'late-arrival-tolerance': '300'  # 5 minutes
            }
        )

        print(f"Real-time analytics test 2443: ✓")

        print(f"\nTest 2443 - RT analytics 2443: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2443 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2443: {error_code}")
            raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Clean up all objects including versions
                try:
                    versions = s3_client.client.list_object_versions(Bucket=bucket_name)
                    for version in versions.get('Versions', []):
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=version['Key'],
                            VersionId=version['VersionId']
                        )
                    for marker in versions.get('DeleteMarkers', []):
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=marker['Key'],
                            VersionId=marker['VersionId']
                        )
                except:
                    # Fallback to simple deletion
                    objects = s3_client.list_objects(bucket_name)
                    for obj in objects:
                        s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass
