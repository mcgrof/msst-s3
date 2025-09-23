#!/usr/bin/env python3
"""
Test 2639: Edge processing 2639

Tests edge location data processing
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2639(s3_client, config):
    """Edge processing 2639"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2639')
        s3_client.create_bucket(bucket_name)

        # Test edge location processing
        edge_location = ['us-east-1-edge', 'eu-central-1-edge', 'ap-northeast-1-edge'][i % 3]
        key = f'edge/{edge_location}/processed-2639.json'

        # Edge processed data
        edge_data = {
            'original_size': 1024 * random.randint(100, 1000),
            'processed_size': 1024 * random.randint(10, 100),
            'compression_ratio': random.uniform(0.1, 0.9),
            'processing_time_ms': random.randint(10, 500),
            'edge_location': edge_location,
            'transformations': ['resize', 'compress', 'convert', 'optimize'],
            'cache_ttl': 3600 * (i % 24 + 1)
        }

        # Store edge processed result
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(edge_data).encode()),
            Metadata={
                'edge-location': edge_location,
                'processing-type': 'edge-compute',
                'latency-ms': str(edge_data['processing_time_ms']),
                'cache-ttl': str(edge_data['cache_ttl'])
            }
        )

        print(f"Edge processing test 2639: ✓")

        print(f"\nTest 2639 - Edge processing 2639: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2639 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2639: {error_code}")
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
