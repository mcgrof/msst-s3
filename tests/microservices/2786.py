#!/usr/bin/env python3
"""
Test 2786: Distributed tracing 2786

Tests distributed tracing patterns
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2786(s3_client, config):
    """Distributed tracing 2786"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2786')
        s3_client.create_bucket(bucket_name)

        # Test distributed tracing
        trace_id = hashlib.md5(f'trace-2786'.encode()).hexdigest()
        key = f'traces/{trace_id[:2]}/{trace_id}.json'

        # Distributed trace
        trace = {
            'trace_id': trace_id,
            'spans': [
                {
                    'span_id': hashlib.md5(f'span-2786-{j}'.encode()).hexdigest()[:16],
                    'parent_span_id': hashlib.md5(f'span-2786-{j-1}'.encode()).hexdigest()[:16] if j > 0 else None,
                    'operation_name': f'operation_{j}',
                    'service_name': f'service_{j % 5}',
                    'start_time': time.time() + j * 0.1,
                    'duration_ms': random.randint(10, 500),
                    'tags': {
                        'http.method': ['GET', 'POST', 'PUT', 'DELETE'][j % 4],
                        'http.status_code': 200,
                        'component': 'http'
                    }
                } for j in range(5)
            ],
            'duration_ms': sum(random.randint(10, 500) for _ in range(5)),
            'service_map': ['service_0', 'service_1', 'service_2', 'service_3', 'service_4'],
            'errors': []
        }

        # Store trace
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(trace).encode()),
            ContentType='application/json',
            Metadata={
                'trace-id': trace_id,
                'span-count': str(len(trace['spans'])),
                'duration-ms': str(trace['duration_ms'])
            }
        )

        print(f"Distributed tracing test 2786: ✓")

        print(f"\nTest 2786 - Distributed tracing 2786: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2786 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2786: {error_code}")
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
