#!/usr/bin/env python3
"""
Test 2400: Inference pipeline 2400

Tests ML inference pipeline setup
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2400(s3_client, config):
    """Inference pipeline 2400"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2400')
        s3_client.create_bucket(bucket_name)

        # Test inference pipeline artifacts
        pipeline_key = f'ml-inference/pipelines/pipeline-2400/'

        # Pipeline configuration
        pipeline_config = {
            'pipeline_id': f'PIPELINE-002400',
            'stages': [
                {'name': 'preprocessing', 'type': 'transform', 'timeout': 100},
                {'name': 'inference', 'type': 'model', 'timeout': 500},
                {'name': 'postprocessing', 'type': 'transform', 'timeout': 100}
            ],
            'batch_size': 64,
            'max_latency_ms': 1000,
            'auto_scaling': {
                'min_instances': 1,
                'max_instances': 10,
                'target_utilization': 0.7
            }
        }

        # Store pipeline components
        for stage in pipeline_config['stages']:
            component_key = f"{pipeline_key}{stage['name']}/component.pkl"
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=component_key,
                Body=io.BytesIO(b'COMPONENT_DATA' * 1000),
                Metadata={
                    'pipeline-id': pipeline_config['pipeline_id'],
                    'stage-name': stage['name'],
                    'stage-type': stage['type'],
                    'timeout-ms': str(stage['timeout'])
                }
            )

        print(f"Inference pipeline test 2400: ✓")

        print(f"\nTest 2400 - Inference pipeline 2400: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2400 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2400: {error_code}")
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
