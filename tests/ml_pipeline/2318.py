#!/usr/bin/env python3
"""
Test 2318: Model storage 2318

Tests ML model artifact storage
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2318(s3_client, config):
    """Model storage 2318"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2318')
        s3_client.create_bucket(bucket_name)

        # Test ML model artifact storage
        model_key = f'ml-models/model-2318/artifacts/model.pkl'

        # Model metadata
        model_metadata = {
            'model_id': f'MODEL-002318',
            'model_type': ['tensorflow', 'pytorch', 'sklearn', 'xgboost'][i % 4],
            'version': f'v8.3.2',
            'metrics': {
                'accuracy': 0.95 + random.uniform(-0.05, 0.05),
                'precision': 0.92 + random.uniform(-0.05, 0.05),
                'recall': 0.93 + random.uniform(-0.05, 0.05),
                'f1_score': 0.94 + random.uniform(-0.05, 0.05)
            },
            'training_date': time.strftime('%Y-%m-%d'),
            'framework_version': '2.10.0'
        }

        # Upload model artifact
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=model_key,
            Body=io.BytesIO(b'MODEL_BINARY_DATA' * 1000),  # Simulated model
            Metadata={
                'model-id': model_metadata['model_id'],
                'model-type': model_metadata['model_type'],
                'model-version': model_metadata['version'],
                'accuracy': str(model_metadata['metrics']['accuracy'])
            },
            Tagging='MLModel=True&Production=False&Framework=' + model_metadata['model_type']
        )

        # Store model metadata
        metadata_key = f'ml-models/model-2318/metadata.json'
        s3_client.put_object(
            bucket_name,
            metadata_key,
            io.BytesIO(json.dumps(model_metadata).encode())
        )

        print(f"ML model storage test 2318: ✓")

        print(f"\nTest 2318 - Model storage 2318: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2318 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2318: {error_code}")
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
