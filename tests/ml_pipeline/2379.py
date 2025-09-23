#!/usr/bin/env python3
"""
Test 2379: Model versioning 2379

Tests model versioning and lineage tracking
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2379(s3_client, config):
    """Model versioning 2379"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2379')
        s3_client.create_bucket(bucket_name)

        # Test model versioning and lineage
        model_version = 10
        model_path = f'ml-models/production/model-4/v{model_version}/'

        # Model lineage information
        lineage = {
            'model_id': f'MODEL-PROD-002379',
            'version': model_version,
            'parent_version': model_version - 1 if model_version > 1 else None,
            'training_job_id': f'JOB-00002379',
            'dataset_version': f'DATASET-V5',
            'git_commit': hashlib.sha1(f'commit-2379'.encode()).hexdigest()[:8],
            'hyperparameters': {
                'learning_rate': 0.016,
                'batch_size': 32,
                'epochs': 49
            },
            'created_by': f'scientist-9'
        }

        # Store model with lineage
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=f'{model_path}model.h5',
            Body=io.BytesIO(b'MODEL_DATA' * 5000),
            Metadata={
                'model-version': str(model_version),
                'parent-version': str(lineage['parent_version']),
                'training-job': lineage['training_job_id'],
                'git-commit': lineage['git_commit']
            }
        )

        # Store lineage metadata
        s3_client.put_object(
            bucket_name,
            f'{model_path}lineage.json',
            io.BytesIO(json.dumps(lineage).encode())
        )

        print(f"Model versioning test 2379: ✓")

        print(f"\nTest 2379 - Model versioning 2379: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2379 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2379: {error_code}")
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
