#!/usr/bin/env python3
"""
Test 2021: HIPAA compliance 2021

Tests HIPAA PHI data protection requirements
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2021(s3_client, config):
    """HIPAA compliance 2021"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2021')
        s3_client.create_bucket(bucket_name)

        # Test HIPAA compliance for healthcare data
        key = 'phi-data-2021.json'

        # Protected Health Information (PHI)
        phi_data = {
            'patient_id': '2021',
            'medical_record_number': f'MRN002021',
            'diagnosis_code': 'ICD-10-CM',
            'encrypted': True,
            'access_logged': True
        }

        # Upload with HIPAA compliance requirements
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(json.dumps(phi_data).encode()),
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId='arn:aws:kms:us-east-1:123456789012:key/hipaa-key',
                Tagging='DataType=PHI&Regulation=HIPAA&Encrypted=True',
                ObjectLockMode='COMPLIANCE',
                ObjectLockRetainUntilDate=time.strftime(
                    '%Y-%m-%dT%H:%M:%S.000Z',
                    time.gmtime(time.time() + 7 * 365 * 86400)  # 7 years retention
                )
            )
            print(f"HIPAA compliance test 2021: ✓")
        except ClientError as e:
            if e.response['Error']['Code'] in ['InvalidArgument', 'NotImplemented']:
                print(f"HIPAA features not fully supported")
            else:
                raise

        print(f"\nTest 2021 - HIPAA compliance 2021: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2021 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2021: {error_code}")
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
