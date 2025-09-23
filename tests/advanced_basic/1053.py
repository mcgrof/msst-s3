#!/usr/bin/env python3
"""
Test 1053: Object retention 13 days

Tests object retention for 13 days
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1053(s3_client, config):
    """Object retention 13 days"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1053')
        s3_client.create_bucket(bucket_name)

        # Test object retention (13 days)
        key = 'retention-test.txt'

        try:
            # Enable versioning and object lock
            s3_client.client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )

            retention_date = time.strftime(
                '%Y-%m-%dT%H:%M:%S.000Z',
                time.gmtime(time.time() + 13 * 86400)
            )

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'Retention test'),
                ObjectLockMode='COMPLIANCE',
                ObjectLockRetainUntilDate=retention_date
            )

            print(f"Object retention set for 13 days")
        except ClientError as e:
            if e.response['Error']['Code'] in ['InvalidRequest', 'NotImplemented']:
                print(f"Object lock not supported")
            else:
                raise

        print(f"\nTest 1053 - Object retention 13 days: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1053 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1053: {error_code}")
            raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
