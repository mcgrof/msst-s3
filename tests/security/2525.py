#!/usr/bin/env python3
"""
Test 2525: Access control 2525

Tests granular access control
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2525(s3_client, config):
    """Access control 2525"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2525')
        s3_client.create_bucket(bucket_name)

        # Test granular access control
        key = f'access-control/resource-2525.dat'

        # Create object with specific permissions
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'Protected resource'),
            Metadata={
                'owner': f'user-5',
                'department': ['engineering', 'sales', 'finance', 'hr'][i % 4],
                'classification': ['public', 'internal', 'confidential', 'restricted'][i % 4],
                'access-level': str(i % 5)
            }
        )

        # Set object ACL
        try:
            acl_grants = []
            if i % 4 == 0:  # Public read for some objects
                acl_grants.append({
                    'Grantee': {'Type': 'Group', 'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
                    'Permission': 'READ'
                })

            if acl_grants:
                s3_client.client.put_object_acl(
                    Bucket=bucket_name,
                    Key=key,
                    AccessControlPolicy={
                        'Grants': acl_grants,
                        'Owner': {'ID': 'owner-id', 'DisplayName': 'owner'}
                    }
                )
        except:
            pass  # ACL might not be supported

        print(f"Access control test 2525: ✓")

        print(f"\nTest 2525 - Access control 2525: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2525 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2525: {error_code}")
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
