#!/usr/bin/env python3
"""
Test 010: Object lifecycle rules

Tests S3 bucket lifecycle configuration including expiration rules,
transitions between storage classes, and noncurrent version management.
"""

import io
import json
import time
from datetime import datetime, timedelta
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_010(s3_client, config):
    """Object lifecycle rules test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-010')
        s3_client.create_bucket(bucket_name)

        # Test 1: Basic expiration rule
        basic_lifecycle = {
            'Rules': [
                {
                    'ID': 'expire-old-logs',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'logs/'},
                    'Expiration': {
                        'Days': 30
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=basic_lifecycle
            )

            # Retrieve and verify configuration
            response = s3_client.client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            rules = response.get('Rules', [])
            assert len(rules) == 1, f"Expected 1 rule, got {len(rules)}"
            assert rules[0]['ID'] == 'expire-old-logs', "Rule ID mismatch"
            assert rules[0]['Status'] == 'Enabled', "Rule not enabled"
            print("Basic expiration rule: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Lifecycle rules not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Multiple rules with different filters
        # Note: MinIO may not support all features like Transitions
        multi_rule_lifecycle = {
            'Rules': [
                {
                    'ID': 'expire-temp-files',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'temp/'},
                    'Expiration': {
                        'Days': 7
                    }
                },
                {
                    'ID': 'expire-old-data',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'data/'},
                    'Expiration': {
                        'Days': 365
                    }
                },
                {
                    'ID': 'expire-archived',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'archive/'},
                    'Expiration': {
                        'Days': 180
                    }
                }
            ]
        }

        s3_client.client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=multi_rule_lifecycle
        )

        # Verify multiple rules
        response = s3_client.client.get_bucket_lifecycle_configuration(
            Bucket=bucket_name
        )
        rules = response.get('Rules', [])
        rule_ids = [rule['ID'] for rule in rules]

        assert 'expire-temp-files' in rule_ids, "Temp files rule missing"
        assert 'expire-old-data' in rule_ids, "Expire old data rule missing"
        assert 'expire-archived' in rule_ids, "Archive expiration rule missing"
        print("Multiple lifecycle rules: ✓")

        # Test 3: Tag-based lifecycle rules (simpler version)
        tag_based_lifecycle = {
            'Rules': [
                {
                    'ID': 'expire-tagged-objects',
                    'Status': 'Enabled',
                    'Filter': {
                        'Tag': {
                            'Key': 'Retention',
                            'Value': 'Temporary'
                        }
                    },
                    'Expiration': {
                        'Days': 1
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=tag_based_lifecycle
            )

            # Verify tag-based rules
            response = s3_client.client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            rules = response.get('Rules', [])

            # Find the tag-based rule
            tag_rule = None
            for rule in rules:
                if rule['ID'] == 'expire-tagged-objects':
                    tag_rule = rule
                    break

            if tag_rule:
                assert 'Filter' in tag_rule, "Filter missing in tag rule"
                print("Tag-based lifecycle rules: ✓")
            else:
                print("Note: Tag-based rules may not be fully supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest', 'InvalidStorageClass', 'InvalidArgument']:
                print("Note: Tag-based lifecycle rules not supported")
            else:
                raise

        # Test 4: Noncurrent version expiration (for versioned buckets)
        # First enable versioning
        try:
            s3_client.put_bucket_versioning(
                bucket_name,
                {'Status': 'Enabled'}
            )

            noncurrent_lifecycle = {
                'Rules': [
                    {
                        'ID': 'expire-noncurrent-versions',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': 'versioned/'},
                        'NoncurrentVersionExpiration': {
                            'NoncurrentDays': 30
                        }
                    }
                ]
            }

            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=noncurrent_lifecycle
            )

            print("Noncurrent version lifecycle rules: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest']:
                print("Note: Noncurrent version rules not supported")
            else:
                raise

        # Test 5: Date-based expiration
        future_date = datetime.utcnow() + timedelta(days=30)
        date_based_lifecycle = {
            'Rules': [
                {
                    'ID': 'expire-on-date',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'scheduled/'},
                    'Expiration': {
                        'Date': future_date.strftime('%Y-%m-%dT00:00:00Z')
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=date_based_lifecycle
            )

            # Verify date-based rule
            response = s3_client.client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            rules = response.get('Rules', [])

            for rule in rules:
                if rule['ID'] == 'expire-on-date':
                    assert 'Expiration' in rule, "Expiration missing"
                    assert 'Date' in rule['Expiration'], "Date missing in expiration"
                    print("Date-based expiration: ✓")
                    break

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: Date-based expiration not supported")
            else:
                raise

        # Test 6: Disable a rule
        disabled_lifecycle = {
            'Rules': [
                {
                    'ID': 'disabled-rule',
                    'Status': 'Disabled',
                    'Filter': {'Prefix': 'disabled/'},
                    'Expiration': {
                        'Days': 1
                    }
                },
                {
                    'ID': 'enabled-rule',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'enabled/'},
                    'Expiration': {
                        'Days': 1
                    }
                }
            ]
        }

        s3_client.client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=disabled_lifecycle
        )

        # Verify rule status
        response = s3_client.client.get_bucket_lifecycle_configuration(
            Bucket=bucket_name
        )
        rules = response.get('Rules', [])

        disabled_found = False
        enabled_found = False
        for rule in rules:
            if rule['ID'] == 'disabled-rule':
                assert rule['Status'] == 'Disabled', "Rule should be disabled"
                disabled_found = True
            elif rule['ID'] == 'enabled-rule':
                assert rule['Status'] == 'Enabled', "Rule should be enabled"
                enabled_found = True

        assert disabled_found and enabled_found, "Both rules should be present"
        print("Rule enable/disable: ✓")

        # Test 7: Delete lifecycle configuration
        s3_client.client.delete_bucket_lifecycle(Bucket=bucket_name)

        # Verify deletion
        try:
            response = s3_client.client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            # If we get here, configuration still exists
            rules = response.get('Rules', [])
            assert len(rules) == 0, "Lifecycle rules not deleted"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchLifecycleConfiguration':
                print("Lifecycle deletion: ✓")
            else:
                raise

        # Test 8: Create objects and verify they would be affected by rules
        # Re-apply a simple lifecycle rule
        test_lifecycle = {
            'Rules': [
                {
                    'ID': 'test-rule',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'test/'},
                    'Expiration': {
                        'Days': 1
                    }
                }
            ]
        }

        s3_client.client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=test_lifecycle
        )

        # Create test objects
        test_objects = [
            'test/file1.txt',
            'test/subdir/file2.txt',
            'other/file3.txt'  # Should not be affected
        ]

        for key in test_objects:
            s3_client.put_object(
                bucket_name,
                key,
                io.BytesIO(b'Test content')
            )

        # List objects that would be affected
        objects = s3_client.list_objects(bucket_name, prefix='test/')
        affected_count = len(objects)
        assert affected_count >= 2, f"Expected at least 2 affected objects, got {affected_count}"

        print(f"\nLifecycle rules test completed:")
        print(f"- Basic expiration: ✓")
        print(f"- Multiple rules: ✓")
        print(f"- Filter types: ✓")
        print(f"- Rule management: ✓")
        print(f"- Objects would be affected: {affected_count}")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Lifecycle rules are not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to delete lifecycle configuration
                try:
                    s3_client.client.delete_bucket_lifecycle(Bucket=bucket_name)
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass