#!/usr/bin/env python3
"""
Test 015: CORS configuration

Tests S3 Cross-Origin Resource Sharing (CORS) configuration including
setting CORS rules, allowed methods, origins, headers, and max age.
"""

import io
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_015(s3_client, config):
    """CORS configuration test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-015')
        s3_client.create_bucket(bucket_name)

        # Test 1: Basic CORS configuration
        basic_cors = {
            'CORSRules': [
                {
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET', 'HEAD'],
                    'AllowedOrigins': ['*'],
                    'MaxAgeSeconds': 3000
                }
            ]
        }

        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=basic_cors
            )

            # Retrieve and verify CORS configuration
            response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
            cors_rules = response.get('CORSRules', [])

            assert len(cors_rules) == 1, f"Expected 1 CORS rule, got {len(cors_rules)}"
            rule = cors_rules[0]
            assert 'GET' in rule['AllowedMethods'], "GET not in allowed methods"
            assert 'HEAD' in rule['AllowedMethods'], "HEAD not in allowed methods"
            assert '*' in rule['AllowedOrigins'], "Wildcard origin not set"
            print("Basic CORS configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: CORS configuration not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Multiple CORS rules with specific origins
        multi_rule_cors = {
            'CORSRules': [
                {
                    'ID': 'Rule1',
                    'AllowedHeaders': ['Authorization', 'Content-Type'],
                    'AllowedMethods': ['GET', 'POST'],
                    'AllowedOrigins': ['https://example.com', 'https://app.example.com'],
                    'ExposeHeaders': ['ETag', 'x-amz-request-id'],
                    'MaxAgeSeconds': 3600
                },
                {
                    'ID': 'Rule2',
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET'],
                    'AllowedOrigins': ['https://public.example.com'],
                    'MaxAgeSeconds': 300
                },
                {
                    'ID': 'Rule3',
                    'AllowedMethods': ['PUT', 'DELETE'],
                    'AllowedOrigins': ['https://admin.example.com'],
                    'AllowedHeaders': ['x-amz-*'],
                    'MaxAgeSeconds': 7200
                }
            ]
        }

        s3_client.client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=multi_rule_cors
        )

        # Verify multiple rules
        response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
        cors_rules = response.get('CORSRules', [])

        assert len(cors_rules) == 3, f"Expected 3 CORS rules, got {len(cors_rules)}"

        # Check rule IDs if present
        rule_ids = [rule.get('ID') for rule in cors_rules if 'ID' in rule]
        if rule_ids:
            assert 'Rule1' in rule_ids, "Rule1 not found"
            assert 'Rule2' in rule_ids, "Rule2 not found"
            assert 'Rule3' in rule_ids, "Rule3 not found"

        print("Multiple CORS rules: ✓")

        # Test 3: CORS with all HTTP methods
        all_methods_cors = {
            'CORSRules': [
                {
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET', 'PUT', 'POST', 'DELETE', 'HEAD'],
                    'AllowedOrigins': ['*'],
                    'MaxAgeSeconds': 3000
                }
            ]
        }

        s3_client.client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=all_methods_cors
        )

        # Verify all methods
        response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
        cors_rules = response.get('CORSRules', [])
        allowed_methods = cors_rules[0].get('AllowedMethods', [])

        expected_methods = ['GET', 'PUT', 'POST', 'DELETE', 'HEAD']
        for method in expected_methods:
            assert method in allowed_methods, f"{method} not in allowed methods"

        print("All HTTP methods CORS: ✓")

        # Test 4: CORS with specific headers
        specific_headers_cors = {
            'CORSRules': [
                {
                    'AllowedHeaders': [
                        'Content-Type',
                        'Content-MD5',
                        'x-amz-acl',
                        'x-amz-meta-*',
                        'Authorization'
                    ],
                    'AllowedMethods': ['GET', 'PUT'],
                    'AllowedOrigins': ['https://secure.example.com'],
                    'ExposeHeaders': [
                        'x-amz-server-side-encryption',
                        'x-amz-request-id',
                        'x-amz-version-id'
                    ],
                    'MaxAgeSeconds': 86400  # 24 hours
                }
            ]
        }

        s3_client.client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=specific_headers_cors
        )

        # Verify specific headers
        response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
        cors_rules = response.get('CORSRules', [])
        rule = cors_rules[0]

        allowed_headers = rule.get('AllowedHeaders', [])
        assert 'Content-Type' in allowed_headers, "Content-Type not in allowed headers"
        assert 'Authorization' in allowed_headers, "Authorization not in allowed headers"

        expose_headers = rule.get('ExposeHeaders', [])
        if expose_headers:  # Some implementations may not support ExposeHeaders
            assert 'x-amz-request-id' in expose_headers, "x-amz-request-id not in expose headers"

        print("Specific headers CORS: ✓")

        # Test 5: CORS with localhost origins (development scenario)
        localhost_cors = {
            'CORSRules': [
                {
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET', 'PUT', 'POST', 'DELETE'],
                    'AllowedOrigins': [
                        'http://localhost:3000',
                        'http://localhost:8080',
                        'http://127.0.0.1:3000',
                        'http://[::1]:3000'  # IPv6 localhost
                    ],
                    'MaxAgeSeconds': 3600
                }
            ]
        }

        s3_client.client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=localhost_cors
        )

        # Verify localhost origins
        response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
        cors_rules = response.get('CORSRules', [])
        allowed_origins = cors_rules[0].get('AllowedOrigins', [])

        assert 'http://localhost:3000' in allowed_origins, "localhost:3000 not in origins"
        assert 'http://localhost:8080' in allowed_origins, "localhost:8080 not in origins"

        print("Localhost origins CORS: ✓")

        # Test 6: Update existing CORS configuration
        updated_cors = {
            'CORSRules': [
                {
                    'AllowedHeaders': ['Content-Type'],
                    'AllowedMethods': ['GET'],
                    'AllowedOrigins': ['https://updated.example.com'],
                    'MaxAgeSeconds': 600
                }
            ]
        }

        s3_client.client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=updated_cors
        )

        # Verify update
        response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
        cors_rules = response.get('CORSRules', [])

        assert len(cors_rules) == 1, "Should have exactly 1 rule after update"
        assert 'https://updated.example.com' in cors_rules[0]['AllowedOrigins'], \
            "Updated origin not found"

        print("CORS update: ✓")

        # Test 7: Invalid CORS configuration (should fail)
        try:
            invalid_cors = {
                'CORSRules': [
                    {
                        'AllowedMethods': ['INVALID_METHOD'],  # Invalid HTTP method
                        'AllowedOrigins': ['*']
                    }
                ]
            }

            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=invalid_cors
            )

            # Some implementations might accept invalid methods
            print("Note: Invalid HTTP method accepted by this implementation")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            assert error_code in ['InvalidRequest', 'MalformedXML', 'InvalidArgument'], \
                f"Unexpected error for invalid CORS: {error_code}"
            print("Invalid CORS rejection: ✓")

        # Test 8: Empty CORS rules (edge case)
        try:
            empty_cors = {
                'CORSRules': []
            }

            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=empty_cors
            )

            # This might be accepted or rejected depending on implementation
            print("Note: Empty CORS rules accepted")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Empty CORS rules rejected: ✓")

        # Test 9: Delete CORS configuration
        s3_client.client.delete_bucket_cors(Bucket=bucket_name)

        # Verify deletion
        try:
            response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
            # If we get here, CORS still exists (shouldn't happen)
            cors_rules = response.get('CORSRules', [])
            assert len(cors_rules) == 0, "CORS rules should be empty after deletion"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            assert error_code in ['NoSuchCORSConfiguration', 'NoSuchBucket'], \
                f"Unexpected error after CORS deletion: {error_code}"
            print("CORS deletion: ✓")

        # Test 10: CORS with wildcard patterns
        wildcard_cors = {
            'CORSRules': [
                {
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET', 'POST'],
                    'AllowedOrigins': [
                        'https://*.example.com',
                        'https://app-*.example.com'
                    ],
                    'MaxAgeSeconds': 3600
                }
            ]
        }

        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=wildcard_cors
            )

            # Verify wildcard patterns
            response = s3_client.client.get_bucket_cors(Bucket=bucket_name)
            cors_rules = response.get('CORSRules', [])
            allowed_origins = cors_rules[0].get('AllowedOrigins', [])

            # Check if wildcards are preserved
            has_wildcards = any('*' in origin and origin != '*' for origin in allowed_origins)
            if has_wildcards:
                print("Wildcard pattern CORS: ✓")
            else:
                print("Note: Wildcard patterns may not be fully supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'MalformedXML']:
                print("Note: Wildcard patterns in origins not supported")

        print(f"\nCORS configuration test completed:")
        print(f"- Basic CORS: ✓")
        print(f"- Multiple rules: ✓")
        print(f"- Various origins: ✓")
        print(f"- Headers configuration: ✓")
        print(f"- CORS management: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("CORS configuration is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to delete CORS configuration first
                try:
                    s3_client.client.delete_bucket_cors(Bucket=bucket_name)
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass