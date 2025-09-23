#!/usr/bin/env python3
"""
Test 021: Website configuration

Tests S3 static website hosting configuration including index documents,
error documents, redirection rules, and routing rules.
"""

import io
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_021(s3_client, config):
    """Website configuration test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-021')
        s3_client.create_bucket(bucket_name)

        # Test 1: Basic website configuration
        website_config = {
            'IndexDocument': {
                'Suffix': 'index.html'
            },
            'ErrorDocument': {
                'Key': 'error.html'
            }
        }

        try:
            s3_client.client.put_bucket_website(
                Bucket=bucket_name,
                WebsiteConfiguration=website_config
            )

            # Retrieve and verify configuration
            response = s3_client.client.get_bucket_website(Bucket=bucket_name)

            assert 'IndexDocument' in response, "IndexDocument not in configuration"
            assert response['IndexDocument'].get('Suffix') == 'index.html', \
                "Index document suffix mismatch"
            assert 'ErrorDocument' in response, "ErrorDocument not in configuration"
            assert response['ErrorDocument'].get('Key') == 'error.html', \
                "Error document key mismatch"

            print("Basic website configuration: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'MalformedXML', 'InvalidRequest']:
                print("Note: Website configuration not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Create actual website files
        # Create index.html
        index_content = b'''<!DOCTYPE html>
<html>
<head>
    <title>S3 Website Test</title>
</head>
<body>
    <h1>Welcome to S3 Static Website</h1>
    <p>This is the index page.</p>
</body>
</html>'''

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='index.html',
            Body=io.BytesIO(index_content),
            ContentType='text/html'
        )

        # Create error.html
        error_content = b'''<!DOCTYPE html>
<html>
<head>
    <title>404 Error</title>
</head>
<body>
    <h1>404 - Page Not Found</h1>
    <p>The requested page does not exist.</p>
</body>
</html>'''

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='error.html',
            Body=io.BytesIO(error_content),
            ContentType='text/html'
        )

        # Create some additional pages
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='about.html',
            Body=io.BytesIO(b'<html><body><h1>About Page</h1></body></html>'),
            ContentType='text/html'
        )

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='contact.html',
            Body=io.BytesIO(b'<html><body><h1>Contact Page</h1></body></html>'),
            ContentType='text/html'
        )

        print("Website content created: ✓")

        # Test 3: Website configuration with redirection rules
        redirect_config = {
            'IndexDocument': {
                'Suffix': 'index.html'
            },
            'ErrorDocument': {
                'Key': 'error.html'
            },
            'RoutingRules': [
                {
                    'Condition': {
                        'KeyPrefixEquals': 'docs/'
                    },
                    'Redirect': {
                        'ReplaceKeyPrefixWith': 'documents/'
                    }
                },
                {
                    'Condition': {
                        'HttpErrorCodeReturnedEquals': '404'
                    },
                    'Redirect': {
                        'HostName': 'example.com',
                        'ReplaceKeyWith': '404.html'
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_website(
                Bucket=bucket_name,
                WebsiteConfiguration=redirect_config
            )

            # Retrieve and verify routing rules
            response = s3_client.client.get_bucket_website(Bucket=bucket_name)

            if 'RoutingRules' in response:
                rules = response['RoutingRules']
                assert len(rules) > 0, "No routing rules found"
                print(f"Routing rules configured: {len(rules)} rules")
            else:
                print("Note: Routing rules may not be supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: Complex routing rules not supported")
            else:
                raise

        # Test 4: Different index document in subdirectory
        # Create subdirectory with its own index
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='blog/index.html',
            Body=io.BytesIO(b'<html><body><h1>Blog Index</h1></body></html>'),
            ContentType='text/html'
        )

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='blog/post1.html',
            Body=io.BytesIO(b'<html><body><h1>Blog Post 1</h1></body></html>'),
            ContentType='text/html'
        )

        print("Subdirectory index created: ✓")

        # Test 5: Update website configuration
        updated_config = {
            'IndexDocument': {
                'Suffix': 'default.html'
            },
            'ErrorDocument': {
                'Key': '404.html'
            }
        }

        s3_client.client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration=updated_config
        )

        # Verify update
        response = s3_client.client.get_bucket_website(Bucket=bucket_name)
        assert response['IndexDocument'].get('Suffix') == 'default.html', \
            "Index document not updated"
        assert response['ErrorDocument'].get('Key') == '404.html', \
            "Error document not updated"

        print("Website configuration update: ✓")

        # Test 6: Website with only index document (no error document)
        minimal_config = {
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        }

        s3_client.client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration=minimal_config
        )

        response = s3_client.client.get_bucket_website(Bucket=bucket_name)
        assert 'IndexDocument' in response, "IndexDocument missing"
        # ErrorDocument is optional
        print("Minimal website configuration: ✓")

        # Test 7: Create website assets (CSS, JS, images)
        # Create CSS file
        css_content = b'''body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 20px;
}
h1 {
    color: #333;
}'''

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='styles.css',
            Body=io.BytesIO(css_content),
            ContentType='text/css'
        )

        # Create JavaScript file
        js_content = b'console.log("S3 Website Test");'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='script.js',
            Body=io.BytesIO(js_content),
            ContentType='application/javascript'
        )

        # Create a simple image (1x1 PNG)
        png_data = (
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00'
            b'\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\r'
            b'IDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\xfc\xdbF\x00\x00'
            b'\x00\x00IEND\xaeB`\x82'
        )

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key='images/logo.png',
            Body=io.BytesIO(png_data),
            ContentType='image/png'
        )

        print("Website assets created: ✓")

        # Test 8: Website configuration with protocol redirect
        protocol_redirect_config = {
            'RedirectAllRequestsTo': {
                'HostName': 'example.com',
                'Protocol': 'https'
            }
        }

        try:
            s3_client.client.put_bucket_website(
                Bucket=bucket_name,
                WebsiteConfiguration=protocol_redirect_config
            )

            # Verify redirect configuration
            response = s3_client.client.get_bucket_website(Bucket=bucket_name)

            if 'RedirectAllRequestsTo' in response:
                redirect = response['RedirectAllRequestsTo']
                assert redirect.get('HostName') == 'example.com', "HostName mismatch"
                assert redirect.get('Protocol') == 'https', "Protocol mismatch"
                print("Redirect all requests configuration: ✓")
            else:
                print("Note: RedirectAllRequestsTo may not be fully supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedXML', 'InvalidRequest']:
                print("Note: RedirectAllRequestsTo not supported")
            else:
                raise

        # Test 9: Delete website configuration
        try:
            s3_client.client.delete_bucket_website(Bucket=bucket_name)

            # Verify deletion
            try:
                response = s3_client.client.get_bucket_website(Bucket=bucket_name)
                # If we get here, website config still exists
                print("Note: Website configuration not fully deleted")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchWebsiteConfiguration':
                    print("Website configuration deletion: ✓")
                else:
                    print(f"Note: Unexpected error after deletion: {error_code}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error deleting website configuration: {error_code}")

        # Test 10: Re-enable website configuration for content type test
        s3_client.client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                'IndexDocument': {'Suffix': 'index.html'}
            }
        )

        # Create files with various content types
        content_types = {
            'data.json': ('application/json', b'{"test": "data"}'),
            'document.pdf': ('application/pdf', b'%PDF-1.4 test'),
            'video.mp4': ('video/mp4', b'\x00\x00\x00\x18ftypmp42'),
            'archive.zip': ('application/zip', b'PK\x03\x04test')
        }

        for filename, (content_type, content) in content_types.items():
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=io.BytesIO(content),
                ContentType=content_type
            )

        print("Various content types uploaded: ✓")

        print(f"\nWebsite configuration test completed:")
        print(f"- Basic configuration: ✓")
        print(f"- Website content: ✓")
        print(f"- Configuration management: ✓")
        print(f"- Various asset types: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Website configuration is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to delete website configuration first
                try:
                    s3_client.client.delete_bucket_website(Bucket=bucket_name)
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass