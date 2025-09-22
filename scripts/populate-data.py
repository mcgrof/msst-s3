#!/usr/bin/env python3
"""
Synthetic data population script for S3 testing.
Generates test buckets and objects with various sizes and types.
"""

import os
import sys
import random
import string
import hashlib
import argparse
import yaml
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import json
import io


class S3DataPopulator:
    """Populates S3 with synthetic test data."""

    def __init__(self, config_file):
        """Initialize with configuration from YAML file."""
        with open(config_file, "r") as f:
            self.config = yaml.safe_load(f)

        # S3 client setup
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.config.get("s3_endpoint_url"),
            aws_access_key_id=self.config.get("s3_access_key"),
            aws_secret_access_key=self.config.get("s3_secret_key"),
            region_name=self.config.get("s3_region", "us-east-1"),
            use_ssl=self.config.get("s3_use_ssl", False),
        )

        # Data generation parameters
        self.bucket_prefix = self.config.get("s3_bucket_prefix", "msst-test")
        self.num_buckets = 3
        self.objects_per_bucket = 10
        self.file_sizes = {
            "tiny": 1024,  # 1KB
            "small": 1024 * 100,  # 100KB
            "medium": 1024 * 1024,  # 1MB
            "large": 1024 * 1024 * 10,  # 10MB
        }

    def generate_random_data(self, size):
        """Generate random binary data of specified size."""
        return os.urandom(size)

    def generate_text_data(self, size):
        """Generate random text data of specified size."""
        chars = string.ascii_letters + string.digits + string.punctuation + " \n"
        return "".join(random.choices(chars, k=size)).encode("utf-8")

    def generate_json_data(self):
        """Generate sample JSON data."""
        data = {
            "timestamp": datetime.now().isoformat(),
            "test_id": "".join(random.choices(string.ascii_lowercase, k=8)),
            "metadata": {
                "version": "1.0",
                "test_framework": "MSST-S3",
                "synthetic": True,
            },
            "values": [random.random() for _ in range(10)],
            "description": "Synthetic test data for S3 compatibility testing",
        }
        return json.dumps(data, indent=2).encode("utf-8")

    def generate_csv_data(self):
        """Generate sample CSV data."""
        lines = ["id,name,value,timestamp\n"]
        for i in range(100):
            lines.append(
                f"{i},item_{i},{random.random():.4f},{datetime.now().isoformat()}\n"
            )
        return "".join(lines).encode("utf-8")

    def create_bucket(self, bucket_name):
        """Create a bucket if it doesn't exist."""
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            print(f"  ✓ Created bucket: {bucket_name}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                print(f"  → Bucket already exists: {bucket_name}")
                return True
            else:
                print(f"  ✗ Error creating bucket {bucket_name}: {e}")
                return False

    def upload_object(self, bucket_name, key, data, metadata=None):
        """Upload an object to S3."""
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata

            # Calculate MD5 for verification
            md5 = hashlib.md5(data).hexdigest()
            extra_args["Metadata"] = extra_args.get("Metadata", {})
            extra_args["Metadata"]["md5"] = md5

            self.s3_client.put_object(
                Bucket=bucket_name, Key=key, Body=io.BytesIO(data), **extra_args
            )
            return True
        except ClientError as e:
            print(f"    ✗ Error uploading {key}: {e}")
            return False

    def populate_bucket(self, bucket_name):
        """Populate a bucket with various test objects."""
        print(f"\n  Populating bucket: {bucket_name}")
        objects_created = 0

        # Create objects of different sizes
        for size_name, size_bytes in self.file_sizes.items():
            key = f"binary/{size_name}/test-{size_name}.bin"
            data = self.generate_random_data(size_bytes)
            if self.upload_object(
                bucket_name,
                key,
                data,
                {"type": "binary", "size_category": size_name},
            ):
                objects_created += 1
                print(f"    ✓ Created {key} ({size_bytes} bytes)")

        # Create text files
        for i in range(3):
            key = f"text/document-{i}.txt"
            data = self.generate_text_data(random.randint(1000, 10000))
            if self.upload_object(
                bucket_name, key, data, {"type": "text", "index": str(i)}
            ):
                objects_created += 1
                print(f"    ✓ Created {key}")

        # Create JSON files
        for i in range(2):
            key = f"json/data-{i}.json"
            data = self.generate_json_data()
            if self.upload_object(
                bucket_name,
                key,
                data,
                {"type": "json", "content-type": "application/json"},
            ):
                objects_created += 1
                print(f"    ✓ Created {key}")

        # Create CSV file
        key = "csv/data.csv"
        data = self.generate_csv_data()
        if self.upload_object(
            bucket_name, key, data, {"type": "csv", "content-type": "text/csv"}
        ):
            objects_created += 1
            print(f"    ✓ Created {key}")

        # Create nested directory structure
        for dir_level in range(3):
            for file_num in range(2):
                path_parts = [f"level{i}" for i in range(dir_level + 1)]
                path_parts.append(f"file{file_num}.dat")
                key = "/".join(path_parts)
                data = self.generate_random_data(1024)
                if self.upload_object(
                    bucket_name,
                    key,
                    data,
                    {"type": "nested", "depth": str(dir_level)},
                ):
                    objects_created += 1
                    print(f"    ✓ Created {key}")

        print(f"  → Created {objects_created} objects in {bucket_name}")
        return objects_created

    def populate(self):
        """Main population routine."""
        print("\n" + "=" * 60)
        print("S3 Synthetic Data Population")
        print("=" * 60)
        print(f"Endpoint: {self.config.get('s3_endpoint_url')}")
        print(f"Bucket prefix: {self.bucket_prefix}")
        print(f"Number of buckets: {self.num_buckets}")
        print(f"Objects per bucket: ~{self.objects_per_bucket}")

        total_buckets = 0
        total_objects = 0

        # Create and populate buckets
        for i in range(self.num_buckets):
            bucket_name = f"{self.bucket_prefix}-{i:02d}"

            if self.create_bucket(bucket_name):
                total_buckets += 1
                objects = self.populate_bucket(bucket_name)
                total_objects += objects

        # Create a versioned bucket
        versioned_bucket = f"{self.bucket_prefix}-versioned"
        if self.create_bucket(versioned_bucket):
            total_buckets += 1
            try:
                # Enable versioning
                self.s3_client.put_bucket_versioning(
                    Bucket=versioned_bucket,
                    VersioningConfiguration={"Status": "Enabled"},
                )
                print(f"  ✓ Enabled versioning on {versioned_bucket}")

                # Upload multiple versions of the same object
                for version in range(3):
                    key = "versioned-object.txt"
                    data = f"Version {version + 1} content\n".encode("utf-8")
                    if self.upload_object(
                        versioned_bucket,
                        key,
                        data,
                        {"version": str(version + 1)},
                    ):
                        total_objects += 1
                        print(f"    ✓ Created version {version + 1} of {key}")
            except ClientError as e:
                print(f"  ⚠ Versioning not supported: {e}")

        # Create a public bucket (if supported)
        public_bucket = f"{self.bucket_prefix}-public"
        if self.create_bucket(public_bucket):
            total_buckets += 1
            try:
                # Try to set public read policy
                policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "PublicRead",
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": ["s3:GetObject"],
                            "Resource": f"arn:aws:s3:::{public_bucket}/*",
                        }
                    ],
                }
                self.s3_client.put_bucket_policy(
                    Bucket=public_bucket, Policy=json.dumps(policy)
                )
                print(f"  ✓ Set public read policy on {public_bucket}")
            except ClientError as e:
                print(f"  ⚠ Public bucket policy not supported: {e}")

            # Add a public object
            key = "public-file.txt"
            data = b"This is a public file for testing."
            if self.upload_object(public_bucket, key, data):
                total_objects += 1
                print(f"    ✓ Created public object: {key}")

        print("\n" + "=" * 60)
        print(f"Population Summary:")
        print(f"  Total buckets created: {total_buckets}")
        print(f"  Total objects created: {total_objects}")
        print("=" * 60 + "\n")

        return total_buckets, total_objects


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Populate S3 with synthetic test data"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to S3 configuration YAML file",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean existing test buckets before populating",
    )

    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Error: Configuration file not found: {args.config}")
        sys.exit(1)

    try:
        populator = S3DataPopulator(args.config)

        if args.clean:
            print("Cleaning existing test buckets...")
            # TODO: Implement cleanup logic

        buckets, objects = populator.populate()

        if buckets == 0:
            print("Warning: No buckets were created")
            sys.exit(1)

        print("✓ Data population completed successfully")
        sys.exit(0)

    except Exception as e:
        print(f"✗ Error during population: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()