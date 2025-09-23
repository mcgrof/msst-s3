# Docker Setup Guide for MSST-S3

This guide documents how to run S3 compatibility tests using Docker
containers as S3 providers.

## Prerequisites

- Docker installed and running
- Python 3.x with virtual environment
- Make utility

## Quick Start

### 1. Setup Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure for Docker Demo

```bash
make defconfig-docker-demo
```

This creates an S3 configuration file (`s3_config.yaml`) configured for
MinIO running locally.

### 3. Start MinIO Container

```bash
docker run -d \
  --name msst-minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio:latest \
  server /data --console-address ":9001"
```

MinIO will be available at:
- S3 API: http://localhost:9000
- Console: http://localhost:9001

### 4. Populate Test Data

```bash
source .venv/bin/activate
python scripts/populate-data.py --config s3_config.yaml
```

This creates:
- 5 test buckets with various configurations
- 52 test objects including:
  - Binary files of different sizes (1KB to 10MB)
  - Text documents
  - JSON and CSV files
  - Nested directory structures
  - Versioned objects

### 5. Run Tests

```bash
source .venv/bin/activate
make test
```

Test results will be saved to `results/` directory with timestamps.

## Validation Results with MinIO

The test suite achieves **100% pass rate** with MinIO:

```
================================================================================
S3 PRODUCTION VALIDATION SUITE
================================================================================
✓ Critical Data Integrity: 100.0% (3/3 passed)
✓ Error Handling & Recovery: 100.0% (2/2 passed)
✓ Multipart Operations: 100.0% (3/3 passed)
✓ Versioning Support: 100.0% (1/1 passed)
✓ Performance Benchmarks: 100.0% (2/2 passed)

Overall: 100.0% passed (11/11 tests)
================================================================================
✓ PRODUCTION READY - All requirements met
```

### Validation Strategies

| Strategy | Time | Command | Use Case |
|----------|------|---------|----------|
| **Quick Check** | 2-5 min | `make test TEST="001 002 003"` | Smoke test |
| **Critical Tests** | 5-10 min | `python scripts/production-validation.py --quick` | Pre-deployment |
| **Full Validation** | 30 min | `python scripts/production-validation.py` | Production ready |

### Performance Metrics with MinIO

- **Small objects (1KB)**: <50ms latency (p95)
- **Large objects (10MB)**: >10 MB/s throughput
- **Concurrent operations**: 50+ ops/second sustained
- **Multipart uploads**: Handles 20MB files efficiently

## Docker Compose Wrapper

A Python wrapper script (`scripts/docker-compose-wrapper.py`) is provided
to manage Docker containers without requiring docker-compose to be
installed. This is useful for systems where docker-compose is not
available or when using newer Docker versions.

Usage:
```bash
python scripts/docker-compose-wrapper.py up -d    # Start containers
python scripts/docker-compose-wrapper.py down     # Stop containers
python scripts/docker-compose-wrapper.py ps       # List containers
python scripts/docker-compose-wrapper.py logs     # View logs
```

## Available S3 Providers

The `docker-compose.yml` file includes configurations for multiple S3
providers:

1. **MinIO** (Recommended for testing)
   - Port: 9000 (S3 API), 9001 (Console)
   - Most complete S3 compatibility
   - Lightweight and fast

2. **LocalStack**
   - Port: 4566
   - AWS services emulator
   - Good for AWS-specific features

3. **Garage**
   - Port: 3900 (S3 API), 3902 (Admin)
   - Distributed storage system
   - Note: May require additional configuration

4. **SeaweedFS**
   - Port: 8333 (S3 API)
   - Distributed file system
   - Good for large-scale testing

5. **Ceph with RadosGW**
   - Port: 8082
   - Enterprise-grade storage
   - Complex but feature-rich

## Troubleshooting

### Docker Compose Issues

If you encounter "docker-compose: command not found":
- The system uses the wrapper script instead
- Modern Docker includes compose as a subcommand: `docker compose`
- The Makefile has been updated to use the wrapper script

### Test File Issues

Test files must have `.py` extension to be properly imported. If tests
fail with "Cannot load test" errors, ensure test files are named like
`001.py`, not just `001`.

### Container Startup Issues

Some containers may fail to start due to:
- Port conflicts: Check if ports are already in use
- Missing configuration files: Some providers need config files in
  `docker/` directory
- Resource constraints: Some providers need more memory/CPU

### Cleanup

To stop and remove all test containers:
```bash
docker stop msst-minio && docker rm msst-minio
```

Or use the wrapper:
```bash
python scripts/docker-compose-wrapper.py down
```

To remove test buckets and data:
```bash
docker volume prune  # Removes all unused volumes
```

## Configuration

The `s3_config.yaml` file contains all S3 connection settings:

```yaml
s3_endpoint_url: "http://localhost:9000"
s3_access_key: "minioadmin"
s3_secret_key: "minioadmin"
s3_bucket_prefix: "msst-demo"
vendor_type: "minio"
```

Modify these settings to test against different S3 providers or cloud
services.

## Test Results

Test results are saved in `results/` directory with timestamps:
- `results.txt`: Human-readable summary
- `results.json`: Machine-readable detailed results
- Individual test logs if verbose mode is enabled

Example successful test run:
```
Total Tests: 3
Passed: 3 (100.0%)
Failed: 0 (0.0%)

[✓] 001: Create and delete empty bucket
[✓] 002: Upload and download objects
[✓] 003: List objects in bucket
```