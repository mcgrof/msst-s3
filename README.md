# MSST-S3: Multi-vendor S3 Storage Test Suite

At the 2025 MSST conference it was brought to my attention there was no generic
S3 interoperability testing framework. I suggested we just get generative AI
to create it.

Why wait?

## Overview

MSST-S3 is a comprehensive interoperability testing framework designed to
validate S3 API compatibility across different storage implementations. Whether
you're developing an S3-compatible storage system, evaluating vendor solutions,
or ensuring consistent behavior across multiple S3 providers, MSST-S3 provides
a standardized test suite to verify compliance and identify implementation
differences.

## Purpose

### Testing Interoperability

MSST-S3 enables you to:

- **Validate S3 Implementations**: Test any S3-compatible storage system against a comprehensive suite of API tests
- **Compare Vendor Solutions**: Run the same tests against multiple S3 providers to identify behavioral differences
- **Ensure API Compliance**: Verify that your S3 implementation correctly handles standard S3 operations
- **Identify Edge Cases**: Discover implementation-specific quirks and limitations across different S3 systems
- **Benchmark Performance**: Compare performance characteristics across different S3 implementations

### Common Use Cases

1. **Storage Vendor Testing**: Validate that your S3-compatible storage product correctly implements the S3 API
2. **Migration Planning**: Test compatibility between source and destination S3 systems before migration
3. **Multi-Cloud Strategy**: Ensure consistent behavior across AWS S3, MinIO, Ceph RGW, and other S3 providers
4. **CI/CD Integration**: Automated testing of S3 compatibility in your development pipeline
5. **Compliance Verification**: Ensure your S3 implementation meets organizational requirements

## Quick Start

### Prerequisites

- Python 3.8 or higher
- Access to one or more S3-compatible endpoints (or use local MinIO)
- S3 credentials (access key and secret key)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/msst-s3.git
cd msst-s3

# Install dependencies
make install-deps
```

### Basic Testing (Fastest Start)

The simplest way to start testing S3 compatibility:

#### Option 1: Automated Docker Demo (Recommended)

```bash
# Full automated demo with MinIO, synthetic data, and tests
make defconfig-docker-demo
make test-with-docker

# This will:
# 1. Start MinIO in Docker
# 2. Populate synthetic test data
# 3. Run compatibility tests
# 4. Generate results report
```

#### Option 2: Manual MinIO Setup

```bash
# Start MinIO manually
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"

# Configure and run basic tests
make defconfig-basic
make test
```

#### Option 3: Test Against AWS S3

```bash
# Load AWS configuration template
make defconfig-aws-s3

# Edit .config to add your AWS credentials
vi .config
# Set CONFIG_S3_ACCESS_KEY and CONFIG_S3_SECRET_KEY

# Run tests
make test
```

### Available Defconfigs

Pre-configured test profiles for common scenarios:

**Basic Configurations:**
- **basic**: Minimal configuration for quick S3 compatibility checks
- **minio-local**: Test against local MinIO instance (localhost:9000)
- **aws-s3**: Full test suite for AWS S3 (requires credentials)

**Docker-based Configurations:**
- **docker-demo**: Automated demo with MinIO and synthetic data
- **docker-localstack**: Test against LocalStack (AWS emulator)
- **docker-ceph**: Test against Ceph RadosGW
- **docker-garage**: Test against Garage S3
- **docker-seaweedfs**: Test against SeaweedFS S3

### Manual Configuration

For custom configurations, use the interactive menu system:

```bash
# Configure your S3 endpoints and test parameters
make menuconfig
```

Navigate through the menu to configure:
- S3 endpoint URLs
- Authentication credentials
- Test categories to run
- Performance test parameters
- Output preferences

### Running Tests

```bash
# Run all enabled tests
make test

# Run specific test categories
make test-basic      # Basic CRUD operations
make test-multipart  # Multipart upload tests
make test-versioning # Object versioning tests
make test-acl        # Access control tests
make test-performance # Performance benchmarks

# Run a specific test
make test TEST=001

# Run tests for a specific group
make test GROUP=acl
```

## Docker S3 Providers

MSST-S3 includes Docker configurations for testing multiple S3 implementations
without manual setup:

### Available S3 Providers

| Provider | Port | Endpoint | Description |
|----------|------|----------|-------------|
| MinIO | 9000 | http://localhost:9000 | High-performance S3-compatible storage |
| LocalStack | 4566 | http://localhost:4566 | AWS services emulator |
| Ceph RadosGW | 8082 | http://localhost:8082 | Ceph's S3 interface |
| Garage | 3900 | http://localhost:3900 | Distributed S3 storage |
| SeaweedFS | 8333 | http://localhost:8333 | Distributed file system with S3 API |

### Docker Commands

```bash
# Start all S3 providers
make docker-up

# Start specific provider
make docker-minio
make docker-localstack
make docker-ceph

# Check status
make docker-status

# View logs
make docker-logs PROVIDER=minio

# Stop all containers
make docker-down
```

### Synthetic Data Population

Generate test data automatically:

```bash
# Populate data after configuring endpoint
make populate-data

# The script creates:
# - Multiple buckets with different configurations
# - Objects of various sizes (1KB to 10MB)
# - Different file types (binary, text, JSON, CSV)
# - Nested directory structures
# - Versioned objects (if supported)
```

### Automated Testing Workflow

```bash
# Complete automated test with Docker provider
make defconfig-docker-demo    # Configure for Docker MinIO
make docker-minio             # Start MinIO container
make populate-data            # Generate test data
make test                     # Run tests

# Or use the all-in-one command:
make defconfig-docker-demo
make test-with-docker
```

## Production Validation

MSST-S3 includes a comprehensive production validation suite to verify S3
systems are ready for production deployment.

### Quick Production Check

Run critical tests only (5-10 minutes):
```bash
python scripts/production-validation.py --config s3_config.yaml --quick
```

### Full Production Validation

Complete production readiness assessment (30-60 minutes):
```bash
python scripts/production-validation.py --config s3_config.yaml
```

### Production Test Categories

| Category | Tests | Coverage | Requirement |
|----------|-------|----------|-------------|
| **Critical - Data Integrity** | 004-006 | 30% | 100% pass |
| **Error Handling** | 011-012 | 20% | 100% pass |
| **Multipart Upload** | 100-102 | 15% | 100% pass |
| **Versioning** | 200 | 5% | 80% pass |
| **Performance** | 600-601 | 10% | 90% pass |

### Production Criteria

The validation script checks:
- ✅ **Data Integrity**: 100% verification with checksums
- ✅ **Latency**: p99 < 1 second for small objects
- ✅ **Throughput**: > 10 MB/s for large objects
- ✅ **Concurrency**: Handle 50+ simultaneous operations
- ✅ **Error Recovery**: Automatic retry with exponential backoff

### Validation Output

```
S3 PRODUCTION VALIDATION SUITE
================================================================================
✓ Critical Data Integrity: 100.0% (3/3 passed)
✓ Error Handling & Recovery: 100.0% (2/2 passed)
✓ Multipart Operations: 100.0% (3/3 passed)
✓ Performance Benchmarks: 100.0% (2/2 passed)

Overall: 100.0% passed
================================================================================
✓ PRODUCTION READY - All requirements met
```

Reports are generated in:
- `validation-report.json` - Machine-readable results
- `validation-report.txt` - Human-readable summary

## Testing Multiple S3 Implementations

### Comparative Testing Workflow

1. **Configure Multiple Endpoints**

   Create configuration profiles for each S3 system:
   ```bash
   # Configure AWS S3
   make menuconfig
   # Save as .config.aws

   # Configure MinIO
   make menuconfig
   # Save as .config.minio

   # Configure Ceph RGW
   make menuconfig
   # Save as .config.ceph
   ```

2. **Run Tests Against Each Implementation**

   ```bash
   # Test AWS S3
   cp .config.aws .config
   make test
   mv results/latest results/aws-s3

   # Test MinIO
   cp .config.minio .config
   make test
   mv results/latest results/minio

   # Test Ceph RGW
   cp .config.ceph .config
   make test
   mv results/latest results/ceph-rgw
   ```

3. **Compare Results**

   The test suite generates detailed reports showing:
   - Pass/fail status for each test
   - Response time comparisons
   - Error messages and incompatibilities
   - Performance metrics

### Automated Multi-Vendor Testing

For automated testing across multiple vendors, use the Ansible integration:

```yaml
# playbooks/inventory/hosts
[s3_vendors]
aws ansible_host=s3.amazonaws.com
minio ansible_host=minio.example.com
ceph ansible_host=ceph.example.com

# Run tests on all vendors
make ansible-run
make ansible-results
```

## Test Categories

### Basic Operations (001-099)
- Bucket creation, listing, deletion
- Object upload, download, deletion
- Metadata operations
- Content-type handling

### Multipart Upload (100-199)
- Large file uploads
- Part management
- Upload abortion and completion
- Concurrent part uploads

### Versioning (200-299)
- Version-enabled buckets
- Object version management
- Version deletion and restoration
- MFA delete protection

### Access Control (300-399)
- Bucket and object ACLs
- Bucket policies
- CORS configuration
- Public access blocking

### Encryption (400-499)
- Server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- Client-side encryption validation
- Encryption in transit
- Key rotation

### Lifecycle Management (500-599)
- Lifecycle rules
- Expiration policies
- Transition rules
- Noncurrent version management

### Performance Tests (600-699)
- Throughput benchmarks
- Latency measurements
- Concurrent operations
- Large-scale operations

### Stress Tests (700-799)
- High concurrency scenarios
- Resource exhaustion tests
- Error recovery
- Rate limiting behavior

## Interpreting Results

### Test Reports

Each test run generates:
- `summary.json`: Overall test results and statistics
- `detailed-results.json`: Individual test outcomes with timings
- `errors.log`: Detailed error messages and stack traces
- `performance-metrics.csv`: Performance data for analysis

### Identifying Incompatibilities

Common incompatibility patterns:
- **Unsupported Features**: Some S3 implementations may not support all features
- **Behavioral Differences**: Different error codes or response formats
- **Performance Variations**: Significant differences in operation latencies
- **Consistency Models**: Eventual vs. strong consistency behaviors

## Best Practices

1. **Baseline Testing**: Always test against AWS S3 as the reference implementation
2. **Isolated Environments**: Use dedicated test buckets to avoid interference
3. **Credential Management**: Store credentials securely, never commit them
4. **Regular Testing**: Run tests regularly to catch regressions
5. **Custom Tests**: Extend the framework with vendor-specific tests when needed

## Extending the Test Suite

### Adding Custom Tests

Create new test files in the appropriate category directory:

```python
# tests/basic/099-custom-test.py
from tests.common.fixtures import s3_client, test_bucket

def test_custom_operation(s3_client, test_bucket):
    """Test vendor-specific S3 operation"""
    # Your test implementation
    pass
```

### Vendor-Specific Configurations

Add vendor-specific settings in the configuration:

```bash
make menuconfig
# Navigate to "Vendor-Specific Settings"
# Configure vendor-specific parameters
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: S3 Compatibility Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: make install-deps

      - name: Configure tests
        run: |
          echo "CONFIG_S3_ENDPOINT_URL=\"${{ secrets.S3_ENDPOINT }}\"" > .config
          echo "CONFIG_S3_ACCESS_KEY=\"${{ secrets.S3_ACCESS_KEY }}\"" >> .config
          echo "CONFIG_S3_SECRET_KEY=\"${{ secrets.S3_SECRET_KEY }}\"" >> .config

      - name: Run tests
        run: make test

      - name: Upload results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: results/
```

## Contributing

### Code Style

Format Python code before committing:

```bash
make style
```

### Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Format code with `make style`
6. Submit a pull request

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/your-org/msst-s3).
