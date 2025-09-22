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
- Access to one or more S3-compatible endpoints
- S3 credentials (access key and secret key)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/msst-s3.git
cd msst-s3

# Install dependencies
make install-deps
```

### Configuration

MSST-S3 uses an interactive menu-based configuration system:

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
