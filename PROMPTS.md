
# ABout S3
S3 is an object storage REST API.

# Goal

Write an S3 interoperability test-suite, to address this I want you to focus
on the following tests suites and implement a vendor neutral solution.

1. s3-tests - The official Ceph S3 compatibility test suite
2. minio/mint - MinIO's testing framework for S3 API compatibility
3. aws-sdk- test suites* - AWS SDK test frameworks
4. boto3 with custom test frameworks
5. s3compat or similar S3-specific testing projects

# Before you proceed

Git clone each git tree under ~/devel/ and do a code analysis of each.
Then *think* hard about this problem.

# Adopt kconfig

Look at ~/devel/kconfig/ for a generic implementation of kconfig.
Then look at ~/devel/init-kconfig/ for an example of how to adopt
kconfig into a new project.

Use the origin/yamlconfig branch to embrace kconfig for this new project
as a git subtree. You can see how ~/devel/kdevops/Makefile.subtrees does
this.

# Adopt ansible and Makefiles 

Learn to adopt Makefile targets for ansible targets as we do in kdevops,
you can use the kdevops/workflows/demos/reboot-limit/ as a simple demo
of how to do this. Look also at ~/devel/kdevops/playbooks/roles/reboot-limit/
for an example role and ~/devel/kdevops/playbooks/reboot-limit.yml.

# Use Python

Use Python for the test suite.

# Itemize tests

Help come up with itemized tests as itemized in spirit with the Linux
filesystem tests ~/devel/xfstests-dev. You can look for a simpler
example on ~/devel/blktests.
