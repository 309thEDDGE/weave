import os

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave.tests.pytest_resources import BucketForTest

"""Pytest Fixtures Documentation:
https://docs.pytest.org/en/7.3.x/how-to/fixtures.html

https://docs.pytest.org/en/7.3.x/how-to
/fixtures.html#teardown-cleanup-aka-fixture-finalization

https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize
"""

s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()

# Test with two different fsspec file systems (above).
@pytest.fixture(params=[s3fs, local_fs])
def set_up_tb_no_cleanup(request, tmpdir):
    fs = request.param
    tb = BucketForTest(tmpdir, fs)
    yield tb
    # Purposefully don't cleanup bucket here, we will clean up in the test.

def test_weave_pytest_suffix(set_up_tb_no_cleanup):
    """Test that env var suffix works, and buckets are still deleted."""
    tb = set_up_tb_no_cleanup

    # Check bucket name includes suffix if applicable.
    suffix = os.environ.get('WEAVE_PYTEST_SUFFIX', '')
    assert tb.bucket_name == f"pytest-temp-bucket{suffix}"

    # Check the bucket was made.
    assert tb.fs.exists(tb.bucket_name)

    # Cleanup the bucket.
    tb.cleanup_bucket()

    # Check the bucket is actually deleted
    assert not tb.fs.exists(tb.bucket_name)