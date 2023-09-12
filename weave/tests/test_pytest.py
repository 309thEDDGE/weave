"""This script contains tests that test that pytest is deleting baskets
correctly."""

import os

from fsspec.implementations.local import LocalFileSystem
import pytest
import s3fs

from weave.tests.pytest_resources import BucketForTest


s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(params=[s3fs, local_fs])
def set_up_tb_no_cleanup(request, tmpdir):
    """Sets up test basket fixture."""

    file_system = request.param
    temp_basket = BucketForTest(tmpdir, file_system)
    # Purposefully don't clean up bucket, it will be cleaned up in the test.
    return temp_basket

# Ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name
def test_weave_pytest_suffix(set_up_tb_no_cleanup):
    """Test that env var suffix works and buckets are still deleted."""

    # Check bucket name includes suffix if applicable.
    suffix = os.environ.get("WEAVE_PYTEST_SUFFIX", "")
    assert set_up_tb_no_cleanup.pantry_name == f"pytest-temp-bucket{suffix}"

    # Check the bucket was made.
    assert set_up_tb_no_cleanup.file_system.exists(
        set_up_tb_no_cleanup.pantry_name
    )

    # Cleanup the bucket.
    set_up_tb_no_cleanup.cleanup_bucket()

    # Check the bucket is actually deleted.
    assert not set_up_tb_no_cleanup.file_system.exists(
        set_up_tb_no_cleanup.pantry_name
    )
