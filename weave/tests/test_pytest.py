"""This script contains tests that test that pytest is deleting baskets
correctly."""

import os

from fsspec.implementations.local import LocalFileSystem
import pytest
import s3fs

from weave.tests.pytest_resources import PantryForTest


s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(
    params=[s3fs, local_fs],
    ids=["S3FileSystem", "LocalFileSystem"],
)
def set_up_tb_no_cleanup(request, tmpdir):
    """Sets up test basket fixture."""

    file_system = request.param
    temp_basket = PantryForTest(tmpdir, file_system)
    # Purposefully don't clean up pantry, it will be cleaned up in the test.
    return temp_basket

# Ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name
def test_weave_pytest_suffix(set_up_tb_no_cleanup):
    """Test that env var suffix works, and pantrys are still deleted."""

    # Check pantry name includes suffix if applicable.
    suffix = os.environ.get('WEAVE_PYTEST_SUFFIX', '')
    assert set_up_tb_no_cleanup.pantry_path == f"pytest-temp-pantry{suffix}"

    # Check the pantry was made.
    assert set_up_tb_no_cleanup.file_system.exists(
        set_up_tb_no_cleanup.pantry_path
    )

    # Cleanup the pantry.
    set_up_tb_no_cleanup.cleanup_pantry()

    # Check the pantry is actually deleted.
    assert not set_up_tb_no_cleanup.file_system.exists(
        set_up_tb_no_cleanup.pantry_path
    )
