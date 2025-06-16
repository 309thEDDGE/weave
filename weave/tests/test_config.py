"""Test weave configuration functions"""

import os
import time
from unittest import mock

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

import weave
from weave.tests.pytest_resources import (get_pymongo_skip_reason,
    get_pymongo_skip_condition)

# Ignore pylint duplicate code. Code here is used to explicitly show pymongo is
# an optional dependency. Duplicate code is found in config.py (where pymongo
# is actually imported)
# pylint: disable-next=duplicate-code
try:
    import pymongo
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True

@pytest.mark.parametrize("selection,expected",[("s3", s3fs.S3FileSystem),
                                         ("local", LocalFileSystem),
                                         ("other", LocalFileSystem)])
@mock.patch.dict(os.environ, os.environ.copy(), clear=True)
def test_config_filesystem(selection, expected):
    """Test selecting FileSystem type from get_file_system"""
    if "S3_ENDPOINT" not in os.environ:
        os.environ["S3_ENDPOINT"] = "dummy_s3_endpoint"
    fs = weave.config.get_file_system(file_system=selection)
    assert isinstance(fs, expected)


# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    get_pymongo_skip_condition(),
    reason=get_pymongo_skip_reason(),
)
@mock.patch.dict(os.environ, os.environ.copy(), clear=True)
def test_get_mongo_arg_timeout():
    """Verify the timeout works when can't connect to host."""
    os.environ['MONGODB_HOST'] = "BAD_HOST"
    timeout = 500
    exc_type = pymongo.errors.ServerSelectionTimeoutError
    with pytest.raises(exc_type):
        start = time.time()
        weave.config.get_mongo_db(timeout=timeout)
    end = time.time()

    assert abs(end-start - timeout/1000) < timeout/10


# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    get_pymongo_skip_condition(),
    reason=get_pymongo_skip_reason(),
)
@mock.patch.dict(os.environ, os.environ.copy(), clear=True)
def test_get_mongo_env_timeout():
    """Verify the timeout works when can't connect to host."""
    timeout = 500
    os.environ['MONGODB_HOST'] = "BAD_HOST"
    os.environ['WEAVE_MONGODB_TIMEOUT'] = str(timeout)
    with pytest.raises(pymongo.errors.ServerSelectionTimeoutError):
        start = time.time()
        weave.config.get_mongo_db()
    end = time.time()

    assert abs(end-start - timeout/1000) < timeout/10
