"""Test weave configuration functions"""

import os
import sys
import time

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

import weave

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
def test_config_filesystem(selection, expected):
    """Test selecting FileSystem type from get_file_system"""
    copy_environ = os.environ.copy()
    if "S3_ENDPOINT" not in os.environ:
        os.environ["S3_ENDPOINT"] = "dummy_s3_endpoint"
    fs = weave.config.get_file_system(file_system=selection)
    os.environ = copy_environ
    assert isinstance(fs, expected)


# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_get_mongo_arg_timeout():
    """Verify the timeout works when can't connect to host."""
    env_copy = os.environ.copy()
    try:
        os.environ['MONGODB_HOST'] = "BAD_HOST"
        timeout = 500
        exc_type = pymongo.errors.ServerSelectionTimeoutError
        with pytest.raises(exc_type):
            start = time.time()
            weave.config.get_mongo_db(timeout=timeout)
        end = time.time()

        assert abs(end-start - timeout/1000) < timeout/10
    finally:
        os.environ = env_copy
        


# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_get_mongo_env_timeout():
    """Verify the timeout works when can't connect to host."""
    env_copy = os.environ.copy()
    try:
        timeout = 500
        os.environ['MONGODB_HOST'] = "BAD_HOST"
        os.environ['WEAVE_MONGODB_TIMEOUT'] = str(timeout)
        with pytest.raises(pymongo.errors.ServerSelectionTimeoutError):
            start = time.time()
            weave.config.get_mongo_db()
        end = time.time()

        assert abs(end-start - timeout/1000) < timeout/10
    finally:
        os.environ = env_copy

