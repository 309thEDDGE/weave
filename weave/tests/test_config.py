"""Test weave configuration functions"""

import os
import sys
import time

import pymongo
import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

import weave


@pytest.mark.parametrize("selection,expected",[("s3", s3fs.S3FileSystem),
                                         ("local", LocalFileSystem),
                                         ("other", LocalFileSystem)])
def test_config_filesystem(selection, expected):
    """Test selecting FileSystem type from get_file_system"""
    fs = weave.config.get_file_system(file_system=selection) 
    assert type(fs) == expected


# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_get_mongo_arg_timeout():
    """Verify the timeout works when can't connect to host."""
    os.environ['MONGODB_HOST'] = "BAD_HOST"
    timeout = 500
    with pytest.raises(pymongo.errors.ServerSelectionTimeoutError):
        start = time.time()
        weave.config.get_mongo_db(timeout=timeout)
    end = time.time()

    assert abs(end-start - timeout/1000) < timeout/10


# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
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
