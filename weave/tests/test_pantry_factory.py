"""Pytest tests for the pantry factory."""
import json
import os
import re
import shutil
import tempfile
import uuid as uuid_lib
import warnings
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import s3fs
import fsspec
from fsspec.implementations.local import LocalFileSystem

from weave import Basket
from weave.pantry_factory import create_pantry
from weave.index.create_index import create_index_from_fs
from weave.index.index_pandas import IndexPandas
from weave.pantry import Pantry
from weave.tests.pytest_resources import PantryForTest, get_file_systems
from weave.__init__ import __version__ as weave_version


###############################################################################
#                      Pytest Fixtures Documentation:                         #
#            https://docs.pytest.org/en/7.3.x/how-to/fixtures.html            #
#                                                                             #
#                  https://docs.pytest.org/en/7.3.x/how-to/                   #
#          fixtures.html#teardown-cleanup-aka-fixture-finalization            #
#                                                                             #
#  https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize  #
###############################################################################

# This module is long and has many tests. Pylint is complaining that it is too
# long. This isn't neccesarily a bad thing, as the alternative would be to
# write the tests continuing in a different script, which is unneccesarily
# complex. Disabling this warning for this script.
# pylint: disable=too-many-lines

# Pylint doesn't like redefining the test fixture here from
# test_basket, but this is the right way to do this in case at some
# point in the future there is a need to differentiate the two.
# pylint: disable=duplicate-code

# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    name="test_pantry",
    params=file_systems,
    ids=file_systems_ids,
)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests."""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


def test_pantry_factory_default_args(test_pantry):
    """"""
    pantry = create_pantry(index=IndexPandas,
                           pantry_path=test_pantry.pantry_path,
                           file_system=test_pantry.file_system)

    print('pantry name: ', pantry.pantry_path)
    print('index: ', pantry.index.to_pandas_df())
    # config_path = os.path.join(test_pantry.pantry_path)
    # test_pantry.pantry_path


def test_pantry_factory_local_config(test_pantry):
    """"""
    if type(test_pantry.file_system) is LocalFileSystem:
        file_system = "LocalFileSystem"
    elif type(test_pantry.file_system) is s3fs.S3FileSystem:
        file_system = "S3FileSystem"

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "config.json")

        with open(config_path, "w", encoding="utf-8") as config_file:
            json.dump({"index":"IndexPandas",
                       "pantry_path":test_pantry.pantry_path,
                       "file_system":file_system,
# I feel like the s3 endpoint shouldn't be hard coded in like this, any suggestions?
                       "S3_ENDPOINT":os.environ["S3_ENDPOINT"]},
                      config_file)
        pantry = create_pantry(config_file=config_path)


    print('pantry name: ', pantry.pantry_path)
    print('index: ', pantry.index.to_pandas_df())

def test_pantry_factory_existing_pantry_config(test_pantry):
    """"""
    if type(test_pantry.file_system) is LocalFileSystem:
        file_system = "LocalFileSystem"
    elif type(test_pantry.file_system) is s3fs.S3FileSystem:
        file_system = "S3FileSystem"
    print(test_pantry.file_system.find(test_pantry.pantry_path))
    config_path = os.path.join(test_pantry.pantry_path, "config.json")

    with test_pantry.file_system.open(config_path, "w", encoding="utf-8") as config_file:
            json.dump({"index":"IndexPandas",
                       "pantry_path":test_pantry.pantry_path,
                       "file_system":file_system,
# I feel like the s3 endpoint shouldn't be hard coded in like this, any suggestions?
                       "S3_ENDPOINT":os.environ["S3_ENDPOINT"]},
                      config_file)
    print(test_pantry.file_system.cat(config_path))

    pantry = create_pantry(pantry_path=test_pantry.pantry_path,
                           file_system=test_pantry.file_system)

    print('pantry name: ', pantry.pantry_path)
    print('index: ', pantry.index.to_pandas_df())





