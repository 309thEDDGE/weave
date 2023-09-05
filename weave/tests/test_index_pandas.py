"""Pytest tests for the index directory."""
import json
import os
import re
import uuid
import warnings
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave import Basket
from weave.pantry import Pantry
from weave.index.index_pandas import PandasIndex
from weave.tests.pytest_resources import BucketForTest


###############################################################################
#                      Pytest Fixtures Documentation:                         #
#            https://docs.pytest.org/en/7.3.x/how-to/fixtures.html            #
#                                                                             #
#                  https://docs.pytest.org/en/7.3.x/how-to/                   #
#          fixtures.html#teardown-cleanup-aka-fixture-finalization            #
#                                                                             #
#  https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize  #
###############################################################################

# Pylint doesn't like that we are redefining the test fixture here from
# test_basket, but I think this is the right way to do this in case at some
# point in the future we need to differentiate the two.
# pylint: disable=duplicate-code

s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(params=[s3fs, local_fs])
def test_pantry(request, tmpdir):
    """Sets up test bucket for the tests"""
    file_system = request.param
    test_bucket = BucketForTest(tmpdir, file_system)
    yield test_bucket
    test_bucket.cleanup_bucket()


# We need to ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name


def test_sync_index_gets_latest_index(test_pantry):
    """Tests PandasIndex.sync_index by generating two distinct objects and
    making sure that they are both syncing to the index pandas DF (represented
    by JSON) on the file_system"""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    pantry2 = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry2.index.generate_index()

    # assert length of index includes both baskets and excludes the index
    assert len(pantry.index.to_pandas_df()) == 2

    #assert all baskets in index are not index baskets
    for i in range(len(pantry.index.to_pandas_df())):
        basket_type = pantry.index.to_pandas_df()["basket_type"][i]
        assert basket_type != "index"


def test_sync_index_calls_generate_index_if_no_index(test_pantry):
    """Test to make sure that if there isn't a index available then
    generate_index will still be called."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    assert len(pantry.index.to_pandas_df()) == 1


def test_get_index_time_from_path(test_pantry):
    """Tests Index._get_index_time_from_path to ensure it returns the correct
    string."""
    path = "C:/asdf/gsdjls/1234567890-index.json"
    # Obviously we need to test a protected access var here.
    # pylint: disable-next=protected-access
    time = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    ).index._get_index_time_from_path(path=path)
    assert time == 1234567890


def test_clean_up_indices_n_not_int(test_pantry):
    """Tests that PandasIndex.clean_up_indices errors on a str (should be int)
    """
    test_str = "the test"
    with pytest.raises(
        ValueError,
        match=re.escape("invalid literal for int() with base 10: 'the test'"),
    ):
        pantry = Pantry(
            PandasIndex,
            pantry_path=test_pantry.pantry_path,
            file_system=test_pantry.file_system,
            sync=True,
        )
        pantry.index.clean_up_indices(n_keep=test_str)


def test_clean_up_indices_leaves_n_indices(test_pantry):
    """Tests that PandasIndex.clean_up_indices leaves behind the correct number
    of indices."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    pantry.index.generate_index()

    # Now there should be two index baskets. clean up all but one of them:
    pantry.index.clean_up_indices(n_keep=1)
    index_path = os.path.join(test_pantry.pantry_path, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 1


def test_clean_up_indices_with_n_greater_than_num_of_indices(test_pantry):
    """Tests that PandasIndex.clean_up_indices behaves well when given a number
    greater than the total number of indices."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    pantry.index.generate_index()

    # Now there should be two index baskets. clean up all but three of them:
    # (this should fail, obvs)
    pantry.index.clean_up_indices(n_keep=3)
    index_path = os.path.join(test_pantry.pantry_path, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 2


def test_is_index_current(test_pantry):
    """Creates two PandasIndex objects and pits them against eachother in order
    to ensure that PandasIndex.is_index_current is working as expected."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    pantry2 = Pantry(
        PandasIndex,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry2.index.generate_index()
    assert pantry2.index.is_index_current() is True
    assert pantry.index.is_index_current() is False
