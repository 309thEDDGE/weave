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

import weave
from weave import Basket
from weave.index.index import Index
from weave import IndexSQLite
from weave.index.create_index import create_index_from_fs
from weave.tests.pytest_resources import BucketForTest, IndexForTest


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
# long. I don't necessarily think that is bad in this case, as the alternative
# would be to write the tests continuing in a different script, which I think
# is unnecesarily complex. Therefor, I am disabling this warning for this
# script.
# pylint: disable=too-many-lines

# Pylint doesn't like that we are redefining the test fixture here from
# test_basket, but I think this is the right way to do this in case at some
# point in the future we need to differentiate the two.
# pylint: disable=duplicate-code

# Create fsspec objects to be tested, and add to file_systems list.
s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()
file_systems = [s3fs, local_fs]

# Create Index CONSTRUCTORS of Indexes to be tested, and add to indexes list.
sqlite_index = IndexSQLite
indexes = [sqlite_index]

# Create combinations of the above parameters to pass into the fixture..
params = []
for file_system in file_systems:
    for index in indexes:
        params.append((file_system, index))


@pytest.fixture(params=params)
def test_pantry(request, tmpdir):
    """Sets up test bucket for the tests"""
    file_system = request.param[0]
    pantry_path = (
        "pytest-temp-bucket" f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    test_bucket = BucketForTest(tmpdir, file_system, pantry_path=pantry_path)

    index_constructor = request.param[1]
    test_index = IndexForTest(
        index_constructor=index_constructor,
        file_system=file_system,
        pantry_path=pantry_path
    )
    index = test_index.index

    yield test_bucket, index
    test_bucket.cleanup_bucket()
    test_index.cleanup_index()


@pytest.fixture(params=indexes)
def test_index_only(request):
    """Sets up only the index for a test (USES DEFAULT FILE_SYSTEM ARGS).

    Use this fixture for tests that DO NOT manipulate the pantry (ie
    type checking tests, etc.)
    """
    index_constructor = request.param
    file_system = weave.config.get_file_system()
    pantry_path = (
        "pytest-temp-bucket" f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    test_index = IndexForTest(
        index_constructor=index_constructor,
        file_system=file_system,
        pantry_path=pantry_path
    )
    index = test_index.index

    yield index
    test_index.cleanup_index()


# We need to ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name
def test_index_abc_get_metadata_returns_dict(test_index_only):
    """Test IndexABC get_metadata returns a python dictionary."""
    ind = test_index_only

    metadata = ind.get_metadata()
    assert isinstance(metadata, dict), "Index.get_metadata must return a dict."


def test_index_abc_generate_index_works(test_pantry):
    """Tests IndexABC generate_index uses the pantry fs to add to the index."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    assert len(ind_df) == 0, "Incorrect number of elements in dataframe"

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    assert len(ind_df) == 1, "Incorrect number of elements in dataframe"

    # Add another basket to the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")

    # Regenerate the index
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    assert len(ind_df) == 2, "Incorrect number of elements in dataframe"


def test_index_abc_to_pandas_df_works(test_pantry):
    """Tests IndexABC to_pandas_df returns dataframe with proper values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    uid, basket_type, label = "0001", "test_basket", "test_label"
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uid,
        basket_type=basket_type,
        label=label
    )

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    # Check we get a pandas dataframe of the correct length.
    assert len(ind_df) == 1 and isinstance(ind_df, pd.DataFrame)

    # Check df columns are correctly named.
    assert (
        list(ind_df.columns) == ["uuid", "upload_time", "parent_uuids",
                                "basket_type", "label", "address",
                                "storage_type"]
    ), "Dataframe columns do not match"

    # Check values of basket are accurate
    assert (
        ind_df.iloc[0]["uuid"] == uid and
        ind_df.iloc[0]["parent_uuids"] == "[]" and
        ind_df.iloc[0]["basket_type"] == basket_type and
        ind_df.iloc[0]["label"] == label and
        ind_df.iloc[0]["address"].endswith(up_dir) and
        ind_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match."

    # Upload another basket
    uid, basket_type, label = "0002", "test_basket", "test_label"
    parent_ids = ["0001"]
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    up_dir = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uid,
        basket_type=basket_type,
        parent_ids=parent_ids,
        label=label
    )

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    # Check we get a pandas dataframe of the correct length.
    assert len(ind_df) == 2 and isinstance(ind_df, pd.DataFrame)

    # Check df columns are correctly named.
    assert (
        list(ind_df.columns) == ["uuid", "upload_time", "parent_uuids",
                                "basket_type", "label", "address",
                                "storage_type"]
    ), "Dataframe columns do not match"

    # Check values of basket are accurate
    assert (
        ind_df.iloc[1]["uuid"] == uid and
        ind_df.iloc[1]["parent_uuids"] == f"{parent_ids}" and
        ind_df.iloc[1]["basket_type"] == basket_type and
        ind_df.iloc[1]["label"] == label and
        ind_df.iloc[1]["address"].endswith(up_dir) and
        ind_df.iloc[1]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match."


def test_index_abc_track_basket_adds_single_basket(test_pantry):
    """Test IndexABC track_basket works when passing a single basket df."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    assert len(ind_df) == 0, "Incorrect number of elements in dataframe"

    # Put basket in the temporary bucket
    uid, basket_type, label = "0001", "test_basket", "test_label"
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uid,
        basket_type=basket_type,
        label=label
    )

    single_indice_index = create_index_from_fs(
        up_dir,
        test_pantry.file_system
    )

    ind.track_basket(single_indice_index)
    ind_df = ind.to_pandas_df()

    assert len(ind_df) == 1, "Incorrect number of elements in dataframe"

    # Check values of basket are accurate
    assert (
        ind_df.iloc[0]["uuid"] == uid and
        ind_df.iloc[0]["parent_uuids"] == "[]" and
        ind_df.iloc[0]["basket_type"] == basket_type and
        ind_df.iloc[0]["label"] == label and
        ind_df.iloc[0]["address"].endswith(up_dir) and
        ind_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match."


def test_index_abc_track_basket_adds_multiple_baskets(test_pantry):
    """Test IndexABC track_basket works when passing a multi-basket df."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Generate the index.
    ind.generate_index()

    # Put basket in the temporary bucket
    uid1, basket_type1, label1 = "0001", "test_basket", "test_label"
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir1 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uid1,
        basket_type=basket_type1,
        label=label1
    )

    first_slice_df = create_index_from_fs(up_dir1, test_pantry.file_system)
    first_slice_df["parent_uuids"] = first_slice_df["parent_uuids"].astype(str)

    # Upload another basket
    uid2, basket_type2, label2 = "0002", "test_basket", "test_label"
    parent_ids2 = ["0001"]
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    up_dir2 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two,
        uid=uid2,
        basket_type=basket_type2,
        parent_ids=parent_ids2,
        label=label2
    )

    second_slice_df = create_index_from_fs(up_dir2, test_pantry.file_system)
    second_slice_df["parent_uuids"] = (
        second_slice_df["parent_uuids"].astype(str)
    )

    dual_slice_df = pd.concat([first_slice_df, second_slice_df])
    assert len(dual_slice_df) == 2, "Invalid dual_indice_index values."

    ind.track_basket(dual_slice_df)
    ind_df = ind.to_pandas_df()

    # Check we get a pandas dataframe of the correct length.
    assert (
        len(ind_df) == 2 and isinstance(ind_df, pd.DataFrame)
    ), "track_basket failed to add multiple items."

    # Check values of basket are accurate
    assert (
        ind_df.iloc[0]["uuid"] == uid1 and
        ind_df.iloc[0]["parent_uuids"] == "[]" and
        ind_df.iloc[0]["basket_type"] == basket_type1 and
        ind_df.iloc[0]["label"] == label1 and
        ind_df.iloc[0]["address"].endswith(up_dir1) and
        ind_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match first record."

    assert (
        ind_df.iloc[1]["uuid"] == uid2 and
        ind_df.iloc[1]["parent_uuids"] == f"{parent_ids2}" and
        ind_df.iloc[1]["basket_type"] == basket_type2 and
        ind_df.iloc[1]["label"] == label2 and
        ind_df.iloc[1]["address"].endswith(up_dir2) and
        ind_df.iloc[1]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match second record."

    # Check df columns are correctly named.
    assert (
        list(ind_df.columns) == ["uuid", "upload_time", "parent_uuids",
                                "basket_type", "label", "address",
                                "storage_type"]
    ), "Dataframe columns do not match"


def test_index_abc_untrack_basket_removes_single_basket(test_pantry):
    """Test IndexABC untrack_basket works when passing a basket address."""
    raise NotImplementedError


def test_index_abc_untrack_basket_removes_multiple_baskets(test_pantry):
    """Test IndexABC untrack_basket works when passing a multi-address list."""
    raise NotImplementedError


def test_index_abc_get_row_works(test_pantry):
    """Test IndexABC get_row returns manifest data of basket addresses."""
    raise NotImplementedError


def test_index_abc_get_parents_path_works(test_pantry):
    """Test IndexABC get_parents(path) returns proper structure and values."""
    raise NotImplementedError


def test_index_abc_get_parents_uuid_works(test_pantry):
    """Test IndexABC get_parents(uuid) returns proper structure and values."""
    raise NotImplementedError


def test_index_abc_get_parents_invalid_basket_address(test_pantry):
    """Test IndexABC get_parents fails given an invalid basket address."""
    raise NotImplementedError


def test_index_abc_get_parents_no_parents(test_pantry):
    """Test IndexABC get_parents returns an empty dataframe when a basket has
    no parents."""
    raise NotImplementedError


def test_index_abc_get_parents_parent_is_child_loop(test_pantry):
    """Test IndexABC get_parents fails if parent-child loop exists.

    Set up 3 baskets, child, parent, grandparent, but the grandparent's
    parent_ids has the child's uid. This causes an infinite loop,
    check that it throw error.
    """
    raise NotImplementedError


def test_index_abc_get_parents_15_deep(test_pantry):
    """Test IndexABC get_parents works with deep parent-child structures.

    Make a parent-child relationship of baskets 15 deep, get all the parents
    Pass a child with a great*15 grandparent, and return all the grandparents
    for the child.
    Manually make the data and compare with the result.
    """
    raise NotImplementedError


def test_index_abc_get_parents_complex_fail(test_pantry):
    """Test IndexABC get_parents fails on an invalid complicated loop tree."""
    raise NotImplementedError


def test_index_abc_get_children_path_works(test_pantry):
    """Test IndexABC get_children(path) returns proper structure and values."""
    raise NotImplementedError


def test_index_abc_get_children_uuid_works(test_pantry):
    """Test IndexABC get_children(uuid) returns proper structure and values."""
    raise NotImplementedError


def test_index_abc_get_children_invalid_basket_address(test_pantry):
    """Test IndexABC get_children fails given an invalid basket address."""
    raise NotImplementedError


def test_index_abc_get_children_no_children(test_pantry):
    """Test IndexABC get_children returns an empty dataframe when a basket has
    no children."""
    raise NotImplementedError


def test_index_abc_get_children_child_is_parent_loop(test_pantry):
    """Test IndexABC get_children fails if parent-child loop exists.

    Set up 3 baskets, child, parent, grandparent, but the grandparents's
    parent_ids has the child's uid. This causes an infinite loop,
    check that it throw error.
    """
    raise NotImplementedError


def test_index_abc_get_children_15_deep(test_pantry):
    """Test IndexABC get_children works with deep parent-child structures.

    Make a parent-child relationship of baskets 15 deep, get the children.
    Pass a child with great*15 grandparent, and return all the grandchildren
    for the highest grandparent.
    Manually make the data and compare with the result.
    """
    raise NotImplementedError


def test_index_abc_get_children_complex_fail(test_pantry):
    """Test IndexABC get_children fails on an invalid complicated loop tree."""
    raise NotImplementedError


def test_index_abc_get_baskets_of_type_works(test_pantry):
    """Test IndexABC get_baskets_of_type returns correct dataframe."""
    raise NotImplementedError


def test_index_abc_get_baskets_of_type_max_rows_works(test_pantry):
    """Test IndexABC get_baskets_of_type max_rows argument works properly."""
    raise NotImplementedError


def test_index_abc_get_baskets_of_type_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_of_type returns empty df if no baskets of type
    """
    raise NotImplementedError


def test_index_abc_get_baskets_of_label_works(test_pantry):
    """Test IndexABC get_baskets_of_label returns correct dataframe."""
    raise NotImplementedError


def test_index_abc_get_baskets_of_label_max_rows_works(test_pantry):
    """Test IndexABC get_baskets_of_label max_rows argument works properly."""
    raise NotImplementedError


def test_index_abc_get_baskets_of_label_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_of_label returns empty df if no baskets have
    the given label."""
    raise NotImplementedError


def test_index_abc_get_baskets_by_upload_time_raises_value_error1(test_pantry):
    """Test IndexABC get_baskets_by_upload_time raises a ValueError when
    neither start nor end times are specified."""
    raise NotImplementedError


def test_index_abc_get_baskets_by_upload_time_raises_value_error2(test_pantry):
    """Test IndexABC get_baskets_by_upload_time raises a ValueError when
    either start or stop times are not valid datetime format (ie, not UTC)."""
    raise NotImplementedError


def test_index_abc_get_baskets_by_upload_time_start_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with only a start time"""
    raise NotImplementedError


def test_index_abc_get_baskets_by_upload_time_end_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with only an end time"""
    raise NotImplementedError


def test_index_abc_get_baskets_by_upload_time_start_end_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with both start and end
    times supplied."""
    raise NotImplementedError


def test_index_abc_get_baskets_by_upload_time_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_by_upload_time returns empty df when no entry
    is found between start/end."""
    raise NotImplementedError


def test_index_abc_builtin_len_works(test_pantry):
    """Test IndexABC builtin __len__ returns number of baskets being tracked"""
    raise NotImplementedError


def test_index_abc_builtin_str_works(test_pantry):
    """Test IndexABC builtin __str__ returns str of the concrete class name."""
    raise NotImplementedError
