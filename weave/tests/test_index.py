"""Pytest tests for the index directory."""
from datetime import datetime
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
from weave import IndexSQLite
from weave.index.index_pandas import PandasIndex
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
pandas_index = PandasIndex
indexes = [sqlite_index, pandas_index]

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
def test_index_abc_builtin_len_works(test_pantry):
    """Test IndexABC builtin __len__ returns number of baskets being tracked"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_three")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0003")

    # Generate the index.
    ind.generate_index()
    assert len(ind) == 3, f"Builtin len returns inaccurate value: {len(ind)}"


def test_index_abc_builtin_str_works(test_index_only):
    """Test IndexABC builtin __str__ returns str of the concrete class name."""
    ind = test_index_only
    assert ind.__class__.__name__ == str(ind), (
        f"Buildin str returns incorrect value: {str(ind)}"
    )


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
    assert len(ind) == 0, "Incorrect number of elements in the index."

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Generate the index.
    ind.generate_index()
    assert len(ind) == 1, "Incorrect number of elements in the index."

    # Add another basket to the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")

    # Regenerate the index
    ind.generate_index()
    assert len(ind) == 2, "Incorrect number of elements in the index."


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

    # Check df columns are named correctly.
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

    # Check df columns are named correctly.
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
    assert len(ind) == 0, "Incorrect number of elements in the index."

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
    assert len(ind) == 1, "Incorrect number of elements in the Index"

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

    # Check df columns are named correctly.
    assert (
        list(ind_df.columns) == ["uuid", "upload_time", "parent_uuids",
                                "basket_type", "label", "address",
                                "storage_type"]
    ), "Dataframe columns do not match"


def test_index_abc_untrack_basket_removes_single_basket(test_pantry):
    """Test IndexABC untrack_basket works when passing a basket address."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_three")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0003")

    # Create the addresses to remove the last two baskets.
    untrack_uuid = "0002"
    untrack_path = os.path.join(test_pantry.pantry_path, "test_basket", "0003")

    # Generate the index.
    ind.generate_index()

    ind.untrack_basket(untrack_uuid)
    assert len(ind) == 2, "untrack_basket(uuid) failed to update index."

    ind.untrack_basket(untrack_path)
    assert len(ind) == 1, "untrack_basket(path) failed to update index."


def test_index_abc_untrack_basket_raises_warning(test_pantry):
    """Test IndexABC untrack_basket raises a warning when the address to be
    untracked wasn't tracked to begin with."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    untracked_uuid = "0009"
    untracked_path = os.path.join("test-pantry-that-isnt-real", "fake_basket")

    # Generate the index.
    ind.generate_index()

    # Test the warning is raised when untracking an already untracked UUID.
    with warnings.catch_warnings(record=True) as warn:
        ind.untrack_basket(untracked_uuid)
    warning_list = [warn[i].message for i in range(len(warn))]
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Incomplete Request. Index could not untrack baskets, "
        "as some were not being tracked to begin with."
    )
    assert warning_1.args[1] == 1

    # Test the warning is raised when untracking an already untracked PATH.
    with warnings.catch_warnings(record=True) as warn:
        ind.untrack_basket(untracked_path)
    warning_list = [warn[i].message for i in range(len(warn))]
    warning_2 = warning_list[0]
    assert warning_2.args[0] == (
        "Incomplete Request. Index could not untrack baskets, "
        "as some were not being tracked to begin with."
    )
    assert warning_2.args[1] == 1


def test_index_abc_untrack_basket_removes_multiple_baskets(test_pantry):
    """Test IndexABC untrack_basket works when passing a multi-address list."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")

    tmp_basket_dir_one = test_pantry.set_up_basket("basket_three")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0003")

    # Generate the index.
    ind.generate_index()

    # Create a list of addresses to remove the first two baskets.
    untrack_uuids = ["0001", "0002"]

    ind.untrack_basket(untrack_uuids)
    assert len(ind) == 1, "untrack_basket([uuid]) failed to update index."


def test_index_abc_get_rows_single_address_works(test_pantry):
    """Test IndexABC get_rows returns manifest data of a single address."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    basket_type = "test_basket_type"

    uid1 = "0001"
    label1 = "label_1"
    parent_ids1 = []

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir1 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        basket_type=basket_type,
        uid=uid1,
        label=label1,
        parent_ids=parent_ids1,
    )

    uid2 = "0002"
    label2 = "label_2"
    parent_ids2 = [uid1]

    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    up_dir2 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two,
        basket_type=basket_type,
        uid=uid2,
        label=label2,
        parent_ids=parent_ids2,
    )

    tmp_basket_dir_three = test_pantry.set_up_basket("basket_three")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_three, uid="0003")

    # Generate the index.
    ind.generate_index()

    first_row_df = ind.get_rows("0001")
    assert isinstance(first_row_df, pd.DataFrame) and len(first_row_df) == 1
    assert (
        first_row_df.iloc[0]["uuid"] == uid1 and
        first_row_df.iloc[0]["parent_uuids"] == f"{parent_ids1}" and
        first_row_df.iloc[0]["basket_type"] == basket_type and
        first_row_df.iloc[0]["label"] == label1 and
        first_row_df.iloc[0]["address"].endswith(up_dir1) and
        first_row_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match first record."

    second_row_df = ind.get_rows(up_dir2)
    assert isinstance(second_row_df, pd.DataFrame) and len(second_row_df) == 1
    assert (
        second_row_df.iloc[0]["uuid"] == uid2 and
        second_row_df.iloc[0]["parent_uuids"] == f"{parent_ids2}" and
        second_row_df.iloc[0]["basket_type"] == basket_type and
        second_row_df.iloc[0]["label"] == label2 and
        second_row_df.iloc[0]["address"].endswith(up_dir2) and
        second_row_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match second record."


def test_index_abc_get_rows_multiple_address_works(test_pantry):
    """Test IndexABC get_rows returns manifest data of multiple addresses."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    basket_type = "test_basket_type"

    uid1 = "0001"
    label1 = "label_1"
    parent_ids1 = []

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir1 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        basket_type=basket_type,
        uid=uid1,
        label=label1,
        parent_ids=parent_ids1,
    )

    uid2 = "0002"
    label2 = "label_2"
    parent_ids2 = [uid1]

    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    up_dir2 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two,
        basket_type=basket_type,
        uid=uid2,
        label=label2,
        parent_ids=parent_ids2,
    )

    tmp_basket_dir_three = test_pantry.set_up_basket("basket_three")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_three, uid="0003")

    # Generate the index.
    ind.generate_index()

    rows_df = ind.get_rows(["0001", "0002"])
    assert isinstance(rows_df, pd.DataFrame) and len(rows_df) == 2
    assert (
        rows_df.iloc[0]["uuid"] == uid1 and
        rows_df.iloc[0]["parent_uuids"] == f"{parent_ids1}" and
        rows_df.iloc[0]["basket_type"] == basket_type and
        rows_df.iloc[0]["label"] == label1 and
        rows_df.iloc[0]["address"].endswith(up_dir1) and
        rows_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match first record."
    assert (
        rows_df.iloc[1]["uuid"] == uid2 and
        rows_df.iloc[1]["parent_uuids"] == f"{parent_ids2}" and
        rows_df.iloc[1]["basket_type"] == basket_type and
        rows_df.iloc[1]["label"] == label2 and
        rows_df.iloc[1]["address"].endswith(up_dir2) and
        rows_df.iloc[1]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match second record."


def test_index_abc_get_parents_path_works(test_pantry):
    """Test IndexABC get_parents(path) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    # setup random strucutre of parents and children
    tmp_dir = test_pantry.set_up_basket("great_grandparent_3")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3000")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_2")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_3")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = test_pantry.set_up_basket("grandparent_2")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="2000", parent_ids=["3000", "3003", "3333"]
    )

    tmp_dir = test_pantry.set_up_basket("grandparent_2_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = test_pantry.set_up_basket("parent_1")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="1000", parent_ids=["2000", "2002", "3303"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_1_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = test_pantry.set_up_basket("child_0")
    child = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # string to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # setup df of the right answer
    parent_ids = [
        "1000",
        "1001",
        "2000",
        "2002",
        "3303",
        "3000",
        "3003",
        "3333",
    ]
    parent_gens = [1, 1, 2, 2, 2, 3, 3, 3]
    index = ind.to_pandas_df()
    parent_answer = index.loc[index["uuid"].isin(parent_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    parent_answer = parent_answer.copy()
    # add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # get the results
    results = ind.get_parents(child)

    # sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_index_abc_get_parents_uuid_works(test_pantry):
    """Test IndexABC get_parents(uuid) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    # setup random strucutre of parents and children
    tmp_dir = test_pantry.set_up_basket("great_grandparent_3")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3000")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_2")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_3")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = test_pantry.set_up_basket("grandparent_2")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="2000", parent_ids=["3000", "3003", "3333"]
    )

    tmp_dir = test_pantry.set_up_basket("grandparent_2_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = test_pantry.set_up_basket("parent_1")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="1000", parent_ids=["2000", "2002", "3303"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_1_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = test_pantry.set_up_basket("child_0")
    child = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # string to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # setup df of the right answer
    parent_ids = [
        "1000",
        "1001",
        "2000",
        "2002",
        "3303",
        "3000",
        "3003",
        "3333",
    ]
    parent_gens = [1, 1, 2, 2, 2, 3, 3, 3]
    index = ind.to_pandas_df()
    parent_answer = index.loc[index["uuid"].isin(parent_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    parent_answer = parent_answer.copy()
    # add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # get the results
    results = ind.get_parents("0000")

    # sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_index_abc_get_parents_invalid_basket_address(test_pantry):
    """Test IndexABC get_parents fails given an invalid basket address."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    basket_path = "INVALIDpath"

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'",
    ):
        ind.get_parents(basket_path)


def test_index_abc_get_parents_no_parents(test_pantry):
    """Test IndexABC get_parents returns an empty dataframe when a basket has
    no parents."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    no_parents = test_pantry.set_up_basket("no_parents")
    no_parents_path = test_pantry.upload_basket(
        tmp_basket_dir=no_parents, uid="0001"
    )

    ind.generate_index()

    parent_indeces = ind.get_parents(no_parents_path)

    assert parent_indeces.empty


def test_index_abc_get_parents_parent_is_child_loop(test_pantry):
    """Test IndexABC get_parents fails if parent-child loop exists.

    Set up 3 baskets, child, parent, grandparent, but the grandparent's
    parent_ids has the child's uid. This causes an infinite loop,
    check that it throw error.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    # create a basket structure with child, parent, and grandparent, but
    # the grandparent's parent, is the child, making an loop for the
    # parent-child relationship
    tmp_dir = test_pantry.set_up_basket("grandparent")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="3000", parent_ids=["1000"]
    )

    tmp_dir = test_pantry.set_up_basket("parent")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="2000", parent_ids=["3000"]
    )

    tmp_dir = test_pantry.set_up_basket("child")
    child = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="1000", parent_ids=["2000"]
    )

    ind.generate_index()

    fail = "1000"

    with pytest.raises(
        ValueError, match=f"Parent-Child loop found at uuid: {fail}"
    ):
        ind.get_parents(child)


def test_index_abc_get_parents_15_deep(test_pantry):
    """Test IndexABC get_parents works with deep parent-child structures.

    Make a parent-child relationship of baskets 15 deep, get all the parents
    Pass a child with a great*15 grandparent, and return all the grandparents
    for the child.
    Manually make the data and compare with the result.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    parent_id = "x"

    for i in range(15):
        child_id = parent_id
        parent_id = str(i)
        tmp = test_pantry.set_up_basket("basket_" + child_id)
        test_pantry.upload_basket(
            tmp_basket_dir=tmp, uid=child_id, parent_ids=[parent_id]
        )

    ind.generate_index()
    index = ind.to_pandas_df()

    child_path = index.loc[index["uuid"] == "x"]["address"].values[0]

    results = ind.get_parents(child_path)

    # Get the anwser to compare to the results we got
    par_ids = [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
    ]
    par_gens = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    answer = index.loc[index["uuid"].isin(par_ids)]

    gen_lvl = "generation_level"

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(par_ids, par_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    # format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)
    assert answer.equals(results)


def test_index_abc_get_parents_complex_fail(test_pantry):
    """Test IndexABC get_parents fails on an invalid complicated loop tree."""
    raise NotImplementedError


def test_index_abc_get_children_path_works(test_pantry):
    """Test IndexABC get_children(path) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    # setup random strucutre of parents and children
    tmp_dir = test_pantry.set_up_basket("great_grandparent_3")
    great_grandparent = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="3000"
    )

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_2")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_3")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = test_pantry.set_up_basket("grandparent_2")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="2000", parent_ids=["3000", "3003", "3333"]
    )

    tmp_dir = test_pantry.set_up_basket("grandparent_2_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = test_pantry.set_up_basket("parent_1")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="1000", parent_ids=["2000", "2002", "3303"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_1_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = test_pantry.set_up_basket("child_0")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # string to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.to_pandas_df()
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    # add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j

    # get the results
    results = ind.get_children(great_grandparent)

    # sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)
    assert child_answer.equals(results)


def test_index_abc_get_children_uuid_works(test_pantry):
    """Test IndexABC get_children(uuid) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # setup random strucutre of parents and children
    tmp_dir = test_pantry.set_up_basket("great_grandparent_3")
    great_grandparent = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="3000"
    )

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_2")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = test_pantry.set_up_basket("great_grandparent_3_3")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = test_pantry.set_up_basket("grandparent_2")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="2000", parent_ids=["3000", "3003", "3333"]
    )

    tmp_dir = test_pantry.set_up_basket("grandparent_2_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = test_pantry.set_up_basket("parent_1")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="1000", parent_ids=["2000", "2002", "3303"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_1_1")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = test_pantry.set_up_basket("child_0")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # string to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.to_pandas_df()
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    # add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = int(j)

    # get the results
    results = ind.get_children("3000")

    # sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)


def test_index_abc_get_children_invalid_basket_address(test_pantry):
    """Test IndexABC get_children fails given an invalid basket address."""
    test_pantry, ind = test_pantry

    basket_path = "INVALIDpath"

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'",
    ):
        ind.get_children(basket_path)


def test_index_abc_get_children_no_children(test_pantry):
    """Test IndexABC get_children returns an empty dataframe when a basket has
    no children."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    no_children = test_pantry.set_up_basket("no_children")
    no_children_path = test_pantry.upload_basket(
        tmp_basket_dir=no_children, uid="0001"
    )

    ind.generate_index()

    children_indexes = ind.get_children(no_children_path)

    assert children_indexes.empty


def test_index_abc_get_children_child_is_parent_loop(test_pantry):
    """Test IndexABC get_children fails if parent-child loop exists.

    Set up 3 baskets, child, parent, grandparent, but the grandparents's
    parent_ids has the child's uid. This causes an infinite loop,
    check that it throw error.
    """
    # TODO: Fix loop
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    # create a basket structure with child, parent, and grandparent, but
    # the grandparent's parent, is the child, making an loop for the
    # parent-child relationship
    tmp_dir = test_pantry.set_up_basket("grandparent")
    grandparent_basket = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="3000", parent_ids=["1000"]
    )

    tmp_dir = test_pantry.set_up_basket("parent")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="2000", parent_ids=["3000"]
    )

    tmp_dir = test_pantry.set_up_basket("child")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="1000", parent_ids=["2000"]
    )

    ind.generate_index()

    fail = "3000"

    with pytest.raises(
        ValueError, match=re.escape(f"Parent-Child loop found at uuid: {fail}")
    ):
        ind.get_children(grandparent_basket)


def test_index_abc_get_children_15_deep(test_pantry):
    """Test IndexABC get_children works with deep parent-child structures.

    Make a parent-child relationship of baskets 15 deep, get the children.
    Pass a child with great*15 grandparent, and return all the grandchildren
    for the highest grandparent.
    Manually make the data and compare with the result.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    parent_id = "x"

    for i in range(15):
        child_id = parent_id
        parent_id = str(i)
        tmp = test_pantry.set_up_basket("basket_" + child_id)
        test_pantry.upload_basket(
            tmp_basket_dir=tmp, uid=child_id, parent_ids=[parent_id]
        )

    ind.generate_index()
    index = ind.to_pandas_df()
    results = ind.get_children("13")

    # Get the anwser to compare to the results we got
    child_ids = [
        "x",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
    ]
    child_gens = [-14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1]
    answer = index.loc[index["uuid"].isin(child_ids)]

    gen_lvl = "generation_level"

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(child_ids, child_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    # format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)
    assert answer.equals(results)


def test_index_abc_get_children_complex_fail(test_pantry):
    """Test IndexABC get_children fails on an invalid complicated loop tree."""
    """Make a complicated tree with a loop to test new algorithm"""
    # Unpack the test_pantry into two variables for the pantry and index.
    # TODO: Fix loop
    test_pantry, ind = test_pantry


    tmp_dir = test_pantry.set_up_basket("parent_8")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="008", parent_ids=["007"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_7")
    parent_path = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="007", parent_ids=["003"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_6")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="006", parent_ids=["008"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_5")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="005", parent_ids=["007"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_4")
    test_pantry.upload_basket(tmp_basket_dir=tmp_dir, uid="004")

    tmp_dir = test_pantry.set_up_basket("parent_3")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="003", parent_ids=["006"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_2")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="002", parent_ids=["004", "005", "008"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_1")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="001", parent_ids=["004"]
    )

    tmp_dir = test_pantry.set_up_basket("child")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="000", parent_ids=["001", "002", "003"]
    )

    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 007")
    ):
        df = ind.get_children(parent_path)


def test_index_abc_get_baskets_of_type_works(test_pantry):
    """Test IndexABC get_baskets_of_type returns correct dataframe."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0001",
    )
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(
        basket_type="other",
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0002",
    )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_type("test_basket")
    assert len(baskets) == 1
    assert "0001" in baskets["uuid"].to_list()


def test_index_abc_get_baskets_of_type_max_rows_works(test_pantry):
    """Test IndexABC get_baskets_of_type max_rows argument works properly."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    for basket_iter in range(3):
        tmp_basket_dir_one = test_pantry.set_up_basket(f"basket_{basket_iter}")
        test_pantry.upload_basket(
            tmp_basket_dir=tmp_basket_dir_one,
            uid=f"000{basket_iter}",
        )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_type("test_basket", max_rows=2)
    assert len(baskets) == 2


def test_index_abc_get_baskets_of_type_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_of_type returns empty df if no baskets of type
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0001",
    )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_type("bad_basket_type")
    assert len(baskets) == 0


def test_index_abc_get_baskets_of_label_works(test_pantry):
    """Test IndexABC get_baskets_of_label returns correct dataframe."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0001",
        label='good_label',
    )
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0002",
        label='bad_label',
    )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_label("good_label")
    assert len(baskets) == 1
    assert "0001" in baskets["uuid"].to_list()


def test_index_abc_get_baskets_of_label_max_rows_works(test_pantry):
    """Test IndexABC get_baskets_of_label max_rows argument works properly."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    for basket_iter in range(3):
        tmp_basket_dir_one = test_pantry.set_up_basket(f"basket_{basket_iter}")
        test_pantry.upload_basket(
            tmp_basket_dir=tmp_basket_dir_one,
            uid=f"000{basket_iter}",
            label='good_label',
        )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_label("good_label", max_rows=2)
    assert len(baskets) == 2


def test_index_abc_get_baskets_of_label_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_of_label returns empty df if no baskets have
    the given label."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0001",
        label='good_label',
    )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_label("bad_label")
    assert len(baskets) == 0


def test_index_abc_get_baskets_by_upload_time_raises_value_error2(test_pantry):
    """Test IndexABC get_baskets_by_upload_time raises a ValueError when
    either start or stop times are not valid datetime format (ie, not UTC)."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    with pytest.raises(ValueError, match="start_time is not datetime object."):
        baskets = ind.get_baskets_by_upload_time(start_time=1)
    with pytest.raises(ValueError, match="end_time is not datetime object."):
        time_str = "2022-02-03 11:12:13"
        baskets = ind.get_baskets_by_upload_time(end_time=time_str)


def test_index_abc_get_baskets_by_upload_time_start_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with only a start time"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    start = datetime.now()

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_by_upload_time(start_time=start)

    assert len(baskets) == 1
    assert "0001" not in baskets["uuid"].to_list()


def test_index_abc_get_baskets_by_upload_time_end_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with only an end time"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    end = datetime.now()

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_by_upload_time(end_time=end)

    assert len(baskets) == 1
    assert "0002" not in baskets["uuid"].to_list()

def test_index_abc_get_baskets_by_upload_time_start_end_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with both start and end
    times supplied."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    start = datetime.now()

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    end = datetime.now()

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0002")

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_by_upload_time(start_time=start, end_time=end)

    assert len(baskets) == 1
    assert "0002" not in baskets["uuid"].to_list()


def test_index_abc_get_baskets_by_upload_time_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_by_upload_time returns empty df when no entry
    is found between start/end."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    start = datetime.now()
    end = datetime.now()

    # Put basket in the temporary bucket.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_by_upload_time(start_time=start, end_time=end)
    assert len(baskets) == 0
