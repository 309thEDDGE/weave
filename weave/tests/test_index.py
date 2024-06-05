"""Pytest tests for the index directory."""

import os
import sys
import re
import warnings
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import fsspec
from fsspec.implementations.local import LocalFileSystem

import weave
from weave import IndexSQLite
from weave import IndexSQL
from weave import Pantry
from weave.index.index_pandas import IndexPandas
from weave.index.create_index import create_index_from_fs
from weave.tests.pytest_resources import PantryForTest, IndexForTest
from weave.tests.pytest_resources import cleanup_sql_index, get_file_systems


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
# write the tests continuing in a different script, which is unnecessarily
# complex. Disabling this warning for this script.
# pylint: disable=too-many-lines

# Pylint doesn't like redefining the test fixture here from
# test_basket, but this is the right way to do this in case at some
# point in the future there is a need to differentiate the two.
# pylint: disable=duplicate-code


# Create fsspec objects to be tested, and add to file_systems list.
file_systems, _ = get_file_systems()

# Create Index CONSTRUCTORS of Indexes to be tested, and add to indexes list.
indexes = [IndexPandas, IndexSQLite]
indexes_ids = ["Pandas", "SQLite"]

# Only add IndexSQL if the env variables are set and dependencies are present.
if "WEAVE_SQL_HOST" in os.environ and "sqlalchemy" in sys.modules:
    indexes.append(IndexSQL)
    indexes_ids.append("SQL")

# Create combinations of the above parameters to pass into the fixture..
params = []
params_ids = []
for iter_file_system in file_systems:
    for iter_index, iter_index_id in zip(indexes, indexes_ids):
        params.append((iter_file_system, iter_index))
        params_ids.append(f"{iter_file_system.__class__.__name__}-"
                          f"-{iter_index_id}")


@pytest.fixture(name="test_pantry", params=params, ids=params_ids)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests."""
    file_system = request.param[0]
    pantry_path = (
        "pytest-temp-pantry" f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    test_pantry = PantryForTest(tmpdir, file_system, pantry_path=pantry_path)

    index_constructor = request.param[1]
    test_index = IndexForTest(
        index_constructor=index_constructor,
        file_system=file_system,
        pantry_path=pantry_path,
    )
    index = test_index.index

    yield test_pantry, index
    test_pantry.cleanup_pantry()
    test_index.cleanup_index()


@pytest.fixture(name="test_index_only", params=indexes, ids=indexes_ids)
def fixture_test_index_only(request):
    """Sets up only the index for a test (USES DEFAULT FILE_SYSTEM ARGS).

    Use this fixture for tests that DO NOT manipulate the pantry (ie
    type checking tests, etc.)
    """
    index_constructor = request.param
    file_system = LocalFileSystem()
    pantry_path = (
        "pytest-temp-pantry" f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    test_index = IndexForTest(
        index_constructor=index_constructor,
        file_system=file_system,
        pantry_path=pantry_path
    )
    index = test_index.index

    yield index
    test_index.cleanup_index()


def test_index_abc_builtin_len_works(test_pantry):
    """Test IndexABC builtin __len__ returns number of baskets being tracked.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
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


def test_index_abc_generate_metadata_returns_dict(test_index_only):
    """Test IndexABC generate_metadata returns a python dictionary."""
    ind = test_index_only

    metadata = ind.generate_metadata()
    assert (
        isinstance(metadata, dict)
    ), "Index.generate_metadata must return a dict."


def test_index_abc_generate_index_works(test_pantry):
    """Tests IndexABC generate_index uses the pantry fs to add to the index."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Generate the index.
    ind.generate_index()
    assert len(ind) == 0, "Incorrect number of elements in the index."

    # Put basket in the temporary pantry
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

    # Put basket in the temporary pantry
    uuid, basket_type, label = "0001", "test_basket", "test_label"
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uuid,
        basket_type=basket_type,
        label=label
    )

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    # Check a pandas dataframe returns of the correct length.
    assert len(ind_df) == 1 and isinstance(ind_df, pd.DataFrame)

    # Check df columns are named correctly.
    assert (
        list(ind_df.columns) == weave.config.get_index_column_names()
    ), "Dataframe columns do not match"

    # Check values of basket are accurate
    assert (
        ind_df.iloc[0]["uuid"] == uuid and
        isinstance(ind_df.iloc[0]["upload_time"], datetime) and
        ind_df.iloc[0]["parent_uuids"] == [] and
        ind_df.iloc[0]["basket_type"] == basket_type and
        ind_df.iloc[0]["label"] == label and
        Path(ind_df.iloc[0]["address"]).match(up_dir) and
        ind_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match."

    # Upload another basket
    uuid, basket_type, label = "0002", "test_basket", "test_label"
    parent_ids = ["0001"]
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_two")
    up_dir = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uuid,
        basket_type=basket_type,
        parent_ids=parent_ids,
        label=label
    )

    # Generate the index.
    ind.generate_index()
    ind_df = ind.to_pandas_df()

    # Check a pandas dataframe returns of the correct length.
    assert len(ind_df) == 2 and isinstance(ind_df, pd.DataFrame)

    # Check df columns are named correctly.
    assert (
        list(ind_df.columns) == weave.config.get_index_column_names()
    ), "Dataframe columns do not match"

    # Check values of basket are accurate
    assert (
        ind_df.iloc[1]["uuid"] == uuid and
        isinstance(ind_df.iloc[1]["upload_time"], datetime) and
        ind_df.iloc[1]["parent_uuids"] == parent_ids and
        ind_df.iloc[1]["basket_type"] == basket_type and
        ind_df.iloc[1]["label"] == label and
        Path(ind_df.iloc[1]["address"]).match(up_dir) and
        ind_df.iloc[1]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match."


def test_index_abc_to_pandas_df_max_rows_and_offset_work(test_pantry):
    """Test IndexABC to_pandas_df max_rows and offset arguments."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
    for basket_iter in range(3):
        tmp_basket_dir_one = test_pantry.set_up_basket(f"basket_{basket_iter}")
        test_pantry.upload_basket(
            tmp_basket_dir=tmp_basket_dir_one,
            uid=f"000{basket_iter}",
        )

    # Generate the index.
    ind.generate_index()

    baskets = ind.to_pandas_df(max_rows=2)
    baskets2 = ind.to_pandas_df(max_rows=2, offset=1)
    assert len(baskets) == 2
    assert len(baskets2) == 2
    assert baskets.iloc[1].uuid == baskets2.iloc[0].uuid


def test_index_abc_track_basket_adds_single_basket(test_pantry):
    """Test IndexABC track_basket works when passing a single basket df."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Generate the index.
    ind.generate_index()
    assert len(ind) == 0, "Incorrect number of elements in the index."

    # Put basket in the temporary pantry
    uuid, basket_type, label = "0001", "test_basket", "test_label"
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uuid,
        basket_type=basket_type,
        label=label
    )

    single_indice_index = create_index_from_fs(
        up_dir,
        test_pantry.file_system
    )

    ind.track_basket(single_indice_index)
    assert len(ind) == 1, "Incorrect number of elements in the Index"

    # Check values of basket are accurate
    ind_df = ind.to_pandas_df()
    assert (
        ind_df.iloc[0]["uuid"] == uuid and
        isinstance(ind_df.iloc[0]["upload_time"], datetime) and
        ind_df.iloc[0]["parent_uuids"] == [] and
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

    # Put basket in the temporary pantry
    uuids = ["0001", "0002"]
    basket_type = "test_basket"
    test_label = "test_label"
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir1 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid=uuids[0],
        basket_type=basket_type,
        label=test_label
    )
    first_slice_df = create_index_from_fs(up_dir1, test_pantry.file_system)

    # Upload another basket
    parent_ids2 = ["0001"]
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    up_dir2 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two,
        uid=uuids[1],
        basket_type=basket_type,
        parent_ids=parent_ids2,
        label=test_label
    )
    second_slice_df = create_index_from_fs(up_dir2, test_pantry.file_system)

    dual_slice_df = pd.concat([first_slice_df, second_slice_df])
    assert len(dual_slice_df) == 2, "Invalid dual_indice_index values."

    ind.track_basket(dual_slice_df)
    ind_df = ind.to_pandas_df()

    # Check a pandas dataframe returns of the correct length.
    assert (
        len(ind_df) == 2 and isinstance(ind_df, pd.DataFrame)
    ), "track_basket failed to add multiple items."

    # Check values of basket are accurate
    assert (
        ind_df.iloc[0]["uuid"] == uuids[0] and
        isinstance(ind_df.iloc[0]["upload_time"], datetime) and
        ind_df.iloc[0]["parent_uuids"] == [] and
        ind_df.iloc[0]["basket_type"] == basket_type and
        ind_df.iloc[0]["label"] == test_label and
        ind_df.iloc[0]["address"].endswith(up_dir1) and
        ind_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match first record."

    assert (
        ind_df.iloc[1]["uuid"] == uuids[1] and
        isinstance(ind_df.iloc[1]["upload_time"], datetime) and
        ind_df.iloc[1]["parent_uuids"] == ["0001"] and
        ind_df.iloc[1]["basket_type"] == basket_type and
        ind_df.iloc[1]["label"] == test_label and
        ind_df.iloc[1]["address"].endswith(up_dir2) and
        ind_df.iloc[1]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match second record."

    # Check df columns are named correctly.
    assert (
        list(ind_df.columns) == weave.config.get_index_column_names()
    ), "Dataframe columns do not match"


def test_index_abc_untrack_basket_removes_single_basket(test_pantry):
    """Test IndexABC untrack_basket works when passing a basket address."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
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

    # Put basket in the temporary pantry.
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

    # Put basket in the temporary pantry.
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

    uuids = ["0001"]
    labels = ["label_1"]
    parent_ids = [[]]

    # Put basket in the temporary pantry.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir1 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        basket_type=basket_type,
        uid=uuids[-1],
        label=labels[-1],
        parent_ids=parent_ids[-1],
    )

    uuids.append("0002")
    labels.append("label_2")
    parent_ids.append([uuids[0]])

    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    up_dir2 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two,
        basket_type=basket_type,
        uid=uuids[-1],
        label=labels[-1],
        parent_ids=parent_ids[-1],
    )

    tmp_basket_dir_three = test_pantry.set_up_basket("basket_three")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_three, uid="0003")

    # Generate the index.
    ind.generate_index()

    first_row_df = ind.get_rows("0001")
    assert isinstance(first_row_df, pd.DataFrame) and len(first_row_df) == 1
    assert (
        first_row_df.iloc[0]["uuid"] == uuids[0] and
        isinstance(first_row_df.iloc[0]["upload_time"], datetime) and
        first_row_df.iloc[0]["parent_uuids"] == parent_ids[0] and
        first_row_df.iloc[0]["basket_type"] == basket_type and
        first_row_df.iloc[0]["label"] == labels[0] and
        Path(first_row_df.iloc[0]["address"]).match(up_dir1) and
        first_row_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match first record."

    second_row_df = ind.get_rows(up_dir2)
    assert isinstance(second_row_df, pd.DataFrame) and len(second_row_df) == 1
    assert (
        second_row_df.iloc[0]["uuid"] == uuids[1] and
        isinstance(second_row_df.iloc[0]["upload_time"], datetime) and
        second_row_df.iloc[0]["parent_uuids"] == parent_ids[1] and
        second_row_df.iloc[0]["basket_type"] == basket_type and
        second_row_df.iloc[0]["label"] == labels[1] and
        Path(second_row_df.iloc[0]["address"]).match(up_dir2) and
        second_row_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match second record."


def test_index_abc_get_rows_multiple_address_works(test_pantry):
    """Test IndexABC get_rows returns manifest data of multiple addresses."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    basket_type = "test_basket_type"

    uuid1 = "0001"
    label1 = "label_1"
    parent_ids1 = []

    # Put basket in the temporary pantry.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    up_dir1 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        basket_type=basket_type,
        uid=uuid1,
        label=label1,
        parent_ids=parent_ids1,
    )

    uuid2 = "0002"
    label2 = "label_2"
    parent_ids2 = [uuid1]

    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    up_dir2 = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two,
        basket_type=basket_type,
        uid=uuid2,
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
        rows_df.iloc[0]["uuid"] == uuid1 and
        isinstance(rows_df.iloc[0]["upload_time"], datetime) and
        rows_df.iloc[0]["parent_uuids"] == parent_ids1 and
        rows_df.iloc[0]["basket_type"] == basket_type and
        rows_df.iloc[0]["label"] == label1 and
        Path(rows_df.iloc[0]["address"]).match(up_dir1) and
        rows_df.iloc[0]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match first record."
    assert (
        rows_df.iloc[1]["uuid"] == uuid2 and
        isinstance(rows_df.iloc[1]["upload_time"], datetime) and
        rows_df.iloc[1]["parent_uuids"] == parent_ids2 and
        rows_df.iloc[1]["basket_type"] == basket_type and
        rows_df.iloc[1]["label"] == label2 and
        Path(rows_df.iloc[1]["address"]).match(up_dir2) and
        rows_df.iloc[1]["storage_type"] == \
            test_pantry.file_system.__class__.__name__
    ), "Retrieved manifest values do not match second record."


def test_index_abc_get_parents_path_works(test_pantry):
    """Test IndexABC get_parents(path) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Setup random strucutre of parents and children
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

    # String to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # Setup df of the right answer
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

    # Pandas wants to make a copy before adding a column
    # Used to remove warning in pytest
    parent_answer = parent_answer.copy()
    # Add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # Get the results
    results = ind.get_parents(child)

    # Sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # Cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_index_abc_get_parents_uuid_works(test_pantry):
    """Test IndexABC get_parents(uuid) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Setup random strucutre of parents and children
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
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # String to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # Setup df of the right answer
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

    # Pandas wants to make a copy before adding a column
    # Used to remove warning in pytest
    parent_answer = parent_answer.copy()
    # Add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # Get the results
    results = ind.get_parents("0000")

    # Sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # Cast to int64 so datatypes match
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
    no parents.
    """
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
    check that it throws an error.
    """

    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Create a basket structure with child, parent, and grandparent, but
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

    # Get the anwser to compare to the results
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

    # Pandas wants to make a copy before adding a column
    # Used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(par_ids, par_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    # Format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)
    assert answer.equals(results)


def test_index_abc_get_parents_complex_fail(test_pantry):
    """Test IndexABC get_parents fails on an invalid complicated loop tree."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    tmp_dir = test_pantry.set_up_basket("parent_8")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="008", parent_ids=["007"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_7")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="007", parent_ids=["000"]
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
        tmp_basket_dir=tmp_dir, uid="002", parent_ids=["0004", "005", "008"]
    )

    tmp_dir = test_pantry.set_up_basket("parent_1")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="001", parent_ids=["004"]
    )

    tmp_dir = test_pantry.set_up_basket("child")
    child_path = test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="000", parent_ids=["001", "002", "003"]
    )

    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 000")
    ):
        ind.get_parents(child_path)


def test_index_abc_get_parents_max_gen_level(test_pantry):
    """Test IndexABC get_parents max_gen_level removes uuids where the
    gen_level is greater than the max_gen_level.
    """

    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Setup random strucutre of parents and children
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
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    ind.generate_index()

    parents_df = ind.get_parents("0000", max_gen_level=2)
    parents_df_uuids = parents_df["uuid"].tolist()
    parents_df_uuids.sort()

    correct_uuids = ["1000", "1001", "2000", "2002", "3303"]
    correct_uuids.sort()

    # Check that max_gen_level filters out the correct uuids
    assert parents_df_uuids == correct_uuids


def test_index_abc_get_children_path_works(test_pantry):
    """Test IndexABC get_children(path) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Setup random strucutre of parents and children
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

    # String to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # Setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.to_pandas_df()
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # Pandas wants to make a copy before adding a column
    # Used to remove warning in pytest
    child_answer = child_answer.copy()
    # Add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j

    # Get the results
    results = ind.get_children(great_grandparent)

    # Sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # Cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)
    assert child_answer.equals(results)


def test_index_abc_get_children_uuid_works(test_pantry):
    """Test IndexABC get_children(uuid) returns proper structure and values."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Setup random strucutre of parents and children
    tmp_dir = test_pantry.set_up_basket("great_grandparent_3")
    test_pantry.upload_basket(
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

    # String to shorten things for ruff
    gen_lvl = "generation_level"

    ind.generate_index()

    # Setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.to_pandas_df()
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # Pandas wants to make a copy before adding a column
    # Used to remove warning in pytest
    child_answer = child_answer.copy()
    # Add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = int(j)

    # Get the results
    results = ind.get_children("3000")

    # Sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid").reset_index(drop=True)
    results = results.sort_values(by="uuid").reset_index(drop=True)

    # Cast to int64 so datatypes match
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
    check that it throws an error.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Create a basket structure with child, parent, and grandparent, but
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
        parent_id = f"00{i}"
        tmp = test_pantry.set_up_basket("basket_" + child_id)
        test_pantry.upload_basket(
            tmp_basket_dir=tmp, uid=child_id, parent_ids=[parent_id]
        )

    ind.generate_index()
    index = ind.to_pandas_df()
    results = ind.get_children("0013")

    # Get the anwser to compare to the results
    child_ids = [
        "x",
        "000",
        "001",
        "002",
        "003",
        "004",
        "005",
        "006",
        "007",
        "008",
        "009",
        "0010",
        "0011",
        "0012",
    ]
    child_gens = [-14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1]
    answer = index.loc[index["uuid"].isin(child_ids)]

    gen_lvl = "generation_level"

    # Pandas wants to make a copy before adding a column
    # Used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(child_ids, child_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    # Format and sort so .equals can be properly used
    answer = answer.sort_values(by="generation_level").reset_index(drop=True)
    results = results.sort_values(by="generation_level").reset_index(drop=True)
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)
    assert answer.equals(results)


def test_index_abc_get_children_complex_fail(test_pantry):
    """Test IndexABC get_children fails on an invalid complicated loop tree.
    Make a complicated tree with a loop to test new algorithm.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
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
        ind.get_children(parent_path)


def test_index_abc_get_children_min_gen_level(test_pantry):
    """Test IndexABC get_children min_gen_level removes uuids where the
    gen_level is less than the min_gen_level.
    """

    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Setup random strucutre of parents and children
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
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    ind.generate_index()

    children_df = ind.get_children("3000", min_gen_level=-2)
    children_df_uuids = children_df["uuid"].tolist()
    children_df_uuids.sort()

    correct_uuids = ["1000", "2000"]
    correct_uuids.sort()

    # Check that min_gen_level filters the correct uuids
    assert children_df_uuids == correct_uuids


def test_index_abc_get_baskets_of_type_works(test_pantry):
    """Test IndexABC get_baskets_of_type returns correct dataframe."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
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


def test_index_abc_get_baskets_of_type_max_rows_and_offset_work(test_pantry):
    """Test IndexABC get_baskets_of_type max_rows and offset arguments."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
    for basket_iter in range(3):
        tmp_basket_dir_one = test_pantry.set_up_basket(f"basket_{basket_iter}")
        test_pantry.upload_basket(
            tmp_basket_dir=tmp_basket_dir_one,
            uid=f"000{basket_iter}",
        )

    # Generate the index.
    ind.generate_index()

    baskets = ind.get_baskets_of_type("test_basket", max_rows=2)
    baskets2 = ind.get_baskets_of_type("test_basket", max_rows=2, offset=1)
    assert len(baskets) == 2
    assert len(baskets2) == 2
    assert baskets.iloc[1].uuid == baskets2.iloc[0].uuid


def test_index_abc_get_baskets_of_type_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_of_type returns empty df if no baskets of
    type.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
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

    # Put basket in the temporary pantry.
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


def test_index_abc_get_baskets_of_label_max_rows_and_offset_work(test_pantry):
    """Test IndexABC get_baskets_of_label max_rows and offset arguments."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
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
    baskets2 = ind.get_baskets_of_label("good_label", max_rows=2, offset=1)
    assert len(baskets) == 2
    assert len(baskets2) == 2
    assert baskets.iloc[1].uuid == baskets2.iloc[0].uuid


def test_index_abc_get_baskets_of_label_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_of_label returns empty df if no baskets have
    the given label.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
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
    either start or stop times are not valid datetime format (ie, not UTC).
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    with pytest.raises(ValueError, match="start_time is not datetime object."):
        ind.get_baskets_by_upload_time(start_time=1)
    with pytest.raises(ValueError, match="end_time is not datetime object."):
        time_str = "2022-02-03 11:12:13"
        ind.get_baskets_by_upload_time(end_time=time_str)


def test_index_abc_get_baskets_by_upload_time_start_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with only a start time.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Save the current time, and set the 'end' time to 5 seconds ago.
    start = datetime.now(timezone.utc) - timedelta(seconds=5)

    # Get the index columns and instantiate with empty lists.
    columns = weave.config.get_index_column_names()
    manifest_dict = dict.fromkeys(columns, ["placeholder-data"])

    # Set placeholder values (for columns that matter)
    # that will have the time and uuid replaced.
    manifest_dict["uuid"] = ["PLACEHOLDER"]
    manifest_dict["upload_time"] = [start]
    manifest_dict["parent_uuids"] = [[]]
    manifest_dict["basket_type"] = ["test_basket_type"]
    manifest_dict["label"] = ["test_label"]
    manifest_dict["address"] = ["temp-pantry-address"]
    manifest_dict["storage_type"] = ["fake-storage"]

    # Create and track a record, with the upload time 1 second before start.
    manifest_dict["uuid"] = ["0001"]
    manifest_dict["upload_time"] = [start - timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time at the start.
    manifest_dict["uuid"] = ["0002"]
    manifest_dict["upload_time"] = [start]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time 1 second after the start.
    manifest_dict["uuid"] = ["0003"]
    manifest_dict["upload_time"] = [start + timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    baskets = ind.get_baskets_by_upload_time(start_time=start)

    assert len(baskets) == 2
    assert "0001" not in baskets["uuid"].to_list()


def test_index_abc_get_baskets_by_upload_time_end_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with only an end time."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry
    ind.to_pandas_df()

    # Save the current time, and set the 'end' time to 5 seconds ago.
    end = datetime.now(timezone.utc) - timedelta(seconds=5)

    # Get the index columns and instantiate with empty lists.
    columns = weave.config.get_index_column_names()
    manifest_dict = dict.fromkeys(columns, ["placeholder-data"])

    # Set placeholder values (for columns that matter)
    # that will have the time and uuid replaced.
    manifest_dict["uuid"] = ["PLACEHOLDER"]
    manifest_dict["upload_time"] = [end]
    manifest_dict["parent_uuids"] = [[]]
    manifest_dict["basket_type"] = ["test_basket_type"]
    manifest_dict["label"] = ["test_label"]
    manifest_dict["address"] = ["temp-pantry-address"]
    manifest_dict["storage_type"] = ["fake-storage"]

    # Create and track a record, with the upload time 1 second before the end.
    manifest_dict["uuid"] = ["0001"]
    manifest_dict["upload_time"] = [end - timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time at the end.
    manifest_dict["uuid"] = ["0002"]
    manifest_dict["upload_time"] = [end]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time 1 second after the end.
    manifest_dict["uuid"] = ["0003"]
    manifest_dict["upload_time"] = [end + timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    baskets = ind.get_baskets_by_upload_time(end_time=end)

    assert len(baskets) == 2
    assert "0003" not in baskets["uuid"].to_list()


def test_index_abc_get_baskets_by_upload_time_start_end_works(test_pantry):
    """Test IndexABC get_baskets_by_upload_time works with both start and end
    times supplied.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry
    ind.to_pandas_df()

    # Save the current time, and set the 'start' time to now.
    start = datetime.now(timezone.utc)

    # Get the index columns and instantiate with empty lists.
    columns = weave.config.get_index_column_names()
    manifest_dict = dict.fromkeys(columns, ["placeholder-data"])

    # Set placeholder values (for columns that matter)
    # that will have the time and uuid replaced.
    manifest_dict["uuid"] = ["PLACEHOLDER"]
    manifest_dict["upload_time"] = [start]
    manifest_dict["parent_uuids"] = [[]]
    manifest_dict["basket_type"] = ["test_basket_type"]
    manifest_dict["label"] = ["test_label"]
    manifest_dict["address"] = ["temp-pantry-address"]
    manifest_dict["storage_type"] = ["fake-storage"]

    # Create and track a record, with the upload time 1 second after the start.
    manifest_dict["uuid"] = ["0001"]
    manifest_dict["upload_time"] = [start]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time 1 second after the start.
    manifest_dict["uuid"] = ["0002"]
    manifest_dict["upload_time"] = [start + timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Make the stop time 2 seconds after the start.
    end = start + timedelta(seconds=2)

    # Create and track a record, with the upload time at the end time.
    manifest_dict["uuid"] = ["0003"]
    manifest_dict["upload_time"] = [end]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time 1 second after the end.
    manifest_dict["uuid"] = ["0004"]
    manifest_dict["upload_time"] = [end + timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Get the baskets inbetween start and end times (1 basket).
    baskets = ind.get_baskets_by_upload_time(start_time=start, end_time=end)

    assert len(baskets) == 3
    assert "0004" not in baskets["uuid"].to_list()


def test_index_abc_get_baskets_by_upload_time_returns_empty_df(test_pantry):
    """Test IndexABC get_baskets_by_upload_time returns empty df when no entry
    is found between start/end.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Save the current time, and set the 'start' time to now.
    # Save the end time to 2 seconds after the start.
    start = datetime.now(timezone.utc)
    end = start + timedelta(seconds=2)

    # Get the index columns and instantiate with empty lists.
    columns = weave.config.get_index_column_names()
    manifest_dict = dict.fromkeys(columns, ["placeholder-data"])

    # Set placeholder values (for columns that matter)
    # that will have the time and uuid replaced.
    manifest_dict["uuid"] = ["PLACEHOLDER"]
    manifest_dict["upload_time"] = [start]
    manifest_dict["parent_uuids"] = [[]]
    manifest_dict["basket_type"] = ["test_basket_type"]
    manifest_dict["label"] = ["test_label"]
    manifest_dict["address"] = ["temp-pantry-address"]
    manifest_dict["storage_type"] = ["fake-storage"]

    # Create and track a record, with the upload time 1 second before the start
    manifest_dict["uuid"] = ["0001"]
    manifest_dict["upload_time"] = [start - timedelta(seconds=1)]

    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Create and track a record, with the upload time 1 second after the end.
    manifest_dict["uuid"] = ["0002"]
    manifest_dict["upload_time"] = [end + timedelta(seconds=1)]
    basket_df = pd.DataFrame.from_dict(manifest_dict)
    ind.track_basket(basket_df)

    # Get the baskets inbetween start and end times (1 basket).
    baskets = ind.get_baskets_by_upload_time(start_time=start, end_time=end)

    assert isinstance(baskets, pd.DataFrame) and len(baskets) == 0


def test_index_abc_columns_in_df_are_same_as_config_index_columns(test_pantry):
    """Test IndexABC tracks the same columns found in
    config.get_index_column_names().
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary pantry.
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one,
        uid="0001",
        label='good_label',
    )

    ind.generate_index()
    ind_df = ind.to_pandas_df()
    ind_df_columns = list(ind_df.columns)

    # Get the columns in the schema, and add derived columns.
    index_columns = weave.config.get_index_column_names()

    # Sort both for comparisons.
    ind_df_columns.sort()
    index_columns.sort()

    assert ind_df_columns == index_columns


def test_read_only_generate_index(test_pantry):
    """Show that weave is able to generate an index when using a read-only fs
    """
    _, index = test_pantry

    test_dir = None
    if os.name == "nt":
        test_dir = "."

    with tempfile.TemporaryDirectory(dir=test_dir) as tmpdir:
        tmp_pantry = Pantry(type(index),
                            pantry_path=tmpdir,
                            file_system=LocalFileSystem())

        tmp_file_path = os.path.join(tmpdir, "temp_basket.txt")
        with open(tmp_file_path, "w", encoding="utf-8") as tmp_file:
            _ = tmp_pantry.upload_basket(
                upload_items=[{"path":tmp_file.name, "stub":False}],
                basket_type="read_only",
            )["uuid"][0]

        zip_path = shutil.make_archive(os.path.join(tmpdir, "test_pantry"),
                                       "zip",
                                       tmpdir)

        read_only_fs = fsspec.filesystem("zip", fo=zip_path, mode="r")

        read_only_pantry = Pantry(type(index),
                                  pantry_path="",
                                  file_system=read_only_fs)

        read_only_pantry.index.generate_index()
        read_only_index = read_only_pantry.index.to_pandas_df()

        remove_path = str(tmpdir).replace('/','-')

        cleanup_sql_index(tmp_pantry.index)
        cleanup_sql_index(read_only_pantry.index)

        assert len(read_only_index) == 1

        read_only_pantry_path = read_only_pantry.pantry_path
        del read_only_pantry
        del read_only_fs

    if os.path.exists(f"weave-{remove_path}.db"):
        os.remove(f"weave-{remove_path}.db")
    if os.path.exists(f"weave-{read_only_pantry_path}.db"):
        os.remove(f"weave-{read_only_pantry_path}.db")
