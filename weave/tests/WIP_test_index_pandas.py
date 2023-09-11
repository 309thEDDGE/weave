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
from weave.index.index import Index
from weave import IndexSQLite
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

# We need to ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name
def test_sync_index_gets_latest_index(test_pantry):
    """Tests Index.sync_index by generating two distinct Index objects and
    making sure that they are both syncing to the index pandas DF (represented
    by JSON) on the file_system"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind2.generate_index()

    # assert length of index includes both baskets and excludes the index
    assert len(ind.to_pandas_df()) == 2

    #assert all baskets in index are not index baskets
    for i in range(len(ind.to_pandas_df())):
        basket_type = ind.to_pandas_df()["basket_type"][i]
        assert basket_type != "index"


def test_sync_index_calls_generate_index_if_no_index(test_pantry):
    """Test to make sure that if there isn't a index available then
    generate_index will still be called."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    assert len(ind.to_pandas_df()) == 1


def test_get_index_time_from_path(test_pantry):
    """Tests Index._get_index_time_from_path to ensure it returns the correct
    string."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    path = "C:/asdf/gsdjls/1234567890-index.json"
    # Obviously we need to test a protected access var here.
    # pylint: disable-next=protected-access
    time = Index(
        file_system=test_pantry.file_system
    )._get_index_time_from_path(path=path)
    assert time == 1234567890


def test_to_pandas_df(test_pantry):
    """Test that Index.to_pandas_df returns a pandas df of the appropriate
    length"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    dataframe = ind.to_pandas_df()
    assert len(dataframe) == 1 and isinstance(dataframe, pd.DataFrame)


def test_clean_up_indices_n_not_int(test_pantry):
    """Tests that Index.clean_up_indices errors on a str (should be int)"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    test_str = "the test"
    with pytest.raises(
        ValueError,
        match=re.escape("invalid literal for int() with base 10: 'the test'"),
    ):
        ind = Index(file_system=test_pantry.file_system)
        ind.clean_up_indices(n_keep=test_str)


def test_clean_up_indices_leaves_n_indices(test_pantry):
    """Tests that Index.clean_up_indices leaves behind the correct number of
    indices."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Now there should be two index baskets. clean up all but one of them:
    ind.clean_up_indices(n_keep=1)
    index_path = os.path.join(test_pantry.pantry_path, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 1


def test_clean_up_indices_with_n_greater_than_num_of_indices(test_pantry):
    """Tests that Index.clean_up_indices behaves well when given a number
    greater than the total number of indices."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Now there should be two index baskets. clean up all but three of them:
    # (this should fail, obvs)
    ind.clean_up_indices(n_keep=3)
    index_path = os.path.join(test_pantry.pantry_path, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 2


def test_is_index_current(test_pantry):
    """Creates two Index objects and pits them against eachother in order to
    ensure that Index.is_index_current is working as expected."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind2.generate_index()
    assert ind2.is_index_current() is True and ind.is_index_current() is False

# TODO: Delete file system items from here. Only test index
def test_delete_basket_deletes_basket(test_pantry):
    """Tests Index.delete_basket to make sure it does, in fact, delete the
    basket."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    ind.generate_index()
    ind.delete_basket(basket_uuid="0002")

    # fs_baskets: Baskets in the file system
    fs_baskets = test_pantry.file_system.ls(
        f"{test_pantry.pantry_path}/test_basket"
    )
    # index_baskets: Baskets in the index object
    index_baskets = ind.index_df[ind.index_df["basket_type"] == "test_basket"]

    # Verify basket removed from the index object
    assert len(index_baskets) == 1
    # Verify index object still tracks the file system
    assert len(fs_baskets) == len(index_baskets)
    # Verify the correct basket was deleted
    assert "0002" not in ind.index_df["uuid"].to_list()


def test_delete_basket_fails_if_basket_is_parent(test_pantry):
    """Ensures that Index.delete_basket fails if the basket is found to be a
    parent."""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two, uid="0002", parent_ids=["0001"]
    )
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    with pytest.raises(
        ValueError,
        match=(
            "The provided value for basket_uuid 0001 is listed as a parent "
            + "UUID for another basket. Please delete that basket before "
            + "deleting it's parent basket."
        ),
    ):
        ind.delete_basket(basket_uuid="0001")


def test_get_parents_valid(test_pantry):
    """setup a valid basket structure, validate the returned index"""
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

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
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
    index = ind.index_df
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
    parent_answer = parent_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_get_parents_invalid_basket_address(test_pantry):
    """try and find the parents of an invalid basket path/address"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    basket_path = "INVALIDpath"

    index = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'",
    ):
        index.get_parents(basket_path)


def test_get_parents_no_parents(test_pantry):
    """try and get all parents of basket with no parent uuids.

    check that it returns an empty dataframe/index
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    no_parents = test_pantry.set_up_basket("no_parents")
    no_parents_path = test_pantry.upload_basket(
        tmp_basket_dir=no_parents, uid="0001"
    )

    index = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    index.generate_index()

    parent_indeces = index.get_parents(no_parents_path)

    assert parent_indeces.empty


def test_get_parents_parent_is_child(test_pantry):
    """set up basket structure with parent-child loop, check that it fails

    set up 3 baskets, child, parent, grandparent, but the grandparent's
    parent_ids has the child's uid. this causes an infinite loop,
    check that it throw error
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

    index = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    index.generate_index()

    fail = "1000"

    with pytest.raises(
        ValueError, match=f"Parent-Child loop found at uuid: {fail}"
    ):
        index.get_parents(child)


def test_get_children_valid(test_pantry):
    """setup a valid basket structure, validate the returned dataframe"""
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

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    # setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.index_df
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
    child_answer = child_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)


def test_get_children_invalid_basket_address(test_pantry):
    """try and find he children of an invalid basket path/address"""
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    basket_path = "INVALIDpath"

    index = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'",
    ):
        index.get_children(basket_path)


def test_get_children_no_children(test_pantry):
    """try and get all children of basket that has no children

    check that it returns an empty dataframe/index
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry


    no_children = test_pantry.set_up_basket("no_children")
    no_children_path = test_pantry.upload_basket(
        tmp_basket_dir=no_children, uid="0001"
    )

    index = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    index.generate_index()

    children_indexes = index.get_children(no_children_path)

    assert children_indexes.empty


def test_get_children_child_is_parent(test_pantry):
    """set up a basket structure with a parent-child loop, check that it fails

    set up 3 baskets, child, parent, grandparent, but the grandparents's
    parent_ids has the child's uid. this causes an infinite loop,
    check that it throw error
    """
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

    index = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    index.generate_index()

    fail = "3000"

    with pytest.raises(
        ValueError, match=re.escape(f"Parent-Child loop found at uuid: {fail}")
    ):
        index.get_children(grandparent_basket)


def test_get_parents_15_deep(test_pantry):
    """Make a parent-child relationship of baskets 15 deep, get all the parents

    so a child with a great*15 grandparent, and return all the grandparents
    for the child
    manually make the data and compare with the result
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

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()
    index = ind.index_df

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
    answer = answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)

    assert answer.equals(results)


def test_get_children_15_deep(test_pantry):
    """Make a parent-child relationship of baskets 15 deep, get the children.

    so a child with great*15 grandparent, and return all the grandchildren
    for the highest grandparent.
    manually make the data and compare with the result
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

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()
    index = ind.index_df

    parent_path = index.loc[index["uuid"] == "13"]["address"].values[0]

    results = ind.get_children(parent_path)

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
    answer = answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)

    assert answer.equals(results)


def test_get_parents_complex_fail(test_pantry):
    """Make a complicated tree with a loop to test new algorithm"""
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

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 000")
    ):
        ind.get_parents(child_path)


def test_get_children_complex_fail(test_pantry):
    """Make a complicated tree with a loop to test new algorithm"""
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

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 007")
    ):
        ind.get_children(parent_path)


def test_get_parents_from_uuid(test_pantry):
    """setup a valid basket structure, validate the returned index from uuid"""
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
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # string to shorten things for ruff
    gen_lvl = "generation_level"

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
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
    index = ind.index_df
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
    parent_answer = parent_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_get_children_from_uuid(test_pantry):
    """setup a valid basket structure, validate the returned index from uuid"""
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
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_dir, uid="0000", parent_ids=["1001", "1000"]
    )

    # string to shorten things for ruff
    gen_lvl = "generation_level"

    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    # setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.index_df
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    # add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j

    # get the results with uid of the great grandparent
    results = ind.get_children("3000")

    # sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)

# TODO: Implement for only the index checking
def test_upload_basket_updates_the_index(test_pantry):
    """
    In this test the index already exists with one basket inside of it.
    This test will add another basket using Index.upload_basket, and then check
    to ensure that the index_df has been updated.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    # add some baskets
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    for i in range(3):
        new_basket = ind.upload_basket(
            upload_items=[
                {"path": str(tmp_basket_dir_two.realpath()), "stub": False}
            ],
            basket_type="test",
        )
        if i == 0:
            first_time = pd.to_datetime(ind.index_df.iloc[1].upload_time)
    time_diff = first_time - pd.to_datetime(ind.index_df.iloc[1].upload_time)

    assert all(ind.index_df.iloc[-1] == new_basket.iloc[0])
    assert time_diff.total_seconds() == 0
    assert len(ind.index_df) == 4

def test_upload_basket_works_on_empty_basket(test_pantry):
    """
    In this test the Index object will upload a basket to a pantry that does
    not have any baskets yet. This test will make sure that this functionality
    is present, and that the index_df has been updated.
    """
    # Unpack the test_pantry into two variables for the pantry and index.
    test_pantry, ind = test_pantry

    # Put basket in the temporary bucket
    tmp_basket = test_pantry.set_up_basket("basket_one")
    ind = Index(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system
    )
    ind.upload_basket(
        upload_items=[{"path": str(tmp_basket.realpath()), "stub": False}],
        basket_type="test",
    )
    assert len(ind.index_df) == 1