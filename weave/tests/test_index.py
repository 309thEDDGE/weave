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
from weave.index.create_index import create_index_from_fs
from weave.index.index import Index
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

# This module is long and has many tests. Pylint is complaining that it is too
# long. This isn't necessarily bad in this case, as the alternative
# would be to write the tests continuuing in a different script, which would
# be unnecesarily complex.
# Disabling this warning for this script.
# pylint: disable=too-many-lines

# Pylint doesn't like redefining the test fixture here from
# test_basket, but this is the right way to do it if at some
# point in the future the two need to be differentiated.
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


# Ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name


def test_root_dir_does_not_exist(test_pantry):
    """Try to create an index in a bucket that doesn't exist,
    check that it throws an error
    """
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    with pytest.raises(FileNotFoundError, match="'root_dir' does not exist"):
        create_index_from_fs(
            os.path.join(tmp_basket_dir_one, "NOT-A-BUCKET"),
            test_pantry.file_system,
        )


def test_root_dir_is_string(test_pantry):
    """Tests create_index_from_fs to make sure it errors when root dir is
    not a string"""
    with pytest.raises(TypeError, match="'root_dir' must be a string"):
        create_index_from_fs(765, test_pantry.file_system)


def test_correct_index(test_pantry):
    """Tests create_index_from_fs to make sure it returns as expected"""
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    addr_one = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_one, uid="0001"
    )

    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    addr_two = test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two, uid="0002", parent_ids=["0001"]
    )

    addresses = [addr_one, addr_two]
    truth_index_dict = {
        "uuid": ["0001", "0002"],
        "upload_time": ["whatever", "dont matter"],
        "parent_uuids": [[], ["0001"]],
        "basket_type": "test_basket",
        "label": "",
        "address": addresses,
        "storage_type": test_pantry.file_system.__class__.__name__,
    }
    expected_index = pd.DataFrame(truth_index_dict)

    actual_index = create_index_from_fs(
        test_pantry.pantry_name, test_pantry.file_system
    )

    # Check that the indexes match, ignoring 'upload_time', and 'address'
    # (address needs to be checked regardless of FS prefix--see next assert)
    assert (
        (expected_index == actual_index)
        .drop(columns=["upload_time", "address"])
        .all()
        .all()
    )

    # Check the addresses are the same, ignoring any FS dependent prefixes.
    assert all(
        (
            actual_index["address"].iloc[i].endswith(addr)
            for i, addr in enumerate(addresses)
        )
    )


# Test with two different fsspec file systems (top of file).
@pytest.fixture(params=[s3fs, local_fs])
def set_up_malformed_baskets(request, tmpdir):
    """
    upload a basket with a basket_details.json with incorrect keys.
    """
    file_system = request.param
    test_pantry = BucketForTest(tmpdir, file_system)

    good_addresses = []
    bad_addresses = []
    for i in range(10):
        tmp_basket_dir = test_pantry.set_up_basket(f"basket_{i}")
        address = test_pantry.upload_basket(
            tmp_basket_dir=tmp_basket_dir, uid=f"000{i}"
        )

        # change a key in the bad baske_manifests
        if (i % 3) == 0:
            bad_addresses.append(address)

            basket_dict = {}
            manifest_address = (
                f"{test_pantry.pantry_name}/test_basket/"
                f"000{i}/basket_manifest.json"
            )

            with test_pantry.file_system.open(
                manifest_address, "rb"
            ) as tp_file:
                basket_dict = json.load(tp_file)
                basket_dict.pop("uuid")
            basket_path = os.path.join(tmp_basket_dir, "basket_manifest.json")
            with open(basket_path, "w", encoding="utf-8") as tp_file:
                json.dump(basket_dict, tp_file)

            test_pantry.file_system.upload(basket_path, manifest_address)

        else:
            good_addresses.append(address)

    yield test_pantry, good_addresses, bad_addresses
    test_pantry.cleanup_bucket()


def test_create_index_with_malformed_basket_works(set_up_malformed_baskets):
    """Check that the index is made correctly when a malformed basket
    exists."""
    test_pantry, good_addresses, _ = set_up_malformed_baskets

    truth_index_dict = {
        "uuid": [f"000{i}" for i in [1, 2, 4, 5, 7, 8]],
        "upload_time": "whatever",
        "parent_uuids": [[], [], [], [], [], []],
        "basket_type": "test_basket",
        "label": "",
        "address": good_addresses,
        "storage_type": test_pantry.file_system.__class__.__name__,
    }
    expected_index = pd.DataFrame(truth_index_dict)

    # Catch the warnings here for bad baskets, but prevent
    # the warnings from dropping through to the pytest log in this test.
    # (Checking the warnings are correct is tested in the next unit test.)
    with warnings.catch_warnings(record=True) as warn:
        actual_index = create_index_from_fs(
            test_pantry.pantry_name, test_pantry.file_system
        )
        message = (
            "baskets found in the following locations "
            "do not follow specified weave schema:\n"
        )

        # Check that the indexes match, ignoring 'upload_time', and 'address'
        # (address needs to be checked regardless of FS prefix-see next assert)
        assert (expected_index == actual_index).drop(
            columns=["upload_time", "address"]
        ).all().all() and str(warn[0].message).startswith(message)

    # Check the addresses are the same, ignoring any FS dependent prefixes.
    assert all(
        (
            actual_index["address"].iloc[i].endswith(addr)
            for i, addr in enumerate(good_addresses)
        )
    )


def test_create_index_with_bad_basket_throws_warning(set_up_malformed_baskets):
    """Check that a warning is thrown during index creation."""
    test_pantry, _, bad_addresses = set_up_malformed_baskets

    with warnings.catch_warnings(record=True) as warn:
        create_index_from_fs(test_pantry.pantry_name, test_pantry.file_system)
        message = (
            "baskets found in the following locations "
            "do not follow specified weave schema:"
        )
        # {bad_addresses} would be included in the message, but
        # due to File System dependent prefixes
        # direct string comparison is not possible

        warn_msg = str(warn[0].message)

        # Check the warning message header/info is correct.
        warn_header_str = warn_msg[: warn_msg.find("\n")]
        assert warn_header_str == message

        # Check the addresses returned in the warning are the correct ones.
        warning_addrs_str = warn_msg[warn_msg.find("\n") + 1 :]
        warning_addrs_list = (
            warning_addrs_str.strip("[]").replace("'", "").split(", ")
        )
        assert all(
            (
                a_addr.endswith(e_addr)
                for a_addr, e_addr in zip(warning_addrs_list, bad_addresses)
            )
        )


def test_sync_index_gets_latest_index(test_pantry):
    """Tests Index.sync_index by generating two distinct Index objects and
    making sure that they are both syncing to the index pandas DF (represented
    by JSON) on the file_system"""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind2.generate_index()

    # Assert length of index includes both baskets and excludes the index
    assert len(ind.to_pandas_df()) == 2

    # Assert all baskets in index are not index baskets
    for i in range(len(ind.to_pandas_df())):
        basket_type = ind.to_pandas_df()["basket_type"][i]
        assert basket_type != "index"


def test_sync_index_calls_generate_index_if_no_index(test_pantry):
    """Test to make sure that if there isn't a index available then
    generate_index will still be called."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    assert len(ind.to_pandas_df()) == 1


def test_get_index_time_from_path(test_pantry):
    """Tests Index._get_index_time_from_path to ensure it returns the correct
    string."""
    path = "C:/asdf/gsdjls/1234567890-index.json"
    # Testing a protected access var here.
    # pylint: disable-next=protected-access
    time = Index(
        file_system=test_pantry.file_system
    )._get_index_time_from_path(path=path)
    assert time == 1234567890


def test_to_pandas_df(test_pantry):
    """Test that Index.to_pandas_df returns a pandas df of the appropriate
    length"""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    dataframe = ind.to_pandas_df()
    assert len(dataframe) == 1 and isinstance(dataframe, pd.DataFrame)


def test_clean_up_indices_n_not_int(test_pantry):
    """Tests that Index.clean_up_indices errors on a str (should be int)"""
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
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
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
    index_path = os.path.join(test_pantry.pantry_name, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 1


def test_clean_up_indices_with_n_greater_than_num_of_indices(test_pantry):
    """Tests that Index.clean_up_indices behaves well when given a number
    greater than the total number of indices."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
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
    index_path = os.path.join(test_pantry.pantry_name, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 2


def test_is_index_current(test_pantry):
    """Creates two Index objects and pits them against eachother in order to
    ensure that Index.is_index_current is working as expected."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind2.generate_index()
    assert ind2.is_index_current() is True and ind.is_index_current() is False


def test_generate_index(test_pantry):
    """Tests the generation of the Index.pandas_df member variable after a
    basket is uploaded without the Index object knowing."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Assert length of index includes both baskets and excludes the index
    assert len(ind.to_pandas_df()) == 2

    # Assert all baskets in index are not index baskets
    for i in range(len(ind.to_pandas_df())):
        basket_type = ind.to_pandas_df()["basket_type"][i]
        assert basket_type != "index"


def test_delete_basket_deletes_basket(test_pantry):
    """Tests Index.delete_basket to make sure it does, in fact, delete the
    basket."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
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
        f"{test_pantry.pantry_name}/test_basket"
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
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two, uid="0002", parent_ids=["0001"]
    )
    ind = Index(
        pantry_name=test_pantry.pantry_name,
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
    """Setup a valid basket structure, validate the returned index"""

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

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
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
    index = ind.index_df
    parent_answer = index.loc[index["uuid"].isin(parent_ids)]

    # Pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    parent_answer = parent_answer.copy()
    # Add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # Get the results
    results = ind.get_parents(child)

    # Sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # Cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_get_parents_invalid_basket_address(test_pantry):
    """Try and find the parents of an invalid basket path/address"""

    basket_path = "INVALIDpath"

    index = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'",
    ):
        index.get_parents(basket_path)


def test_get_parents_no_parents(test_pantry):
    """Try and get all parents of basket with no parent uuids.

    Check that it returns an empty dataframe/index
    """

    no_parents = test_pantry.set_up_basket("no_parents")
    no_parents_path = test_pantry.upload_basket(
        tmp_basket_dir=no_parents, uid="0001"
    )

    index = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    index.generate_index()

    parent_indeces = index.get_parents(no_parents_path)

    assert parent_indeces.empty


def test_get_parents_parent_is_child(test_pantry):
    """Set up basket structure with parent-child loop, check that it fails

    Set up 3 baskets, child, parent, grandparent, but the grandparent's
    parent_ids has the child's uid. this causes an infinite loop,
    check that it throw error
    """

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

    index = Index(
        pantry_name=test_pantry.pantry_name,
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
    """Setup a valid basket structure, validate the returned dataframe"""

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

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    # Setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.index_df
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # Pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    # Add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j

    # Get the results
    results = ind.get_children(great_grandparent)

    # Sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # Cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)


def test_get_children_invalid_basket_address(test_pantry):
    """Try and find he children of an invalid basket path/address"""

    basket_path = "INVALIDpath"

    index = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'",
    ):
        index.get_children(basket_path)


def test_get_children_no_children(test_pantry):
    """Try and get all children of basket that has no children

    Check that it returns an empty dataframe/index
    """

    no_children = test_pantry.set_up_basket("no_children")
    no_children_path = test_pantry.upload_basket(
        tmp_basket_dir=no_children, uid="0001"
    )

    index = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    index.generate_index()

    children_indexes = index.get_children(no_children_path)

    assert children_indexes.empty


def test_get_children_child_is_parent(test_pantry):
    """Set up a basket structure with a parent-child loop, check that it fails

    Set up 3 baskets, child, parent, grandparent, but the grandparents's
    parent_ids has the child's uid. this causes an infinite loop,
    check that it throw error
    """

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

    index = Index(
        pantry_name=test_pantry.pantry_name,
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

    of a child with a great*15 grandparent, and return all the grandparents
    for the child
    Manually make the data and compare with the result
    """

    parent_id = "x"

    for i in range(15):
        child_id = parent_id
        parent_id = str(i)
        tmp = test_pantry.set_up_basket("basket_" + child_id)
        test_pantry.upload_basket(
            tmp_basket_dir=tmp, uid=child_id, parent_ids=[parent_id]
        )

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()
    index = ind.index_df

    child_path = index.loc[index["uuid"] == "x"]["address"].values[0]

    results = ind.get_parents(child_path)

    # Get the anwser to compare to the test results
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
    # used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(par_ids, par_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    # Format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)

    assert answer.equals(results)


def test_get_children_15_deep(test_pantry):
    """Make a parent-child relationship of baskets 15 deep, get the children.

    For a parent with great*15 grandchildren, return all the grandchildren
    for the highest grandparent.
    Manually make the data and compare with the result
    """

    parent_id = "x"

    for i in range(15):
        child_id = parent_id
        parent_id = str(i)
        tmp = test_pantry.set_up_basket("basket_" + child_id)
        test_pantry.upload_basket(
            tmp_basket_dir=tmp, uid=child_id, parent_ids=[parent_id]
        )

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()
    index = ind.index_df

    parent_path = index.loc[index["uuid"] == "13"]["address"].values[0]

    results = ind.get_children(parent_path)

    # Get the anwser to compare to the test results
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

    # Pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(child_ids, child_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    # Format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)

    assert answer.equals(results)


def test_get_parents_complex_fail(test_pantry):
    """Make a complicated tree with a loop to test new algorithm"""

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
        pantry_name=test_pantry.pantry_name,
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
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 007")
    ):
        ind.get_children(parent_path)


def test_get_parents_from_uuid(test_pantry):
    """Setup a valid basket structure, validate the returned index from uuid"""

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

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
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
    index = ind.index_df
    parent_answer = index.loc[index["uuid"].isin(parent_ids)]

    # Pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    parent_answer = parent_answer.copy()
    # Add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # Get the results
    results = ind.get_parents("0000")

    # Sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # Cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_get_children_from_uuid(test_pantry):
    """Setup a valid basket structure, validate the returned index from uuid"""

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

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    # Setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1, -2, -3]
    index = ind.index_df
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # Pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    # Add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j

    # Get the results with uid of the great grandparent
    results = ind.get_children("3000")

    # Sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    # Cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)


def test_upload_basket_updates_the_index(test_pantry):
    """
    In this test the index already exists with one basket inside of it.
    This test will add another basket using Index.upload_basket, and then check
    to ensure that the index_df has been updated.
    """
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
        sync=True,
    )
    ind.generate_index()

    # Add some baskets
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
    # Put basket in the temporary bucket
    tmp_basket = test_pantry.set_up_basket("basket_one")
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system
    )
    ind.upload_basket(
        upload_items=[{"path": str(tmp_basket.realpath()), "stub": False}],
        basket_type="test",
    )
    assert len(ind.index_df) == 1


@patch.object(uuid, "uuid1")
@patch("weave.upload.UploadBasket.upload_basket_supplement_to_fs")
def test_upload_basket_gracefully_fails(
    mocked_obj_1, mocked_obj_2, test_pantry
):
    """
    In this test an engineered failure to upload the basket occurs.
    Index.upload_basket() should not add anything to the index_df.
    Additionally, the basket in question should be deleted from storage
    (The process fails after only after a partial upload).
    """
    tmp_basket = test_pantry.set_up_basket("basket_one")

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system
    )

    non_unique_id = "0001"
    with pytest.raises(
        ValueError,
        match="This error provided for test_upload_basket_gracefully_fails",
    ):
        mocked_obj_1.side_effect = ValueError(
            "This error provided for test_upload_basket_gracefully_fails"
        )
        mocked_obj_2.return_value.hex = non_unique_id
        ind.upload_basket(
            upload_items=[{"path": str(tmp_basket.realpath()), "stub": False}],
            basket_type="test",
        )

    assert not test_pantry.file_system.exists(
        os.path.join(test_pantry.pantry_name, "test", non_unique_id)
    )


def test_index_get_basket_works_correctly(test_pantry):
    """Test that Index.get_basket() returns a Basket object with correct
    values
    """

    uid = "0001"
    tmp_basket_name = "basket_one"
    tmp_basket_type = "test_basket"
    txt_file_name = "test.txt"

    tmp_basket_dir = test_pantry.set_up_basket(
        tmp_basket_name, file_name=txt_file_name
    )
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir, uid=uid, basket_type=tmp_basket_type
    )

    expected_basket = Basket(
        uid,
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system
    )

    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system
    )
    retrieved_basket = ind.get_basket(uid)

    expected_file_path = os.path.join(
        test_pantry.pantry_name,
        tmp_basket_type,
        uid,
        tmp_basket_name,
        txt_file_name,
    )

    assert retrieved_basket.ls(tmp_basket_name)[0].endswith(expected_file_path)

    assert expected_basket.manifest_path == retrieved_basket.manifest_path
    assert expected_basket.supplement_path == retrieved_basket.supplement_path
    assert expected_basket.metadata_path == retrieved_basket.metadata_path

    assert expected_basket.get_manifest() == retrieved_basket.get_manifest()
    assert (
        expected_basket.get_supplement() == retrieved_basket.get_supplement()
    )
    assert expected_basket.get_metadata() == retrieved_basket.get_metadata()


def test_index_get_basket_graceful_fail(test_pantry):
    """Test Index.get_basket() throws proper ValueErrors with invalid
    inputs.
    """

    bad_uid = "DOESNT EXIST LOL"
    ind = Index(
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system
    )

    with pytest.raises(ValueError, match=f"Basket does not exist: {bad_uid}"):
        ind.get_basket(bad_uid)
