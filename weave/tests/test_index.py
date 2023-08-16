import json
import os
import re
import warnings
import uuid
from unittest.mock import patch

import pandas as pd
import numpy as np
import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave import Basket
from weave.index.create_index import create_index_from_fs
from weave.index.index import Index
from weave.tests.pytest_resources import BucketForTest

"""Pytest Fixtures Documentation:
https://docs.pytest.org/en/7.3.x/how-to/fixtures.html

https://docs.pytest.org/en/7.3.x/how-to
/fixtures.html#teardown-cleanup-aka-fixture-finalization

https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize
"""

s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()

# Test with two different fsspec file systems (above).
@pytest.fixture(params=[s3fs, local_fs])
def set_up_tb(request, tmpdir):
    fs = request.param
    tb = BucketForTest(tmpdir, fs)
    yield tb
    tb.cleanup_bucket()

def test_root_dir_does_not_exist(set_up_tb):
    """try to create an index in a bucket that doesn't exist,
    check that it throws an error
    """
    tb = set_up_tb
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    with pytest.raises(
        FileNotFoundError, match="'root_dir' does not exist"
    ):
        create_index_from_fs(
            os.path.join(tmp_basket_dir_one, "NOT-A-BUCKET"),
            tb.fs
        )

def test_root_dir_is_string(set_up_tb):
    tb = set_up_tb
    with pytest.raises(TypeError, match="'root_dir' must be a string"):
        create_index_from_fs(765, tb.fs)

def test_correct_index(set_up_tb):
    tb = set_up_tb
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    addr_one = tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    addr_two = tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002",
                    parent_ids = ['0001'])

    addresses = [addr_one, addr_two]
    truth_index_dict = {
        "uuid": ["0001", "0002"],
        "upload_time": ["whatever", "dont matter"],
        "parent_uuids": [[], ["0001"]],
        "basket_type": "test_basket",
        "label": "",
        "address": addresses,
        "storage_type": tb.fs.__class__.__name__,
    }
    expected_index = pd.DataFrame(truth_index_dict)

    actual_index = create_index_from_fs(tb.bucket_name, tb.fs)

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
        [actual_index['address'].iloc[i].endswith(addr)
         for i, addr in enumerate(addresses)]
    )

# Test with two different fsspec file systems (top of file).
@pytest.fixture(params=[s3fs, local_fs])
def set_up_malformed_baskets(request, tmpdir):
    """
    upload a basket with a basket_details.json with incorrect keys.
    """
    fs = request.param
    tb = BucketForTest(tmpdir, fs)

    good_addresses = []
    bad_addresses= []
    for i in range(10):
        tmp_basket_dir = tb.set_up_basket(f"basket_{i}")
        address = tb.upload_basket(
            tmp_basket_dir=tmp_basket_dir, uid=f"000{i}"
        )

        #change a key in the bad baske_manifests
        if (i % 3) == 0:
            bad_addresses.append(address)

            basket_dict = {}
            manifest_address = (f"{tb.bucket_name}/test_basket/"
                                f"000{i}/basket_manifest.json")

            with tb.fs.open(manifest_address,"rb") as f:
                basket_dict = json.load(f)
                basket_dict.pop("uuid")
            basket_path = os.path.join(tmp_basket_dir, "basket_manifest.json")
            with open(basket_path, "w") as f:
                json.dump(basket_dict, f)

            tb.fs.upload(basket_path,manifest_address)

        else:
            good_addresses.append(address)

    yield tb, good_addresses, bad_addresses
    tb.cleanup_bucket()

def test_create_index_with_malformed_basket_works(set_up_malformed_baskets):
    '''Check that the index is made correctly when a malformed basket exists.
    '''
    tb, good_addresses, bad_addresses = set_up_malformed_baskets

    truth_index_dict = {
        "uuid": [f"000{i}" for i in [1,2,4,5,7,8]],
        "upload_time": "whatever",
        "parent_uuids": [[], [], [], [], [], []],
        "basket_type": "test_basket",
        "label": "",
        "address": good_addresses,
        "storage_type": tb.fs.__class__.__name__,
    }
    expected_index = pd.DataFrame(truth_index_dict)

    # We catch the warnings here, as it will warn for bad baskets, but we don't
    # want the warning to drop through to the pytest log in this test.
    # (Checking the warnings are correct is tested in the next unit test.)
    with warnings.catch_warnings(record = True) as w:
        actual_index = create_index_from_fs(tb.bucket_name, tb.fs)
        message = ('baskets found in the following locations '
                  'do not follow specified weave schema:\n')

        # Check that the indexes match, ignoring 'upload_time', and 'address'
        # (address needs to be checked regardless of FS prefix-see next assert)
        assert (
            (expected_index == actual_index)
            .drop(columns=["upload_time", "address"])
            .all()
            .all()
            and str(w[0].message).startswith(message)
        )

    # Check the addresses are the same, ignoring any FS dependent prefixes.
    assert all(
        [actual_index['address'].iloc[i].endswith(addr)
         for i, addr in enumerate(good_addresses)]
    )

def test_create_index_with_bad_basket_throws_warning(set_up_malformed_baskets):
    '''Check that a warning is thrown during index creation.'''
    tb, good_addresses, bad_addresses = set_up_malformed_baskets

    with warnings.catch_warnings(record = True) as w:
        create_index_from_fs(tb.bucket_name, tb.fs)
        message = ('baskets found in the following locations '
                  'do not follow specified weave schema:')
        # {bad_addresses} would be included in the message, but we can't do a
        # direct string comparison due to FS dependent prefixes.

        warn_msg = str(w[0].message)

        # Check the warning message header/info is correct.
        warn_header_str = warn_msg[:warn_msg.find("\n")]
        assert warn_header_str == message

        # Check the addresses returned in the warning are the ones we expect.
        warning_addrs_str = warn_msg[warn_msg.find("\n")+1:]
        warning_addrs_list = warning_addrs_str.strip("[]") \
                                              .replace("'", '') \
                                              .split(', ')
        assert all(
            [a_addr.endswith(e_addr)
             for a_addr, e_addr in zip(warning_addrs_list, bad_addresses)]
        )

def test_sync_index_gets_latest_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind2.generate_index()

    # assert length of index includes both baskets and does not include the index
    assert len(ind.to_pandas_df()) == 2

def test_sync_index_calls_generate_index_if_no_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    assert len(ind.to_pandas_df()) == 1

def test_get_index_time_from_path(set_up_tb):
    tb = set_up_tb
    path = "C:/asdf/gsdjls/1234567890-index.json"
    time = Index(file_system=tb.fs)._get_index_time_from_path(path=path)
    assert time == 1234567890

def test_to_pandas_df(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    df = ind.to_pandas_df()
    assert len(df) == 1 and type(df) is pd.DataFrame

def test_clean_up_indices_n_not_int(set_up_tb):
    tb = set_up_tb
    test_str = "the test"
    with pytest.raises(
        ValueError, match=re.escape(
            "invalid literal for int() with base 10: 'the test'"
        )
    ):
        ind = Index(file_system=tb.fs)
        ind.clean_up_indices(n_keep=test_str)

def test_clean_up_indices_leaves_n_indices(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Now there should be two index baskets. clean up all but one of them:
    ind.clean_up_indices(n_keep=1)
    index_path = os.path.join(tb.bucket_name, 'index')
    assert len(tb.fs.ls(index_path)) == 1

def test_clean_up_indices_with_n_greater_than_num_of_indices(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Now there should be two index baskets. clean up all but three of them:
    # (this should fail, obvs)
    ind.clean_up_indices(n_keep=3)
    index_path = os.path.join(tb.bucket_name, 'index')
    assert len(tb.fs.ls(index_path)) == 2

def test_is_index_current(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind2.generate_index()
    assert ind2.is_index_current() is True and ind.is_index_current() is False

def test_generate_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Assert length of index includes both baskets and does not include the index
    assert len(ind.to_pandas_df()) == 2

def test_delete_basket_deletes_basket(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    ind.generate_index()
    ind.delete_basket(basket_uuid="0002")

    # fs_baskets: Baskets in the file system
    fs_baskets = tb.fs.ls(f"{tb.bucket_name}/test_basket")
    # index_baskets: Baskets in the index object
    index_baskets = ind.index_df[ind.index_df["basket_type"]=='test_basket']

    # Verify basket removed from the index object
    assert len(index_baskets) == 1
    # Verify index object still tracks the file system
    assert len(fs_baskets) == len(index_baskets)
    # Verify the correct basket was deleted
    assert "0002" not in ind.index_df["uuid"].to_list()

def test_delete_basket_fails_if_basket_is_parent(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two,
                     uid="0002", parent_ids=["0001"])
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    with pytest.raises(
        ValueError, match=(
            "The provided value for basket_uuid 0001 is listed as a parent " +
            "UUID for another basket. Please delete that basket before " +
            "deleting it's parent basket."
        )
    ):
        ind.delete_basket(basket_uuid="0001")

def test_get_parents_valid(set_up_tb):
    """setup a valid basket structure, validate the returned index
    """
    tb = set_up_tb

    #setup random strucutre of parents and children 
    tmp_dir = tb.set_up_basket("great_grandparent_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3000")

    tmp_dir = tb.set_up_basket("great_grandparent_3_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = tb.set_up_basket("great_grandparent_3_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = tb.set_up_basket("great_grandparent_3_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = tb.set_up_basket("grandparent_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="2000",
                     parent_ids=["3000", "3003", "3333"])

    tmp_dir = tb.set_up_basket("grandparent_2_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = tb.set_up_basket("parent_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="1000",
                     parent_ids=["2000", "2002", "3303"])

    tmp_dir = tb.set_up_basket("parent_1_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = tb.set_up_basket("child_0")
    child = tb.upload_basket(tmp_basket_dir=tmp_dir,
                             uid="0000",
                             parent_ids=["1001", "1000"])


    #string to shorten things for ruff
    gen_lvl = "generation_level"

    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    # setup df of the right answer
    parent_ids = [
        "1000", "1001", "2000", "2002", "3303", "3000", "3003", "3333"
    ]
    parent_gens = [1, 1, 2, 2, 2, 3, 3, 3]
    index = ind.index_df
    parent_answer = index.loc[index["uuid"].isin(parent_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    parent_answer = parent_answer.copy()
    #add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # get the results
    results = ind.get_parents(child)

    # sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    #cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_get_parents_invalid_basket_address(set_up_tb):
    """try and find the parents of an invalid basket path/address"""
    tb = set_up_tb

    basket_path = "INVALIDpath"

    index = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)

    with pytest.raises(
        FileNotFoundError, 
        match=f"basket path or uuid does not exist '{basket_path}'"
    ):
        index.get_parents(basket_path)


def test_get_parents_no_parents(set_up_tb):
    """try and get all parents of basket with no parent uuids.

    check that it returns an empty dataframe/index
    """
    tb = set_up_tb

    no_parents = tb.set_up_basket("no_parents")
    no_parents_path = tb.upload_basket(tmp_basket_dir=no_parents, uid="0001")

    index = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    index.generate_index()

    parent_indeces = index.get_parents(no_parents_path)

    assert parent_indeces.empty


def test_get_parents_parent_is_child(set_up_tb):
    """set up basket structure with parent-child loop, check that it fails

    set up 3 baskets, child, parent, grandparent, but the grandparent's
    parent_ids has the child's uid. this causes an infinite loop,
    check that it throw error
    """
    tb = set_up_tb

    # create a basket structure with child, parent, and grandparent, but
    # the grandparent's parent, is the child, making an loop for the
    # parent-child relationship
    tmp_dir = tb.set_up_basket("grandparent")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="3000",
                     parent_ids=["1000"])

    tmp_dir = tb.set_up_basket("parent")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="2000",
                     parent_ids=["3000"])

    tmp_dir = tb.set_up_basket("child")
    child = tb.upload_basket(tmp_basket_dir=tmp_dir,
                             uid="1000",
                             parent_ids=["2000"])

    index = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    index.generate_index()

    fail = '1000'

    with pytest.raises(
        ValueError, match=f"Parent-Child loop found at uuid: {fail}"
    ):
        index.get_parents(child)


def test_get_children_valid(set_up_tb):
    """setup a valid basket structure, validate the returned dataframe
    """
    tb = set_up_tb

    #setup random strucutre of parents and children 
    tmp_dir = tb.set_up_basket("great_grandparent_3")
    great_grandparent = tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3000")

    tmp_dir = tb.set_up_basket("great_grandparent_3_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = tb.set_up_basket("great_grandparent_3_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = tb.set_up_basket("great_grandparent_3_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = tb.set_up_basket("grandparent_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="2000",
                     parent_ids=["3000", "3003", "3333"])

    tmp_dir = tb.set_up_basket("grandparent_2_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = tb.set_up_basket("parent_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="1000",
                     parent_ids=["2000", "2002", "3303"])

    tmp_dir = tb.set_up_basket("parent_1_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = tb.set_up_basket("child_0")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="0000",
                     parent_ids=["1001", "1000"])

    #string to shorten things for ruff
    gen_lvl = "generation_level"

    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    # setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1,-2,-3]
    index = ind.index_df
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    #add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j


    # get the results
    results = ind.get_children(great_grandparent)

    # sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    #cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)


def test_get_children_invalid_basket_address(set_up_tb):
    """try and find he children of an invalid basket path/address"""
    tb = set_up_tb

    basket_path = "INVALIDpath"

    index = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)

    with pytest.raises(
        FileNotFoundError,
        match=f"basket path or uuid does not exist '{basket_path}'"
    ):
        index.get_children(basket_path)


def test_get_children_no_children(set_up_tb):
    """try and get all children of basket that has no children

    check that it returns an empty dataframe/index
    """
    tb = set_up_tb

    no_children = tb.set_up_basket("no_children")
    no_children_path = tb.upload_basket(tmp_basket_dir=no_children, uid="0001")

    index = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    index.generate_index()

    children_indexes = index.get_children(no_children_path)

    assert children_indexes.empty


def test_get_children_child_is_parent(set_up_tb):
    """set up a basket structure with a parent-child loop, check that it fails

    set up 3 baskets, child, parent, grandparent, but the grandparents's
    parent_ids has the child's uid. this causes an infinite loop,
    check that it throw error
    """
    tb = set_up_tb

    # create a basket structure with child, parent, and grandparent, but
    # the grandparent's parent, is the child, making an loop for the
    # parent-child relationship
    tmp_dir = tb.set_up_basket("grandparent")
    gp = tb.upload_basket(tmp_basket_dir=tmp_dir,
                          uid="3000",
                          parent_ids=["1000"])

    tmp_dir = tb.set_up_basket("parent")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="2000",
                     parent_ids=["3000"])

    tmp_dir = tb.set_up_basket("child")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="1000",
                     parent_ids=["2000"])

    index = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    index.generate_index()

    fail = '3000'

    with pytest.raises(
        ValueError, match=re.escape(f"Parent-Child loop found at uuid: {fail}")
    ):
        index.get_children(gp)


def test_get_parents_15_deep(set_up_tb):
    """Make a parent-child relationship of baskets 15 deep, get all the parents

    so a child with a great*15 grandparent, and return all the grandparents
    for the child
    manually make the data and compare with the result
    """
    tb = set_up_tb

    parent_id = "x"

    for i in range(15):
        child_id = parent_id
        parent_id = str(i)
        tmp = tb.set_up_basket("basket_" + child_id)
        tb.upload_basket(tmp_basket_dir=tmp,
                         uid=child_id, 
                         parent_ids=[parent_id])

    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()
    index = ind.index_df

    child_path = index.loc[index["uuid"] == 'x']["address"].values[0]

    results = ind.get_parents(child_path)

    # Get the anwser to compare to the results we got
    par_ids = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13']
    par_gens = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    answer = index.loc[index["uuid"].isin(par_ids)]

    gen_lvl = "generation_level"

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(par_ids, par_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    #format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)

    assert answer.equals(results)


def test_get_children_15_deep(set_up_tb):
    """Make a parent-child relationship of baskets 15 deep, get the children.

    so a child with great*15 grandparent, and return all the grandchildren
    for the highest grandparent.
    manually make the data and compare with the result
    """
    tb = set_up_tb

    parent_id = "x"

    for i in range(15):
        child_id = parent_id
        parent_id = str(i)
        tmp = tb.set_up_basket("basket_" + child_id)
        tb.upload_basket(
            tmp_basket_dir=tmp,
            uid=child_id,
            parent_ids=[parent_id])

    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()
    index = ind.index_df

    parent_path = index.loc[index["uuid"] == '13']["address"].values[0]

    results = ind.get_children(parent_path)

    # Get the anwser to compare to the results we got
    child_ids = ['x','0','1','2','3','4','5','6','7','8','9','10','11','12']
    child_gens = [-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1]
    answer = index.loc[index["uuid"].isin(child_ids)]

    gen_lvl = "generation_level"

    #pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    answer = answer.copy()
    for i, j in zip(child_ids, child_gens):
        answer.loc[answer["uuid"] == i, gen_lvl] = j

    #format and sort so .equals can be properly used
    answer = answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")
    answer[gen_lvl] = answer[gen_lvl].astype(np.int64)

    assert answer.equals(results)


def test_get_parents_complex_fail(set_up_tb):
    """Make a complicated tree with a loop to test new algorithm
    """
    tb = set_up_tb

    tmp_dir = tb.set_up_basket("parent_8")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="008", parent_ids=["007"])

    tmp_dir = tb.set_up_basket("parent_7")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="007",
                     parent_ids=["000"])

    tmp_dir = tb.set_up_basket("parent_6")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="006", parent_ids=["008"])

    tmp_dir = tb.set_up_basket("parent_5")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="005", parent_ids=["007"])

    tmp_dir = tb.set_up_basket("parent_4")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="004")

    tmp_dir = tb.set_up_basket("parent_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="003", parent_ids=["006"])

    tmp_dir = tb.set_up_basket("parent_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="002",
                     parent_ids=["0004", "005", "008"])

    tmp_dir = tb.set_up_basket("parent_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="001",
                     parent_ids=["004"])

    tmp_dir = tb.set_up_basket("child")
    child_path = tb.upload_basket(tmp_basket_dir=tmp_dir,
                                  uid="000",
                                  parent_ids=["001", "002", "003"])


    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 000")
    ):
        ind.get_parents(child_path)


def test_get_children_complex_fail(set_up_tb):
    """Make a complicated tree with a loop to test new algorithm
    """
    tb = set_up_tb

    tmp_dir = tb.set_up_basket("parent_8")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="008", parent_ids=["007"])

    tmp_dir = tb.set_up_basket("parent_7")
    parent_path = tb.upload_basket(tmp_basket_dir=tmp_dir,
                                   uid="007",
                                   parent_ids=["003"])

    tmp_dir = tb.set_up_basket("parent_6")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="006", parent_ids=["008"])

    tmp_dir = tb.set_up_basket("parent_5")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="005", parent_ids=["007"])

    tmp_dir = tb.set_up_basket("parent_4")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="004")

    tmp_dir = tb.set_up_basket("parent_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="003", parent_ids=["006"])

    tmp_dir = tb.set_up_basket("parent_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="002",
                     parent_ids=["004", "005", "008"])

    tmp_dir = tb.set_up_basket("parent_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="001", parent_ids=["004"])

    tmp_dir = tb.set_up_basket("child")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="000",
                     parent_ids=["001", "002", "003"])


    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    with pytest.raises(
        ValueError, match=re.escape("Parent-Child loop found at uuid: 007")
    ):
        ind.get_children(parent_path)


def test_get_parents_from_uuid(set_up_tb):
    """setup a valid basket structure, validate the returned index from uuid
    """
    tb = set_up_tb

    #setup random strucutre of parents and children 
    tmp_dir = tb.set_up_basket("great_grandparent_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3000")

    tmp_dir = tb.set_up_basket("great_grandparent_3_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = tb.set_up_basket("great_grandparent_3_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = tb.set_up_basket("great_grandparent_3_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = tb.set_up_basket("grandparent_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="2000",
                     parent_ids=["3000", "3003", "3333"])

    tmp_dir = tb.set_up_basket("grandparent_2_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = tb.set_up_basket("parent_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="1000",
                     parent_ids=["2000", "2002", "3303"])

    tmp_dir = tb.set_up_basket("parent_1_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = tb.set_up_basket("child_0")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="0000",
                     parent_ids=["1001", "1000"])


    #string to shorten things for ruff
    gen_lvl = "generation_level"

    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    # setup df of the right answer
    parent_ids = [
        "1000", "1001", "2000", "2002", "3303", "3000", "3003", "3333"
    ]
    parent_gens = [1, 1, 2, 2, 2, 3, 3, 3]
    index = ind.index_df
    parent_answer = index.loc[index["uuid"].isin(parent_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    parent_answer = parent_answer.copy()
    #add the generation levels
    for i, j in zip(parent_ids, parent_gens):
        parent_answer.loc[parent_answer["uuid"] == i, gen_lvl] = j

    # get the results
    results = ind.get_parents('0000')

    # sort so that they can be properly compared to
    parent_answer = parent_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    #cast to int64 so datatypes match
    parent_answer[gen_lvl] = parent_answer[gen_lvl].astype(np.int64)

    assert parent_answer.equals(results)


def test_get_children_from_uuid(set_up_tb):
    """setup a valid basket structure, validate the returned index from uuid
    """
    tb = set_up_tb

    #setup random strucutre of parents and children 
    tmp_dir = tb.set_up_basket("great_grandparent_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3000")

    tmp_dir = tb.set_up_basket("great_grandparent_3_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3003")

    tmp_dir = tb.set_up_basket("great_grandparent_3_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3333")

    tmp_dir = tb.set_up_basket("great_grandparent_3_3")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="3303")

    tmp_dir = tb.set_up_basket("grandparent_2")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="2000",
                     parent_ids=["3000", "3003", "3333"])

    tmp_dir = tb.set_up_basket("grandparent_2_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="2002")

    tmp_dir = tb.set_up_basket("parent_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="1000",
                     parent_ids=["2000", "2002", "3303"])

    tmp_dir = tb.set_up_basket("parent_1_1")
    tb.upload_basket(tmp_basket_dir=tmp_dir, uid="1001")

    tmp_dir = tb.set_up_basket("child_0")
    tb.upload_basket(tmp_basket_dir=tmp_dir,
                     uid="0000",
                     parent_ids=["1001", "1000"])

    #string to shorten things for ruff
    gen_lvl = "generation_level"

    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    # setup df of the right answer
    child_ids = ["2000", "1000", "0000"]
    child_gens = [-1,-2,-3]
    index = ind.index_df
    child_answer = index.loc[index["uuid"].isin(child_ids)]

    # pandas wants to make a copy before adding a column
    # used to remove warning in pytest
    child_answer = child_answer.copy()
    #add the generation levels
    for i, j in zip(child_ids, child_gens):
        child_answer.loc[child_answer["uuid"] == i, gen_lvl] = j


    # get the results with uid of the great grandparent
    results = ind.get_children('3000')

    # sort so that they can be properly compared to
    child_answer = child_answer.sort_values(by="uuid")
    results = results.sort_values(by="uuid")

    #cast to int64 so datatypes match
    child_answer[gen_lvl] = child_answer[gen_lvl].astype(np.int64)

    assert child_answer.equals(results)

def test_upload_basket_updates_the_index(set_up_tb):
    """
    In this test the index already exists with one basket inside of it.
    This test will add another basket using Index.upload_basket, and then check
    to ensure that the index_df has been updated.
    """
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.generate_index()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    ind.upload_basket(upload_items=[{'path':str(tmp_basket_dir_two.realpath()),
                                     'stub':False}],
                      basket_type="test")
    assert(len(ind.index_df) == 2)

def test_upload_basket_works_on_empty_basket(set_up_tb):
    """
    In this test the Index object will upload a basket to a pantry that does
    not have any baskets yet. This test will make sure that this functionality
    is present, and that the index_df has been updated.
    """
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket = tb.set_up_basket("basket_one")
    ind = Index(tb.bucket_name, file_system=tb.fs)
    ind.upload_basket(upload_items=[{'path':str(tmp_basket.realpath()),
                                     'stub':False}],
                      basket_type="test")
    assert(len(ind.index_df) == 1)

@patch.object(uuid, 'uuid1')
@patch('weave.upload.UploadBasket.upload_basket_supplement_to_fs')
def test_upload_basket_gracefully_fails(mocked_obj_1, mocked_obj_2, set_up_tb):
    """
    In this test an engineered failure to upload the basket occurs.
    Index.upload_basket() should not add anything to the index_df.
    Additionally, the basket in question should be deleted from storage (I will
    make the process fail only after a partial upload).
    """
    tb = set_up_tb
    tmp_basket = tb.set_up_basket("basket_one")

    ind = Index(tb.bucket_name, file_system=tb.fs)

    non_unique_id = "0001"
    with pytest.raises(
        ValueError,
        match="This error provided for test_upload_basket_gracefully_fails"
    ):
        mocked_obj_1.side_effect = ValueError(
            "This error provided for test_upload_basket_gracefully_fails"
        )
        mocked_obj_2.return_value.hex = non_unique_id
        ind.upload_basket(upload_items=[{'path':str(tmp_basket.realpath()),
                                         'stub':False}],
                          basket_type="test")

    assert not tb.fs.exists(
        os.path.join(tb.bucket_name, "test", non_unique_id)
    )

def test_index_get_basket_works_correctly(set_up_tb):
    """Test that Index.get_basket() returns a Basket object with correct values
    """
    tb = set_up_tb

    uid = "0001"
    tmp_basket_name = "basket_one"
    tmp_basket_type = "test_basket"
    txt_file_name = "test.txt"

    tmp_basket_dir = tb.set_up_basket(tmp_basket_name, file_name=txt_file_name)
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir,
                     uid=uid,
                     basket_type=tmp_basket_type)

    expected_basket = Basket(uid, tb.bucket_name, file_system=tb.fs)

    ind = Index(tb.bucket_name, file_system=tb.fs)
    retrieved_basket = ind.get_basket(uid)

    expected_file_path = os.path.join(tb.bucket_name, tmp_basket_type, uid,
                                      tmp_basket_name, txt_file_name)

    assert (
        retrieved_basket.ls(tmp_basket_name)[0].endswith(expected_file_path)
    )

    assert expected_basket.manifest_path == retrieved_basket.manifest_path
    assert expected_basket.supplement_path == retrieved_basket.supplement_path
    assert expected_basket.metadata_path == retrieved_basket.metadata_path

    assert expected_basket.get_manifest() == retrieved_basket.get_manifest()
    assert (
        expected_basket.get_supplement() == retrieved_basket.get_supplement()
    )
    assert expected_basket.get_metadata() == retrieved_basket.get_metadata()

def test_index_get_basket_graceful_fail(set_up_tb):
    """Test Index.get_basket() throws proper ValueErrors with invalid inputs.
    """
    tb = set_up_tb

    bad_uid = "DOESNT EXIST LOL"
    ind = Index(tb.bucket_name, file_system=tb.fs)

    with pytest.raises(
        ValueError,
        match=f"Basket does not exist: {bad_uid}"
    ):
        ind.get_basket(bad_uid)