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
from weave.index.index_pandas import PandasIndex
from weave.pantry import Pantry
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
# long. I don't necessarily think that is bad in this case, as the alternative
# would be to write the tests continuuing in a different script, which I think
# is unnecesarily complex. Therefor, I am disabling this warning for this
# script.
# pylint: disable=too-many-lines

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


def test_root_dir_does_not_exist(test_pantry):
    """try to create an index in a bucket that doesn't exist,
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
        test_pantry.pantry_path, test_pantry.file_system
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

        # change a key in the bad basket_manifests
        if (i % 3) == 0:
            bad_addresses.append(address)

            basket_dict = {}
            manifest_address = (
                f"{test_pantry.pantry_path}/test_basket/"
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

    # We catch the warnings here, as it will warn for bad baskets, but we don't
    # want the warning to drop through to the pytest log in this test.
    # (Checking the warnings are correct is tested in the next unit test.)
    with warnings.catch_warnings(record=True) as warn:
        actual_index = create_index_from_fs(
            test_pantry.pantry_path, test_pantry.file_system
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
        create_index_from_fs(test_pantry.pantry_path, test_pantry.file_system)
        message = (
            "baskets found in the following locations "
            "do not follow specified weave schema:"
        )
        # {bad_addresses} would be included in the message, but we can't do a
        # direct string comparison due to FS dependent prefixes.

        warn_msg = str(warn[0].message)

        # Check the warning message header/info is correct.
        warn_header_str = warn_msg[: warn_msg.find("\n")]
        assert warn_header_str == message

        # Check the addresses returned in the warning are the ones we expect.
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


def test_pantry_fails_with_bad_path(test_pantry):
    """Tests Index.delete_basket to make sure it does, in fact, delete the
    basket."""
    bad_path = 'BadPath'
    error_msg = f"Invalid pantry Path. Pantry does not exist at: {bad_path}"
    with pytest.raises(ValueError, match=error_msg):
        pantry = Pantry(PandasIndex,
                        pantry_path="BadPath",
                        file_system=test_pantry.file_system)


def test_delete_basket_deletes_basket(test_pantry):
    """Tests Index.delete_basket to make sure it does, in fact, delete the
    basket."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    pantry = Pantry(PandasIndex,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    pantry.index.generate_index()
    pantry.delete_basket(basket_address="0002")

    # fs_baskets: Baskets in the file system
    fs_baskets = test_pantry.file_system.ls(
        os.path.join(test_pantry.pantry_path,"test_basket")
    )

    # Verify pantry object still tracks the file system
    assert len(fs_baskets) == 1
    # Verify pantry index updated
    assert len(pantry.index) == 1
    # Verify the correct basket was deleted from filesystem
    check_path = os.path.join(test_pantry.pantry_path,"test_basket","0002")
    assert check_path not in fs_baskets
    # Verify the correct basket was deleted from the Index
    error_msg = "The provided value for basket_uuid 0002 does not exist."
    with pytest.raises(ValueError, match=error_msg):
        pantry.index.get_basket("0002")


def test_pantry_delete_basket_with_parents(test_pantry):
    """Tests Index.delete_basket to make sure it does, in fact, delete the
    basket."""
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")
    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(
        tmp_basket_dir=tmp_basket_dir_two, uid="0002", parent_ids=["0001"]
    )

    pantry = Pantry(PandasIndex,
                pantry_path=test_pantry.pantry_path,
                file_system=test_pantry.file_system)
    pantry.index.generate_index()

    error_msg = ("The provided value for basket_uuid 0001 is listed as a "
                 "parent UUID for another basket. Please delete that basket "
                 "before deleting it's parent basket.")
    with pytest.raises(ValueError, match=error_msg):
        pantry.delete_basket(basket_address="0001")


def test_upload_basket_updates_the_pantry(test_pantry):
    """
    In this test the index already exists with one basket inside of it.
    This test will add another basket using Index.upload_basket, and then check
    to ensure that the index_df has been updated.
    """
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(PandasIndex,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()

    # add some baskets
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    for i in range(3):
        new_basket = pantry.upload_basket(
            upload_items=[
                {"path": str(tmp_basket_dir_two.realpath()), "stub": False}
            ],
            basket_type="test_basket",
        )
        if i == 0:
            first_time = pd.to_datetime(
                            pantry.index.index_df.iloc[1].upload_time
            )
    time_diff = first_time - pd.to_datetime(
                                    pantry.index.index_df.iloc[1].upload_time
    )

    # Make sure the index row hasn't changed
    assert all(pantry.index.index_df.iloc[-1] == new_basket.iloc[0])

    # fs_baskets: Baskets in the file system
    fs_baskets = test_pantry.file_system.ls(
        os.path.join(test_pantry.pantry_path,"test_basket")
    )
    # Ensure all baskets are in index and file_system
    assert len(pantry.index) == 4
    assert len(fs_baskets) == 4


@patch.object(uuid, "uuid1")
@patch("weave.upload.UploadBasket.upload_basket_supplement_to_fs")
def test_upload_basket_gracefully_fails(
    mocked_obj_1, mocked_obj_2, test_pantry
):
    """
    In this test an engineered failure to upload the basket occurs.
    Index.upload_basket() should not add anything to the index_df.
    Additionally, the basket in question should be deleted from storage (I will
    make the process fail only after a partial upload).
    """
    tmp_basket = test_pantry.set_up_basket("basket_one")

    pantry = Pantry(PandasIndex,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system
    )

    # TODO: This part should go in Index testing too
    non_unique_id = "0001"
    with pytest.raises(
        ValueError,
        match="This error provided for test_upload_basket_gracefully_fails",
    ):
        mocked_obj_1.side_effect = ValueError(
            "This error provided for test_upload_basket_gracefully_fails"
        )
        mocked_obj_2.return_value.hex = non_unique_id
        pantry.upload_basket(
            upload_items=[{"path": str(tmp_basket.realpath()), "stub": False}],
            basket_type="test",
        )

    assert not test_pantry.file_system.exists(
        os.path.join(test_pantry.pantry_path, "test", non_unique_id)
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

    pantry = Pantry(PandasIndex,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system
    )
    pantry.index.generate_index()
    retrieved_basket = pantry.get_basket(uid)

    expected_basket = Basket(
        uid,
        pantry=pantry,
    )

    expected_file_path = os.path.join(
        test_pantry.pantry_path,
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
    pantry = Pantry(PandasIndex,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system
    )

    with pytest.raises(ValueError, match=f"Basket does not exist: {bad_uid}"):
        pantry.get_basket(bad_uid)


def test_pantry_get_metadata_no_data(test_pantry):
    """Test the pantry reads in empty metadata and the index metadata."""
    pantry = Pantry(PandasIndex,
                pantry_path=test_pantry.pantry_path,
                file_system=test_pantry.file_system
    )

    assert len(pantry.metadata) == 1
    assert 'index_metadata' in pantry.metadata


def test_pantry_save_metadata(test_pantry):
    """Test Pantry can save metadata correctly."""
    pantry = Pantry(PandasIndex,
                pantry_path=test_pantry.pantry_path,
                file_system=test_pantry.file_system
    )
    pantry.save_metadata()

    if pantry.file_system.exists(pantry.metadata_path):
        with pantry.file_system.open(pantry.metadata_path, "rb") as file:
            file_metadata = json.load(file)

    assert pantry.file_system.exists(pantry.metadata_path)
    assert pantry.metadata == file_metadata


def test_pantry_get_metadata_existing_data(test_pantry):
    """Test the Pantry and Index can load in existing metadata."""
    pantry = Pantry(PandasIndex,
                pantry_path=test_pantry.pantry_path,
                file_system=test_pantry.file_system
    )
    pantry.metadata['test'] = 'test'
    pantry.metadata['index_metadata']['test'] = 'test'
    pantry.save_metadata()

    pantry2 = Pantry(PandasIndex,
                pantry_path=test_pantry.pantry_path,
                file_system=test_pantry.file_system
    )

    assert pantry2.metadata['test'] == 'test'
    assert pantry2.index.metadata['test'] == 'test'
