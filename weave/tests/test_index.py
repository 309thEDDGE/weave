import json
import os
import re
import warnings

import pandas as pd
import pytest
import s3fs

from weave.index import create_index_from_fs
from weave.index import Index
from weave.tests.pytest_resources import BucketForTest

"""Pytest Fixtures Documentation:
https://docs.pytest.org/en/7.3.x/how-to/fixtures.html

https://docs.pytest.org/en/7.3.x/how-to
/fixtures.html#teardown-cleanup-aka-fixture-finalization"""

@pytest.fixture
def set_up_tb(tmpdir):
    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )
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

    truth_index_dict = {
        "uuid": ["0001", "0002"],
        "upload_time": ["whatever", "dont matter"],
        "parent_uuids": [[], ["0001"]],
        "basket_type": "test_basket",
        "label": "",
        "address": [addr_one, addr_two],
        "storage_type": "s3",
    }
    truth_index = pd.DataFrame(truth_index_dict)

    minio_index = create_index_from_fs(tb.bucket_name, tb.fs)

    # check that the indexes match, ignoring 'upload_time'
    assert (
        (truth_index == minio_index)
        .drop(columns=["upload_time"])
        .all()
        .all()
    )

@pytest.fixture
def set_up_malformed_baskets(tmpdir):
    """
    upload a basket with a basket_details.json with incorrect keys.
    """
    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )
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
    '''check that the index is made correctly when a malformed basket exists.
    '''
    tb, good_addresses, bad_addresses = set_up_malformed_baskets

    truth_index_dict = {
        "uuid": [f"000{i}" for i in [1,2,4,5,7,8]],
        "upload_time": "whatever",
        "parent_uuids": [[], [], [], [], [], []],
        "basket_type": "test_basket",
        "label": "",
        "address": good_addresses,
        "storage_type": "s3",
    }
    truth_index = pd.DataFrame(truth_index_dict)

    minio_index = create_index_from_fs(tb.bucket_name, tb.fs)
    assert (
        (truth_index == minio_index)
        .drop(columns=["upload_time"])
        .all()
        .all()
    )

def test_create_index_with_bad_basket_throws_warning(set_up_malformed_baskets):
    '''check that a warning is thrown during index creation
    '''
    tb, good_addresses, bad_addresses = set_up_malformed_baskets

    with warnings.catch_warnings(record = True) as w:
        create_index_from_fs(tb.bucket_name, tb.fs)
        message = ('baskets found in the following locations '
                  'do not follow specified weave schema:\n'
                  f'{bad_addresses}')
        assert str(w[0].message) == message

def test_sync_index_gets_latest_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    ind2 = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind2.generate_index()

    # assert length of index includes both baskets
    assert len(ind.to_pandas_df()) == 3

def test_sync_index_calls_generate_index_if_no_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
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

    # create index
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
        ind.clean_up_indices(n=test_str)

def test_clean_up_indices_leaves_n_indices(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Now there should be two index baskets. clean up all but one of them:
    ind.clean_up_indices(n=1)
    index_path = os.path.join(tb.bucket_name, 'index')
    assert len(tb.fs.ls(index_path)) == 1

def test_clean_up_indices_with_n_greater_than_num_of_indices(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # Now there should be two index baskets. clean up all but three of them:
    # (this should fail, obvs)
    ind.clean_up_indices(n=3)
    index_path = os.path.join(tb.bucket_name, 'index')
    assert len(tb.fs.ls(index_path)) == 2

def test_is_index_current(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # add another basket
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

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()

    # assert length of index includes both baskets
    assert len(ind.to_pandas_df()) == 3

def test_delete_basket_deletes_basket(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.bucket_name, file_system=tb.fs, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    ind.generate_index()
    ind.delete_basket(basket_uuid="0002")
    ind.clean_up_indices(n=1)
    ind.generate_index()
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
