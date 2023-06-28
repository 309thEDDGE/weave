import json
import os
import re
import tempfile
from unittest.mock import patch

from fsspec.implementations.local import LocalFileSystem
import pandas as pd
import pytest

from weave.config import get_file_system
from weave.index import create_index_from_s3
from weave.index import Index
from weave.tests.pytest_resources import BucketForTest
from weave.uploader import upload_basket


class TestCreateIndex:
    """
    A class for to test functions in create_index.py
    """

    def setup_class(self):
        """
        create file locally, upload basket, delete file locally
        """
        self.fs = LocalFileSystem()
        self.basket_type = "test_basket_type"
        self.test_bucket = "index-testing-bucket"

        self.file_system_dir = tempfile.TemporaryDirectory()
        self.file_system_dir_path = self.file_system_dir.name

        self.bucket_path = os.path.join(
            self.file_system_dir_path, self.test_bucket
        )

        # make sure minio bucket doesn't exist. if it does, delete it.
        if self.fs.exists(f"{self.bucket_path}"):
            self.fs.rm(f"{self.bucket_path}", recursive=True)

        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name

        # make something to put in basket
        file_path = os.path.join(self.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write("this is a test file")

    def teardown_class(self):
        """
        remove baskets from s3
        """
        self.fs.rm(f"{self.bucket_path}", recursive=True)
        self.temp_dir.cleanup()
        self.file_system_dir.cleanup()

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_root_dir_is_string(self, patch):
        with pytest.raises(TypeError, match="'root_dir' must be a string"):
            create_index_from_s3(765)

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_correct_index(self, patch):
        # just use the data uploaded and create and
        # index and check that it's right

        # upload basket 2 levels deep
        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/1234",
            "1234",
            self.basket_type,
            ["1111", "2222"],
            label="my label",
        )

        # upload basket 3 levels deep
        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/one_deeper/4321",
            "4321",
            self.basket_type,
            ["333", "444"],
            label="my label",
        )

        truth_index_dict = {
            "uuid": ["1234", "4321"],
            "upload_time": ["1679335295759652", "1234567890"],
            "parent_uuids": [["1111", "2222"], ["333", "444"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": [
                f"{self.bucket_path}/{self.basket_type}/1234",
                f"{self.bucket_path}/{self.basket_type}/one_deeper/4321",
            ],
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)

        minio_index = create_index_from_s3(f"{self.bucket_path}")

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == minio_index)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_create_index_with_wrong_keys(self, patch):
        """
        upload a basket with a basket_details.json with incorrect keys.
        check that correct error is thrown. delete said basket from s3
        """

        # make something to put in basket
        file_path = os.path.join(self.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write("this is another test file")

        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/5678",
            "5678",
            self.basket_type,
            ["3333"],
            label="my label",
        )

        # change a key in this basket_manifest.json
        basket_dict = {}
        with self.fs.open(
            f"{self.bucket_path}/{self.basket_type}/5678/basket_manifest.json",
            "rb",
        ) as f:
            basket_dict = json.load(f)
            basket_dict.pop("uuid")
        basket_path = os.path.join(self.local_dir_path, "basket_manifest.json")
        with open(basket_path, "w") as f:
            json.dump(basket_dict, f)
        self.fs.upload(
            basket_path,
            f"{self.bucket_path}/{self.basket_type}/5678/basket_manifest.json",
        )

        with pytest.raises(ValueError, match="basket found at"):
            create_index_from_s3(f"{self.bucket_path}")

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_root_dir_does_not_exist(self, patch):
        """try to create an index in a bucket that doesn't exist,
        check that it throws an error
        """
        with pytest.raises(
            FileNotFoundError, match="'root_dir' does not exist"
        ):
            create_index_from_s3(
                os.path.join(self.file_system_dir_path, "NOT-A-BUCKET")
            )



"""Pytest Fixtures Documentation:
https://docs.pytest.org/en/7.3.x/how-to/fixtures.html

https://docs.pytest.org/en/7.3.x/how-to
/fixtures.html#teardown-cleanup-aka-fixture-finalization"""

@pytest.fixture
def set_up_tb(tmpdir):
    tb = BucketForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()


def test_sync_index_gets_latest_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    
    # Regenerate index outside of current index object
    ind2 = Index(bucket_name=tb.s3_bucket_name, sync=True)
    ind2.generate_index()

    # assert length of index includes both baskets
    assert len(ind.to_pandas_df()) == 3

def test_sync_index_calls_generate_index_if_no_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    assert len(ind.to_pandas_df()) == 1

def test_get_index_time_from_path():
    path = "C:/asdf/gsdjls/1234567890-index.json"
    time = Index()._get_index_time_from_path(path=path)
    assert time == 1234567890

def test_to_pandas_df(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    df = ind.to_pandas_df()
    assert len(df) == 1 and type(df) is pd.DataFrame

def test_clean_up_indices_n_not_int():
    test_str = "the test"
    with pytest.raises(
        ValueError, match=re.escape(
            "invalid literal for int() with base 10: 'the test'"
        )
    ):
        ind = Index()
        ind.clean_up_indices(n=test_str)

def test_clean_up_indices_leaves_n_indices(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()
    
    # Now there should be two index baskets. clean up all but one of them:
    ind.clean_up_indices(n=1)
    fs = get_file_system()
    index_path = os.path.join(tb.s3_bucket_name, 'index')
    assert len(fs.ls(index_path)) == 1

def test_clean_up_indices_with_n_greater_than_num_of_indices(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    ind.generate_index()
    
    # Now there should be two index baskets. clean up all but three of them:
    # (this should fail, obvs)
    ind.clean_up_indices(n=3)
    fs = get_file_system()
    index_path = os.path.join(tb.s3_bucket_name, 'index')
    assert len(fs.ls(index_path)) == 2

def test_is_index_current(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    ind.to_pandas_df()

    # add another basket
    tmp_basket_dir_two = tb.set_up_basket("basket_two")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    
    # Regenerate index outside of current index object
    ind2 = Index(bucket_name=tb.s3_bucket_name, sync=True)
    ind2.generate_index()
    assert ind2.is_index_current() is True and ind.is_index_current() is False

def test_generate_index(set_up_tb):
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # create index
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
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
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
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
    ind = Index(bucket_name=tb.s3_bucket_name, sync=True)
    with pytest.raises(
        ValueError, match=(
            "The provided value for basket_uuid 0001 is listed as a parent " +
            "UUID for another basket. Please delete that basket before " +
            "deleting it's parent basket."
        )
    ):
        ind.delete_basket(basket_uuid="0001")
