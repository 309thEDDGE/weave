import pytest
import tempfile
import os
import json
import pandas as pd
from weave.index import Index, create_index_from_s3
from fsspec.implementations.local import LocalFileSystem
from unittest.mock import patch

from weave.uploader import upload_basket


class TestIndex:
    """
    A class for to test functions in index.py
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
        self.index_path = os.path.join(self.bucket_path, 'index', 'index.json')




    def teardown_class(self):
        """remove baskets from s3"""
        self.file_system_dir.cleanup()

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def upload_basket_for_setup(self, patch):
        # make sure minio bucket doesn't exist. if it does, delete it.
        if self.fs.exists(f"{self.bucket_path}"):
            self.fs.rm(f"{self.bucket_path}", recursive=True)

        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name

        # make something to put in basket
        file_path = os.path.join(self.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write("this is a test file")

        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/1234",
            "1234",
            self.basket_type,
            ["1111", "2222"],
            label="my label",
        )

    def setup_method(self, patch):
        self.upload_basket_for_setup()

    def teardown_method(self):
        self.fs.rm(f"{self.bucket_path}", recursive=True)
        self.temp_dir.cleanup()

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_create_index_root_dir_is_string(self, patch):
        with pytest.raises(TypeError, match="'root_dir' must be a string"):
            create_index_from_s3(765, self.fs)

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_create_index_correct_index(self, patch):
        # just use the data uploaded and create and
        # index and check that it's right

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

        minio_index = create_index_from_s3(f"{self.bucket_path}", self.fs)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == minio_index)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_create_index_with_wrong_keys(self, patch):
        """upload a basket with a basket_details.json with incorrect keys.
        check that correct error is thrown. delete said basket from s3
        """

        # change a key in this basket_manifest.json
        basket_dict = {}
        with self.fs.open(
            f"{self.bucket_path}/{self.basket_type}/1234/basket_manifest.json",
            "rb",
        ) as f:
            basket_dict = json.load(f)
            basket_dict.pop("uuid")
        basket_path = os.path.join(self.local_dir_path, "basket_manifest.json")
        with open(basket_path, "w") as f:
            json.dump(basket_dict, f)
        self.fs.upload(
            basket_path,
            f"{self.bucket_path}/{self.basket_type}/1234/basket_manifest.json",
        )

        with pytest.raises(ValueError, match="basket found at"):
            create_index_from_s3(f"{self.bucket_path}", self.fs)

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_create_index_root_dir_does_not_exist(self, patch):
        """try to create an index in a bucket that doesn't exist,
        check that it throws an error
        """
        with pytest.raises(
            FileNotFoundError, match="'root_dir' does not exist"
        ):
            create_index_from_s3(
                os.path.join(self.file_system_dir_path, "NOT-A-BUCKET"),
                self.fs
            )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_index_bucket_name_is_pathlike(self, patch):
        bucket_path = 27
        with pytest.raises(
            TypeError,
            match="expected str, bytes or os.PathLike object, not int",
        ):
            Index(bucket_path)

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_index_bucket_name_exists(self, patch):
        bucket_path = 'Not A CORRECT path'
        with pytest.raises(
            ValueError,
            match=f"Specified bucket does not exist: {bucket_path}",
        ):
            Index(bucket_path)

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_index_df_is_none_with_no_existing_index_file(self, patch):
        '''when index.json does not exist remotely, index_df should be None'''
        my_index = Index(self.bucket_path)
        assert my_index.index_df == None

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_index_df_exists_with_existing_index_file(self, patch):
        '''When index.json does exist remotely, index_df is created'''
        truth_index_dict = {
            "uuid": "1234",
            "upload_time": "1679335295759652",
            "parent_uuids": [["1111", "2222"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": f"{self.bucket_path}/{self.basket_type}/1234",
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)
        self.fs.mkdir(os.path.join(self.bucket_path, 'index'))
        truth_index.to_json(self.index_path)

        my_index = Index(self.bucket_path)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == my_index.index_df)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_update_index_creates_remote_index_json(self, patch):
        '''run update index, check remote file is correct'''
        my_index = Index(self.bucket_path)
        my_index.update_index()

        minio_index = pd.read_json(
            self.fs.open(self.index_path),
            dtype = {'uuid': str}
        )

        truth_index_dict = {
            "uuid": "1234",
            "upload_time": "1679335295759652",
            "parent_uuids": [["1111", "2222"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": f"{self.bucket_path}/{self.basket_type}/1234",
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == minio_index)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_update_index_updates_remote_index_json(self, patch):
        '''run update index when a remote index already exists'''
        my_index = Index(self.bucket_path)
        my_index.update_index()

        #upload another basket and update the index again
        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/4321",
            "4321",
            self.basket_type,
            ["333", "444"],
            label="my label",
        )

        my_index.update_index()

        minio_index = pd.read_json(
            self.fs.open(self.index_path),
            dtype = {'uuid': str}
        )

        truth_index_dict = {
            "uuid": ["1234", "4321"],
            "upload_time": ["1679335295759652", "1234567890"],
            "parent_uuids": [["1111", "2222"], ["333", "444"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": [
                f"{self.bucket_path}/{self.basket_type}/1234",
                f"{self.bucket_path}/{self.basket_type}/4321",
            ],
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)


        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == minio_index)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_update_index_creates_local_index(self, patch):
        '''check that update_index creates index_df'''
        my_index = Index(self.bucket_path)
        my_index.update_index()

        truth_index_dict = {
            "uuid": "1234",
            "upload_time": "1679335295759652",
            "parent_uuids": [["1111", "2222"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": f"{self.bucket_path}/{self.basket_type}/1234",
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == my_index.index_df)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_update_index_updates_local_index(self, patch):
        '''check that update_index updates local index_df'''
        my_index = Index(self.bucket_path)
        my_index.update_index()

        #upload another basket and update the index again
        upload_basket(
            [{"path": self.local_dir_path, "stub": False}],
            f"{self.bucket_path}/{self.basket_type}/4321",
            "4321",
            self.basket_type,
            ["333", "444"],
            label="my label",
        )

        my_index.update_index()

        truth_index_dict = {
            "uuid": ["1234", "4321"],
            "upload_time": ["1679335295759652", "1234567890"],
            "parent_uuids": [["1111", "2222"], ["333", "444"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": [
                f"{self.bucket_path}/{self.basket_type}/1234",
                f"{self.bucket_path}/{self.basket_type}/4321",
            ],
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == my_index.index_df)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_get_index_from_local(self, patch):
        '''check that get_index returns local index_df when it exists'''
        my_index = Index(self.bucket_path)
        my_index.update_index()

        index_df = my_index.get_index()

        truth_index_dict = {
            "uuid": "1234",
            "upload_time": "1679335295759652",
            "parent_uuids": [["1111", "2222"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": f"{self.bucket_path}/{self.basket_type}/1234",
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == index_df)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_get_index_from_remote(self, patch):
        '''if no remote index, get_index should create one and return it'''
        my_index = Index(self.bucket_path)
        index_df = my_index.get_index()

        truth_index_dict = {
            "uuid": "1234",
            "upload_time": "1679335295759652",
            "parent_uuids": [["1111", "2222"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": f"{self.bucket_path}/{self.basket_type}/1234",
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == index_df)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )


#test sync index
    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_sync_index_updates_local_df(self, patch):
        my_index = Index(self.bucket_path)
        my_index.update_index()

        # make_index_out_of_date
        truth_index_dict = {
            "uuid": ["1234", "4321"],
            "upload_time": ["1679335295759652", "1234567890"],
            "parent_uuids": [["1111", "2222"], ["333", "444"]],
            "basket_type": "test_basket_type",
            "label": "my label",
            "address": [
                f"{self.bucket_path}/{self.basket_type}/1234",
                f"{self.bucket_path}/{self.basket_type}/4321",
            ],
            "storage_type": "s3",
        }
        truth_index = pd.DataFrame(truth_index_dict)
        truth_index.to_json(self.index_path)

        my_index.sync_index()

        print(
            (truth_index == my_index.index_df)
            .drop(columns=["upload_time"])
        )

        # check that the indexes match, ignoring 'upload_time'
        assert (
            (truth_index == my_index.index_df)
            .drop(columns=["upload_time"])
            .all()
            .all()
        )
