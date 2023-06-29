import os
import tempfile
import pytest
import uuid
from fsspec.implementations.local import LocalFileSystem
from unittest.mock import patch

from weave import upload


class TestUpload:
    def setup_class(self):
        self.fs = LocalFileSystem()
        self.basket_type = "test_basket_type"
        self.bucket_name = "weave-test-bucket"

        self.file_system_dir = tempfile.TemporaryDirectory()
        self.file_system_dir_path = self.file_system_dir.name

        # make sure minio bucket doesn't exist. if it does, delete it.
        if self.fs.exists(f"{self.bucket_name}"):
            self.fs.rm(f"{self.bucket_name}", recursive=True)

        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name
        self.data_file_path = os.path.join(self.local_dir_path, "test.txt")
        with open(self.data_file_path, "w") as f:
            f.write("0123456789")

        self.upload_items = [{"path": self.data_file_path, "stub": False}]

    def teardown_class(self):
        self.temp_dir.cleanup()
        if self.fs.exists(f"{self.bucket_name}"):
            self.fs.rm(f"{self.bucket_name}", recursive=True)

        self.file_system_dir.cleanup()

    @pytest.fixture
    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def run_uploader(self, patch):
        parent_ids = [uuid.uuid1().hex]
        metadata = {"oh": "i don't know", "something": "stupid"}
        label = "my label"
        self.upload_path = upload(
            self.upload_items,
            self.basket_type,
            self.bucket_name,
            parent_ids,
            metadata,
            label,
            test_prefix=self.file_system_dir_path,
        )

        uploaded_files = self.fs.ls(self.upload_path)
        return uploaded_files

    def test_upload_test_txt_in_uploaded_files(self, run_uploader):
        assert os.path.join(self.upload_path, "test.txt") in run_uploader

    def test_upload_basket_manifest_in_uploaded_files(self, run_uploader):
        assert (
            os.path.join(self.upload_path, "basket_manifest.json")
            in run_uploader
        )

    def test_upload_basket_supplement_in_uploaded_files(self, run_uploader):
        assert (
            os.path.join(self.upload_path, "basket_supplement.json")
            in run_uploader
        )

    def test_upload_basket_metadata_in_uploaded_files(self, run_uploader):
        assert (
            os.path.join(self.upload_path, "basket_metadata.json")
            in run_uploader
        )

    def test_upload_nothing_else_in_uploaded_files(self, run_uploader):
        assert len(run_uploader) == 4

    def test_upload_bucket_name_is_string(self):
        bucket_name = 7

        with pytest.raises(
            TypeError, match=f"'bucket_name' must be a string: '{bucket_name}'"
        ):
            upload(self.upload_items, self.basket_type, bucket_name)
