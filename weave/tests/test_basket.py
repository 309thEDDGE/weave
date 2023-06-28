import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch

from fsspec.implementations.local import LocalFileSystem
import pytest

from weave.basket import Basket
from weave.index import create_index_from_s3
from weave.uploader import upload_basket
from weave.tests.pytest_resources import BucketForTest


class TestBasket:
    def setup_class(self):
        self.fs = LocalFileSystem()
        self.basket_type = "test_basket_type"
        self.file_system_dir = tempfile.TemporaryDirectory()
        self.file_system_dir_path = self.file_system_dir.name
        self.test_bucket = os.path.join(
            self.file_system_dir_path, "pytest-bucket"
        )
        self.fs.mkdir(self.test_bucket)
        self.uuid = "1234"
        self.basket_path = os.path.join(
            self.test_bucket, self.basket_type, self.uuid
        )

    def setup_method(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        if self.fs.exists(self.basket_path):
            self.fs.rm(self.basket_path, recursive=True)

    def teardown_method(self):
        if self.fs.exists(self.basket_path):
            self.fs.rm(self.basket_path, recursive=True)
        self.temp_dir.cleanup()

    def teardown_class(self):
        if self.fs.exists(self.test_bucket):
            self.fs.rm(self.test_bucket, recursive=True)
        self.file_system_dir.cleanup()

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_address_does_not_exist(self, patch):
        basket_path = Path("i n v a l i d p a t h")
        with pytest.raises(
            ValueError, match=f"Basket does not exist: {basket_path}"
        ):
            Basket(Path(basket_path))

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_no_manifest_file(self, patch):
        # upload basket
        upload_basket(
            [{"path": self.temp_dir_path, "stub": False}],
            self.basket_path,
            self.uuid,
            self.basket_type,
        )

        manifest_path = os.path.join(self.basket_path, "basket_manifest.json")
        os.remove(manifest_path)

        with pytest.raises(
            FileNotFoundError,
            match=(
                "Invalid Basket, basket_manifest.json "
                + f"does not exist: {manifest_path}"
            ),
        ):
            Basket(Path(self.basket_path))

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_basket_path_is_pathlike(self, patch):
        # upload basket
        upload_basket(
            [{"path": self.temp_dir_path, "stub": False}],
            self.basket_path,
            self.uuid,
            self.basket_type,
        )

        basket_path = 1
        with pytest.raises(
            TypeError,
            match="expected str, bytes or os.PathLike object, not int",
        ):
            Basket(basket_path)

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_no_suppl_file(self, patch):
        # upload basket
        upload_basket(
            [{"path": self.temp_dir_path, "stub": False}],
            self.basket_path,
            self.uuid,
            self.basket_type,
        )

        supplement_path = os.path.join(
            self.basket_path, "basket_supplement.json"
        )
        os.remove(supplement_path)

        with pytest.raises(
            FileNotFoundError,
            match=(
                "Invalid Basket, basket_supplement.json "
                + f"does not exist: {supplement_path}"
            ),
        ):
            Basket(Path(self.basket_path))

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_manifest(self, patch):
        # upload basket
        upload_basket(
            [{"path": self.temp_dir_path, "stub": False}],
            self.basket_path,
            self.uuid,
            self.basket_type,
        )

        basket = Basket(Path(self.basket_path))
        manifest = basket.get_manifest()
        assert manifest == {
            "uuid": "1234",
            "parent_uuids": [],
            "basket_type": "test_basket_type",
            "label": "",
            "upload_time": manifest["upload_time"],
        }

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_manifest_cached(self, patch):
        # upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        basket = Basket(Path(self.basket_path))

        # Read the basket_manifest.json file and store as a dictionary
        # in the object for later access.
        manifest = basket.get_manifest()

        manifest_path = basket.manifest_path

        # Manually replace the manifest file
        self.fs.rm(manifest_path)
        with self.fs.open(manifest_path, "w") as outfile:
            json.dump({"junk": "b"}, outfile)

        # Manifest should already be stored
        # and the new file shouldn't be read
        manifest = basket.get_manifest()
        assert manifest == {
            "uuid": "1234",
            "parent_uuids": [],
            "basket_type": "test_basket_type",
            "label": "",
            "upload_time": manifest["upload_time"],
        }

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_supplement(self, patch):
        # upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        basket = Basket(Path(self.basket_path))
        supplement = basket.get_supplement()
        assert supplement == {
            "upload_items": upload_items,
            "integrity_data": [],
        }

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_supplement_cached(self, patch):
        # upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        basket = Basket(Path(self.basket_path))

        # Read the basket_supplement.json file and store as a dictionary
        # in the object for later access.
        supplement = basket.get_supplement()

        supplement_path = basket.supplement_path

        # Manually replace the Supplement file
        self.fs.rm(supplement_path)
        with self.fs.open(supplement_path, "w") as outfile:
            json.dump({"junk": "b"}, outfile)

        # Supplement should already be cached
        # and the new file shouldn't be read
        supplement = basket.get_supplement()
        assert supplement == {
            "upload_items": upload_items,
            "integrity_data": [],
        }

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_metadata(self, patch):
        # upload basket
        metadata_in = {"test": 1}
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items,
            self.basket_path,
            self.uuid,
            self.basket_type,
            metadata=metadata_in,
        )

        basket = Basket(Path(self.basket_path))
        metadata = basket.get_metadata()
        assert metadata_in == metadata

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_metadata_cached(self, patch):
        # upload basket
        metadata_in = {"test": 1}
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items,
            self.basket_path,
            self.uuid,
            self.basket_type,
            metadata=metadata_in,
        )

        basket = Basket(Path(self.basket_path))

        # Read the basket_metadata.json file and store as a dictionary
        # in the object for later access.
        metadata = basket.get_metadata()

        metadata_path = basket.metadata_path

        # Manually replace the metadata file
        self.fs.rm(metadata_path)
        with self.fs.open(metadata_path, "w") as outfile:
            json.dump({"junk": "b"}, outfile)

        # Metadata should already be cached
        # and the new file shouldn't be read
        metadata = basket.get_metadata()
        assert metadata_in == metadata

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_get_metadata_none(self, patch):
        # upload basket
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        basket = Basket(Path(self.basket_path))
        metadata = basket.get_metadata()
        assert metadata is None

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_ls(self, patch):
        # upload basket
        data_path = os.path.join(self.temp_dir_path, "data.json")
        with self.fs.open(data_path, "w") as outfile:
            json.dump({"junk": "b"}, outfile)

        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        source_dir_name = os.path.basename(self.temp_dir_path)
        uploaded_dir_path = f"{self.basket_path}/{source_dir_name}"
        basket = Basket(Path(self.basket_path))
        assert basket.ls() == [uploaded_dir_path]

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_ls_relpath(self, patch):
        # upload basket
        data_path = os.path.join(self.temp_dir_path, "data.json")
        with self.fs.open(data_path, "w") as outfile:
            json.dump({"junk": "b"}, outfile)

        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        source_dir_name = os.path.basename(self.temp_dir_path)
        uploaded_json_path = f"{self.basket_path}/{source_dir_name}/data.json"
        basket = Basket(Path(self.basket_path))
        assert basket.ls(Path(source_dir_name)) == [uploaded_json_path]

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_ls_relpath_period(self, patch):
        # upload basket
        data_path = os.path.join(self.temp_dir_path, "data.json")
        with self.fs.open(data_path, "w") as outfile:
            json.dump({"junk": "b"}, outfile)

        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )

        source_dir_name = os.path.basename(self.temp_dir_path)
        uploaded_dir_path = f"{self.basket_path}/{source_dir_name}"
        basket = Basket(Path(self.basket_path))
        assert basket.ls(".") == [uploaded_dir_path]

    @patch("weave.config.get_file_system", return_value=LocalFileSystem())
    def test_basket_ls_is_pathlike(self, patch):
        upload_items = [{"path": self.temp_dir_path, "stub": False}]
        upload_basket(
            upload_items, self.basket_path, self.uuid, self.basket_type
        )
        basket = Basket(Path(self.basket_path))

        with pytest.raises(
            TypeError,
            match="expected str, bytes or os.PathLike object, not int",
        ):
            basket.ls(1)

"""Pytest Fixtures Documentation:
https://docs.pytest.org/en/7.3.x/how-to/fixtures.html

https://docs.pytest.org/en/7.3.x/how-to
/fixtures.html#teardown-cleanup-aka-fixture-finalization"""

@pytest.fixture
def set_up_tb(tmpdir):
    tb = BucketForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()

def test_basket_ls_after_find(set_up_tb):
    """The s3fs.S3FileSystem.ls() func is broken after running {}.find()

    s3fs.S3FileSystem.find() function is called during index creation. The
    solution to this problem is to ensure Basket.ls() uses the argument
    refresh=True when calling s3fs.ls(). This ensures that cached results
    from s3fs.find() (which is called during create_index_from_s3() and do not
    include directories) do not affect the s3fs.ls() function used to enable
    the Basket.ls() function.
    """
    # set_up_tb is at this point a class object, but it's a weird name
    # because it looks like a function name (because it was before pytest
    # did weird stuff to it) so I just rename it to mbatb for reading purposes
    tb = set_up_tb
    tmp_basket_dir_name = "test_basket_temp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    tmp_basket_dir = tb.add_lower_dir_to_temp_basket(tmp_basket_dir)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # create index on bucket
    create_index_from_s3(tb.s3_bucket_name)

    # run find in case index creation changes
    tb.s3fs_client.find(tb.s3_bucket_name)

    # set up basket
    test_b = Basket(s3_basket_path)
    what_should_be_in_base_dir_path = {
        os.path.join(s3_basket_path, tmp_basket_dir_name, i)
        for i in ["nested_dir", "test.txt"]
    }
    ls = test_b.ls(tmp_basket_dir_name)
    assert set(ls) == what_should_be_in_base_dir_path