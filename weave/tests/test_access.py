import os
import tempfile
import pytest
import uuid

from weave import upload, config

class TestUpload:
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir        
        self.s3fs_client = config.get_file_system()
        self.basket_type = "test_basket_type"
        self.test_bucket = "test-bucket"
        self._set_up_bucket()
        
        self.local_dir_path = self.tmpdir.mkdir("test_dir_local")
        self.tmp_basket_txt_file = self.local_dir_path.join("test.txt")
        self.tmp_basket_txt_file.write("This is a file for testing purposes")
        
        self.upload_items = [{"path": self.tmp_basket_txt_file.strpath,
                              "stub": False}]
        self.run_uploader()

    def _set_up_bucket(self):
        try:
            self.s3fs_client.mkdir(self.test_bucket)
        except FileExistsError:
            self.cleanup_bucket()
            self._set_up_bucket()
        
    def cleanup_bucket(self):
        self.s3fs_client.rm(self.test_bucket, recursive=True)

    def run_uploader(self):
        parent_ids = [uuid.uuid1().hex]
        metadata = {"oh": "i don't know", "something": "stupid"}
        label = "my label"
        self.upload_path = upload(
            self.upload_items,
            self.basket_type,
            self.test_bucket,
            parent_ids,
            metadata,
            label,
            test_prefix=self.local_dir_path,
        )
        
        self.uploaded_files = self.s3fs_client.ls(self.upload_path)

@pytest.fixture
def set_up(tmpdir):
    tu = TestUpload(tmpdir)
    yield tu
    tu.cleanup_bucket()
    
def test_upload_test_txt_in_uploaded_files(set_up):
    tu = set_up
    
    # print(f"\nUpload Path: {tu.upload_path}")
    # print(f"Upload File {os.path.join(tu.upload_path, 'test.txt').lstrip(os.sep)}")
    # print("Uploaded Files", tu.uploaded_files)
    
    assert (
        os.path.join(tu.upload_path, "test.txt").lstrip(os.sep) 
        in tu.uploaded_files
    )

def test_upload_basket_manifest_in_uploaded_files(set_up):
    tu = set_up
    assert (
        os.path.join(tu.upload_path, "basket_manifest.json").lstrip(os.sep)
        in tu.uploaded_files
    )

def test_upload_basket_supplement_in_uploaded_files(set_up):
    tu = set_up
    assert (
        os.path.join(tu.upload_path, "basket_supplement.json").lstrip(os.sep)
        in tu.uploaded_files
    )

def test_upload_basket_metadata_in_uploaded_files(set_up):
    tu = set_up
    assert (
        os.path.join(tu.upload_path, "basket_metadata.json").lstrip(os.sep)
        in tu.uploaded_files
    )

def test_upload_nothing_else_in_uploaded_files(set_up):
    tu = set_up
    assert len(tu.uploaded_files) == 4

def test_upload_bucket_name_is_string(set_up):
    tu = set_up
    bucket_name = 7

    with pytest.raises(
        TypeError, match=f"'bucket_name' must be a string: '{bucket_name}'"
    ):
        upload(tu.upload_items, tu.basket_type, bucket_name)
