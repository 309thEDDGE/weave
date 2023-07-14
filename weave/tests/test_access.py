import os
import pytest
import uuid

from weave import upload
from weave.tests.pytest_resources import BucketForTest

class UploadForTest(BucketForTest):
    """
    Test class extended from BucketForTest to include custom call for upload.
    """
    def __init__(self, tmpdir):
        super().__init__(tmpdir)
        
    def run_uploader(self, tmp_basket_dir):
        """
        Wrapper to call the weave upload function.
        """
        upload_items = [{'path': str(os.path.join(tmp_basket_dir, "test.txt")),
                         'stub': False}]
        b_type = "test_basket"
        metadata = {"oh": "i don't know", "something": "stupid"}
        label = "my label"
        parent_ids = [uuid.uuid1().hex]
        
        self.upload_path = upload(
            upload_items,
            b_type,
            self.s3_bucket_name,
            parent_ids,
            metadata,
            label,
            test_prefix="test_prefix",
        )
        
        self.uploaded_files = self.s3fs_client.ls(self.upload_path)
        
        return self.upload_path
    
@pytest.fixture
def set_up_tb(tmpdir):
    tb = UploadForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()
    
def test_upload_test_txt_in_uploaded_files(set_up_tb):
    """
    Test that uploaded test files are properly uploaded.
    """
    tb = set_up_tb
    
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    upload_path = tb.run_uploader(tmp_basket_dir)
    
    assert (
        os.path.join(upload_path, "test.txt") in tb.uploaded_files
    )

def test_upload_basket_manifest_in_uploaded_files(set_up_tb):
    """
    Test that basket manifest files are properly uploaded.
    """
    tb = set_up_tb
    
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    upload_path = tb.run_uploader(tmp_basket_dir)
    
    assert (
        os.path.join(upload_path, "basket_manifest.json") 
        in tb.uploaded_files
    )

def test_upload_basket_supplement_in_uploaded_files(set_up_tb):
    """
    Test that basket supplement files are properly uploaded.
    """
    tb = set_up_tb
    
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    upload_path = tb.run_uploader(tmp_basket_dir)
    
    assert (
        os.path.join(upload_path, "basket_supplement.json")
        in tb.uploaded_files
    )

def test_upload_basket_metadata_in_uploaded_files(set_up_tb):
    """
    Test that basket metadata files are properly uploaded.
    """
    tb = set_up_tb
    
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    upload_path = tb.run_uploader(tmp_basket_dir)
    
    assert (
        os.path.join(upload_path, "basket_metadata.json")
        in tb.uploaded_files
    )

def test_upload_nothing_else_in_uploaded_files(set_up_tb):
    """
    Test that only basket data and required files are uploaded.
    """
    tb = set_up_tb
    
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    tb.run_uploader(tmp_basket_dir)
    
    assert len(tb.uploaded_files) == 4

def test_upload_bucket_name_is_string(set_up_tb):
    """
    Test that an error is raised when the bucket name is not a string.
    """
    tb = set_up_tb
    
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    tb.run_uploader(tmp_basket_dir)
    
    bucket_name = 7

    upload_items = [{'path': str(os.path.join(tmp_basket_dir, "test.txt")),
                     'stub': False}]
    
    with pytest.raises(
        TypeError, match=f"'bucket_name' must be a string: '{bucket_name}'"
    ):
        upload(upload_items, "test_basket", bucket_name)