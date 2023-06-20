#in here we test everything we want to try and break it with
#every test function can only have 1 assert statement
#

import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch

from fsspec.implementations.local import LocalFileSystem
import pytest
import s3fs

from weave import validate, Basket, upload_basket, create_index_from_s3



all_true = {"manifest": True, "supplement": True, "metadata": True}




# stuff for the temp directories/basksets
fs = LocalFileSystem()
print('\n-----------------------------\n\n\nfs: ', fs)
basket_type = "test_basket_type"
print("basket type: ", basket_type)
file_system_dir = tempfile.TemporaryDirectory()
print('file system dir: ', file_system_dir)
file_system_dir_path = file_system_dir.name
print('file system dir path: ', file_system_dir_path)
test_bucket = os.path.join(file_system_dir_path, "pytest-bucket")
print('test bucket: ', test_bucket)
fs.mkdir(test_bucket)
uuid = "1234"
basket_path = os.path.join(test_bucket, basket_type, uuid)
print('basket path: ', basket_path)
temp_dir = tempfile.TemporaryDirectory()
print('temp dir: ', temp_dir)
temp_dir_path = temp_dir.name
print('temp dir path: ', temp_dir_path)
print('\n\n\n--------------------------------------')


class TestValidate():
    def __innit__(self, tmpdir):
        # stuff for the temp directories/basksets
        self.fs = LocalFileSystem()
        # print('\n-----------------------------\n\n\nfs: ', fs)
        self.basket_type = "test_basket_type"
        # print("basket type: ", basket_type)
        self.file_system_dir = tempfile.TemporaryDirectory()
        # print('file system dir: ', file_system_dir)
        self.file_system_dir_path = self.file_system_dir.name
        # print('file system dir path: ', file_system_dir_path)
        self.test_bucket = os.path.join(self.file_system_dir_path, "pytest-bucket")
        # print('test bucket: ', test_bucket)
        self.fs.mkdir(self.test_bucket)
        self.uuid = "1234"
        self.basket_path = os.path.join(self.test_bucket, self.basket_type, self.uuid)
        # print('basket path: ', basket_path)
        self.temp_dir = tempfile.TemporaryDirectory()
        # print('temp dir: ', temp_dir)
        self.temp_dir_path = self.temp_dir.name
        # print('temp dir path: ', temp_dir_path)
        # print('\n\n\n--------------------------------------')





def setup_function():  
    global temp_dir
    temp_dir = tempfile.TemporaryDirectory()
    global temp_dir_path
    temp_dir_path = temp_dir.name
    if fs.exists(basket_path):
        fs.rm(basket_path, recursive=True)

    
def teardown_function():
    if (fs.exists(basket_path)):
        fs.rm(basket_path, recursive=True)
    temp_dir.cleanup()

    
    
    
@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_validate_bucket_does_not_exist(patch):
    basket_path = Path("THISisNOTaPROPERbucketNAMEorPATH")
    
    with pytest.raises(
        ValueError, match=f"Invalid basket path: {basket_path}"
    ):
        validate.validate_bucket(basket_path)

        
@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_validate_no_manifest_file(patch):
    upload_basket(
        [{"path": temp_dir_path, "stub": False}],
        basket_path,
        uuid,
        basket_type,
    )
    
    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    os.remove(manifest_path)
    
    with pytest.raises(
        FileNotFoundError, 
        match=f"Invalid Basket, basket_manifest.json doest not exist at: {basket_path}"
    ):
        validate.validate_bucket(basket_path)
    
    
@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_validate_no_supplement_file(patch):
    upload_basket(
        [{"path": temp_dir_path, "stub": False}],
        basket_path,
        uuid,
        basket_type,
    
    )
    
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    os.remove(supplement_path)
    
    with pytest.raises(
        FileNotFoundError, 
        match=f"Invalid Basket, basket_supplement.json doest not exist at: {basket_path}"
    ):
        validate.validate_bucket(basket_path)
    
    
@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_validate_basket_path_is_pathlike(patch):
    # upload basket
    upload_basket(
        [{"path": temp_dir_path, "stub": False}],
        basket_path,
        uuid,
        basket_type,
    )

    # basket_path = 1
    with pytest.raises(
        TypeError,
        match="expected str, bytes or os.PathLike object, not int",
    ):
        validate.validate_bucket(1)

        
        
@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_validate_manifest_is_valid(patch):
    # upload basket
    upload_basket(
        [{"path": temp_dir_path, "stub": False}],
        basket_path,
        uuid,
        basket_type,
    )
    
    # validate_bucket returns a dictionary showing manifest, supplement, and metadata
    # and if their json's were valid by saying true or false
    dict_out = validate.validate_bucket(basket_path)
    # checks if manifest's data is true (valid json scheme)
    assert dict_out['manifest']


@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_validate_supplement_is_valid(patch):
    # upload basket
    upload_basket(
        [{"path": temp_dir_path, "stub": False}],
        basket_path,
        uuid,
        basket_type,
    )
    
    # validate_bucket returns a dictionary showing manifest, supplement, and metadata
    # and if their json's were valid by saying true or false
    dict_out = validate.validate_bucket(basket_path)
    # checks if manifest's data is true (valid json scheme)
    assert dict_out['supplement']
    
    
    
    
    
    
    
    
    
class MinioBucketAndTempBasket():
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.basket_list = []
        ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
        self.s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
        self._set_up_bucket()
        
    def _set_up_bucket(self):
        try:
            self.s3_bucket_name = 'pytest-temp-bucket'
            self.s3fs_client.mkdir(self.s3_bucket_name)
        except FileExistsError:
            self.cleanup_bucket()
            self._set_up_bucket()
            
    def set_up_basket(self, tmp_dir_name):
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
        tmp_basket_txt_file = tmp_basket_dir.join("text.txt")
        tmp_basket_txt_file.write("This is a text file for testing purposes.")
        return tmp_basket_dir
    
    def add_lower_dir_to_temp_basket(self, tmp_basket_dir):
        nd = tmp_basket_dir.mkdir("nested_dir")
        nd.join("another_test.txt").write("more test text")
        return tmp_basket_dir
    
    def upload_basket(self, tmp_basket_dir, uid='0000'):
        b_type = "test_basket"
        up_dir = os.path.join(self.s3_bucket_name, b_type, uid)
        upload_basket(
            upload_items=[{'path':str(tmp_basket_dir.realpath()),
                          'stub':False}],
            upload_directory=up_dir,
            unique_id=uid,
            basket_type=b_type
        )
        return up_dir
    
    def cleanup_bucket(self):
        self.s3fs_client.rm(self.s3_bucket_name, recursive=True)
        


@pytest.fixture
def set_up_MBATB(tmpdir):
    mbatb = MinioBucketAndTempBasket(tmpdir)
    yield mbatb
    mbatb.cleanup_bucket()
    
def test_basket_ls_after_find(set_up_MBATB):
    """The s3fs.S3FileSystem.ls() func is broken after running {}.find()

    s3fs.S3FileSystem.find() function is called during index creation. The
    solution to this problem is to ensure Basket.ls() uses the argument
    refresh=True when calling s3fs.ls(). This ensures that cached results
    from s3fs.find() (which is called during create_index_from_s3() and do not
    include directories) do not affect the s3fs.ls() function used to enable
    the Basket.ls() function.
    """
    # set_up_MBATB is at this point a class object, but it's a weird name
    # because it looks like a function name (because it was before pytest
    # did weird stuff to it) so I just rename it to mbatb for reading purposes
    
    mbatb = set_up_MBATB
    tmp_basket_dir_name = "test_basket_temp_dir"
    tmp_basket_dir = mbatb.set_up_basket(tmp_basket_dir_name)
    tmp_basket_dir = mbatb.add_lower_dir_to_temp_basket(tmp_basket_dir)
    s3_basket_path = mbatb.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    # create index on bucket
    create_index_from_s3(mbatb.s3_bucket_name)
    
    test_b = Basket(s3_basket_path)
    what_should_be_in_base_dir_path = {
        os.path.join(s3_basket_path, tmp_basket_dir_name, i)
        for i in ["nested_dir", "text.txt"]
    }
    ls = test_b.ls(tmp_basket_dir_name)
    assert set(ls) == what_should_be_in_base_dir_path