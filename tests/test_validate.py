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

from weave import validate


correct_out = {"manifest": True, "supplement": True, "metadata": True}


def test_manifest_uuid():
    print('\n my test \n')
#     fs = LocalFileSystem()
    
#     ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
#     s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
    
#     print(s3fs_client)
    


# def test_bucket_does_not_exist():
    
#     out = validate.validate_bucket("THISisNOTaPROPERbucketNAME")
    
#     assert correct_out == out
    
#     if (correct_out == out):
#         print('correct output')
#     else:
#         print('incorrect output')
    
    
#     basket_path = Path('bad path')
#     with pytest.raises(
#         ValueError, match=f"Basket does not exist: {basket_path}"
#     ):
#         print('error raised')
    
    
#     print("local file system: ", LocalFileSystem())
#     print('test')
    
# print('\n \n \n test2')
    
    # tempPath = './TestingValidation/'
    # validate_bucket(tempPath)
    
    
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
    
    def add_lower_dir_to_temp_basket(self, tmp_bakset_dir):
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