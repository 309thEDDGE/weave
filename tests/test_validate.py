
"""
test no manifest in baskset
test invalid manifest json structure in basket
test invalid json object

test no supplement in basket
test invalid supplement json structure in basket
test invalid json object

test no metadata in basket
test invalid metadata json structure (unable to convert to json object) in root basket


test basket inside of basket (this could be one directory down or many)
test if manifest.json is inside sub-directory of a basket (this one goes with the previous one)

test if there are 2 or more manifest files in basket
test if there are 2 or more supplement files in basket 
test if there are 2 or more metadata files in basket


test if there are no files in the bucket
test if there are no baskets found at all (no manifest.json found in any directory)


possibly test each individual item in the manifest and supplement
	like if an item is not a string, throw error
	if an item is not a bool, but a num, throw error, etc.
	do this for every item?
	the validate from jsonschema might do this already so maybe we don't need to.


test if the bucket is also a basket (manifest inside the root bucket)


test a deeply nested basket


"""




import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch

from fsspec.implementations.local import LocalFileSystem
import pytest
import s3fs

from weave import validate, Basket, upload_basket, create_index_from_s3
import weave


class TestValidate():
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
        except:
            self.cleanup_bucket()
            self._set_up_bucket()
            
    def set_up_basket(
            self, 
            tmp_dir_name, 
            is_man=True, 
            is_sup=True, 
            is_meta=True, 
            man_data='', 
            sup_data='', 
            meta_data='',
        ):
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
        
        if is_man:
            tmp_manifest = tmp_basket_dir.join("basket_manifest.json")
            
            #this gives a default valid manifest json schema 
            if man_data == '':
                man_data = '''{
                    "uuid": "str3",
                    "upload_time": "uploadtime string", 
                    "parent_uuids": [ "string1", "string2", "string3" ],
                    "basket_type": "basket type string",
                    "label": "label string"
                }'''
            
            tmp_manifest.write(man_data)
        
        if is_sup:
            tmp_supplement = tmp_basket_dir.join("basket_supplement.json")
            
            #this gives a default valid supplement json schema 
            if sup_data == '':
                sup_data = '''{
                    "upload_items":
                    [
                    { "path": "str", "stub": false}
                    ],

                    "integrity_data": 
                    [
                    { 
                        "file_size": 33, 
                        "hash": "string", 
                        "access_date":"string", 
                        "source_path": "string", 
                        "byte_count": 1, 
                        "stub":false, 
                        "upload_path":"string"
                    }
                    ]
                }'''
            
            tmp_supplement.write(sup_data)
            
        if is_meta:
            tmp_metadata = tmp_basket_dir.join("basket_metadata.json")
            
            #this gives a default valid metadata json structure
            if meta_data == '':
                meta_data = '''{"Test":1, "test_bool":55}'''
            
            tmp_metadata.write(meta_data)
    
        return tmp_basket_dir
    
                            
    
    def add_lower_dir_to_temp_basket(self, tmp_basket_dir, new_dir_name="nested_dir"):
        nd = tmp_basket_dir.mkdir(new_dir_name)
        nd.join("nested_file.txt").write('this is a nested file to ensure the directory is created')
        return tmp_basket_dir
        
                            
    
    def upload_basket(self, tmp_basket_dir, uid='0000', metadata={}):
        b_type = "test_basket"
        up_dir = os.path.join(self.s3_bucket_name, b_type, uid)
        
        upload_basket(
            upload_items=[{'path':str(tmp_basket_dir.realpath()),
                           'stub':False}],
            upload_directory=up_dir,
            unique_id=uid,
            basket_type=b_type,
            metadata=metadata
        )
        return up_dir
    
        
    def cleanup_bucket(self):
        self.s3fs_client.rm(self.s3_bucket_name, recursive=True)
   



    
@pytest.fixture
def set_up_TestValidate(tmpdir):
    tv = TestValidate(tmpdir)
    yield tv
    tv.cleanup_bucket()
    

    
    
def test_validate_bucket_does_not_exist(set_up_TestValidate):
    tv = set_up_TestValidate
    
    bucket_path = Path("THISisNOTaPROPERbucketNAMEorPATH")
    
    with pytest.raises(
        ValueError, match=f"Invalid Bucket Path. Bucket does not exist at: {bucket_path}"
    ):
        validate.validate_bucket(bucket_path)
    
    

def test_validate_no_supplement_file(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=False, is_sup=False, is_meta=False)
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001', metadata={"Test":1, "test_bool":True})
    
    
    supplement_path = os.path.join(s3_basket_path_0001, "basket_supplement.json")
    tv.s3fs_client.rm(supplement_path)
        
    with pytest.raises(
        FileNotFoundError, match=f"Invalid Basket. No Supplement file found at: {s3_basket_path_0001}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)

        
        
    
def test_validate_no_metadata_file(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=False, is_sup=False, is_meta=False)
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
    
    assert validate.validate_bucket(tv.s3_bucket_name) == True
    
    

def test_validate_invalid_manifest_stucture(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_man"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=True, man_data='{"Bad":1}}', is_sup=True, is_meta=False)
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
    
    manifest_path = os.path.join(s3_basket_path_0001, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path_0001, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, match=f"Invalid Basket. Manifest could not be loaded into json at: {s3_basket_path_0001}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
        
def test_validate_invalid_supplement_structure(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_supp"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=True, sup_data='{"Bad":1}}', is_sup=True, is_meta=False)
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
    
    manifest_path = os.path.join(s3_basket_path_0001, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path_0001, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, match=f"Invalid Basket. Supplement could not be loaded into json at: {s3_basket_path_0001}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
    
    
def test_validate_invalid_metadata_json(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_meta"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=True, meta_data='{"Bad":1}}', is_sup=True, is_meta=True)
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
    
    manifest_path = os.path.join(s3_basket_path_0001, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path_0001, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, match=f"Invalid Basket. Metadata could not be loaded into json at: {s3_basket_path_0001}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
    
    
def test_validate_nested_basket(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_nested_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=True, is_sup=True, is_meta=True)
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
      
    with pytest.raises(
        ValueError, match=f"Invalid Basket. Manifest File found in sub directory of basket at: {s3_basket_path_0001}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
    
def test_validate_deeply_nested_basket(set_up_TestValidate):
    tv = set_up_TestValidate
    
    
    tmp_basket_dir_name = "my_deep_nest"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=True, is_sup=True, is_meta=True)
    
    
    
    nested_dir_name = "nest1"
    my_nested_dir = add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir, new_dir_name=nested_dir_name)
    print('my_nested_dir: ', my_nested_dir)
    
    
    
    s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
      
    with pytest.raises(
        ValueError, match=f"Invalid Basket. Manifest File found in sub directory of basket at: {s3_basket_path_0001}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)

  
    
    
    
    
# class MinioBucketAndTempBasket():
#     def __init__(self, tmpdir):
#         self.tmpdir = tmpdir
#         self.basket_list = []
#         ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
#         self.s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
#         self._set_up_bucket()
        
#     def _set_up_bucket(self):
#         try:
#             self.s3_bucket_name = 'pytest-temp-bucket'
#             self.s3fs_client.mkdir(self.s3_bucket_name)
#         except FileExistsError:
#             self.cleanup_bucket()
#             self._set_up_bucket()
            
#     def set_up_basket(self, tmp_dir_name):
#         tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
#         tmp_basket_txt_file = tmp_basket_dir.join("text.txt")
#         tmp_basket_txt_file.write("This is a text file for testing purposes.")
#         return tmp_basket_dir
    
#     def add_lower_dir_to_temp_basket(self, tmp_basket_dir):
#         nd = tmp_basket_dir.mkdir("nested_dir")
#         nd.join("another_test.txt").write("more test text")
#         return tmp_basket_dir
    
#     def upload_basket(self, tmp_basket_dir, uid='0000'):
#         b_type = "test_basket"
#         up_dir = os.path.join(self.s3_bucket_name, b_type, uid)
#         upload_basket(
#             upload_items=[{'path':str(tmp_basket_dir.realpath()),
#                           'stub':False}],
#             upload_directory=up_dir,
#             unique_id=uid,
#             basket_type=b_type
#         )
#         return up_dir
    
#     def cleanup_bucket(self):
#         self.s3fs_client.rm(self.s3_bucket_name, recursive=True)
        


# @pytest.fixture
# def set_up_MBATB(tmpdir):
#     mbatb = MinioBucketAndTempBasket(tmpdir)
#     yield mbatb
#     mbatb.cleanup_bucket()
    
# def test_basket_ls_after_find(set_up_MBATB):
#     """The s3fs.S3FileSystem.ls() func is broken after running {}.find()

#     s3fs.S3FileSystem.find() function is called during index creation. The
#     solution to this problem is to ensure Basket.ls() uses the argument
#     refresh=True when calling s3fs.ls(). This ensures that cached results
#     from s3fs.find() (which is called during create_index_from_s3() and do not
#     include directories) do not affect the s3fs.ls() function used to enable
#     the Basket.ls() function.
#     """
#     # set_up_MBATB is at this point a class object, but it's a weird name
#     # because it looks like a function name (because it was before pytest
#     # did weird stuff to it) so I just rename it to mbatb for reading purposes
    
#     mbatb = set_up_MBATB
#     tmp_basket_dir_name = "test_basket_temp_dir"
#     tmp_basket_dir = mbatb.set_up_basket(tmp_basket_dir_name)
#     tmp_basket_dir = mbatb.add_lower_dir_to_temp_basket(tmp_basket_dir)
#     s3_basket_path = mbatb.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
#     # create index on bucket
#     create_index_from_s3(mbatb.s3_bucket_name)
    
#     test_b = Basket(s3_basket_path)
#     what_should_be_in_base_dir_path = {
#         os.path.join(s3_basket_path, tmp_basket_dir_name, i)
#         for i in ["nested_dir", "text.txt"]
#     }
#     ls = test_b.ls(tmp_basket_dir_name)
#     assert set(ls) == what_should_be_in_base_dir_path