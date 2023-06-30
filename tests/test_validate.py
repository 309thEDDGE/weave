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
    """
    A class to test functions in validate.py
    """
    def __init__(self, tmpdir):
        """
        Initializes the TestValidate class
        assign the tmpdir, initialize the basket_list, 
        assign the s3fs client, call set_up_bucket
        """
        self.tmpdir = tmpdir
        
        self.basket_list = []
        ck={"endpoint_url": os.environ["S3_ENDPOINT"]}
        self.s3fs_client = s3fs.S3FileSystem(client_kwargs=ck)
        self._set_up_bucket()
        
        
    def _set_up_bucket(self):
        """
        make a temp s3 directory with the bucket name
        """
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
        """
        
        """
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
        
        if is_man:
            tmp_manifest = tmp_basket_dir.join("basket_manifest.json")
            
            #this gives a default valid manifest json schema 
            if man_data == '':
                man_data = '''{
                    "uuid": "str",
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
            
    
    def add_lower_dir_to_temp_basket(
            self, 
            tmp_basket_dir, 
            new_dir_name="nested_dir"
    ):
        nd = tmp_basket_dir.mkdir(new_dir_name)
        nd.join("nested_file.txt").write(
            'this is a nested file to ensure the directory is created'
        )
        return nd
    
    
    def upload_basket(self, tmp_basket_dir, uid='0000', metadata={}):
        upload_dir = self.s3_bucket_name
        b_type = "test_basket"
        up_dir = os.path.join(upload_dir, b_type, uid)
        
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
        ValueError, match=f"Invalid Bucket Path. "
        f"Bucket does not exist at: {bucket_path}"
    ):
        validate.validate_bucket(bucket_path)
    

def test_validate_no_supplement_file(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(
                                tmp_basket_dir_name, 
                                is_man=False, 
                                is_sup=False, 
                                is_meta=False
                            )
    
    s3_basket_path = tv.upload_basket(
                                tmp_basket_dir=tmp_basket_dir, 
                                metadata={"Test":1, "test_bool":True}
                            )    
    
    supplement_path = os.path.join(
            s3_basket_path, 
            "basket_supplement.json"
        )
    
    tv.s3fs_client.rm(supplement_path)
        
    with pytest.raises(
        FileNotFoundError, 
        match=f"Invalid Basket. "
        f"No Supplement file found at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)

    
def test_validate_no_metadata_file(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(
                                tmp_basket_dir_name, 
                                is_man=False, 
                                is_sup=False, 
                                is_meta=False
                            )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    assert validate.validate_bucket(tv.s3_bucket_name) == True
    
    
def test_validate_invalid_manifest_schema(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_man_schema"
    
    # the '100' is supposed to be a string, not a number, 
    # this is invalid against the schema
    bad_manifest_data = """{
                    "uuid": 100, 
                    "upload_time": "str", 
                    "parent_uuids": [ "str1", "str2", "str3" ],
                    "basket_type": "str",
                    "label": "str"
                }"""

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            man_data=bad_manifest_data, 
                            is_sup=True, 
                            is_meta=False
                        )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Manifest Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    

def test_validate_invalid_manifest_json(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_man"
    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            man_data='{"Bad":1}}', 
                            is_sup=True, 
                            is_meta=False
                        )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Manifest could not be loaded into json at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_invalid_supplement_schema(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_sup_schema"
    
    # the '1231231' is supposed to be a boolean, not a number, 
    # this is invalid against the schema
    bad_supplement_data = """{
                    "upload_items":
                    [
                    { "path": "str", "stub": 1231231}
                    ],

                    "integrity_data": 
                    [
                    { 
                        "file_size": 33, 
                        "hash": "str", 
                        "access_date":"str", 
                        "source_path": "str", 
                        "byte_count": 1, 
                        "stub":false, 
                        "upload_path":"str"
                    }
                    ]
                }"""

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            is_sup=True, 
                            sup_data=bad_supplement_data, 
                            is_meta=False
                        )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
        
def test_validate_invalid_supplement_json(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_supp"
    tmp_basket_dir = tv.set_up_basket(
                        tmp_basket_dir_name, 
                        is_man=True, 
                        sup_data='{"Bad":1}}', 
                        is_sup=True, 
                        is_meta=False
                    )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Supplement could not be loaded into json at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_invalid_metadata_json(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_meta"
    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            meta_data='{"Bad":1}}', 
                            is_sup=True, 
                            is_meta=True
                        )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
    
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Metadata could not be loaded into json at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
      
def test_validate_nested_basket(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_nested_basket"
    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            is_sup=True, 
                            is_meta=True
                        )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
      
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Manifest File found in sub directory of basket at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
    
"""
################################################
################################################
### STILL WORK IN PROGRESS NOT PROPERLY WORKING
################################################
################################################
def test_validate_deeply_nested_basket(set_up_TestValidate):
    tv = set_up_TestValidate
    
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name, is_man=False, is_sup=False, is_meta=False)
    

    
    
    my_nested_dir = tv.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir, new_dir_name='nest_level')

    for i in range(4):    
        nested_dir_name = "nest_level_" + str(i)
        my_nested_dir = tv.add_lower_dir_to_temp_basket(tmp_basket_dir=my_nested_dir, new_dir_name=nested_dir_name)
        print('my_nested_dir: ', my_nested_dir)
        
    nested_basket_name = "my_nested_basket"
    nested_basket_dir = tv.set_up_basket(nested_basket_name, is_man=True, is_sup=True, is_meta=False)
        
    # newnewstjfkdsj = tv.set_up_basket(my_nested_dir)
    print('self basket dir:', tv.s3_bucket_name)
    print('nested_basket_dir: ', nested_basket_dir)
    invalid_basket_path = tv.upload_basket(tmp_basket_dir=nested_basket_dir, uid='9999', upload_dir=my_nested_dir, b_type='new_test_basket')
    
    print('invalid basket path: ', invalid_basket_path)
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
    
        
#     deep_basket = "deep_basket"
#     deep_basket_dir = tv.set_up_basket(deep_basket, is_man=True, is_sup=True, is_meta=True)
        
#     s3_basket_path_0002 = tv.upload_basket(tmp_basket_dir=my_nested_dir, uid='0003')
    
#     s3_basket_path_0001 = tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid='0001')
#     print('s3_basket_path:', s3_basket_path_0001)
    print('\n\ndeeply nested:')
    mylist = tv.s3fs_client.find(tv.s3_bucket_name, withdirs=True)
    for i in mylist:
        print(i)
    print('after deeply nested')
    # with pytest.raises(
    #     ValueError, match=f"Invalid Basket. Manifest File found in sub directory of basket at: {s3_basket_path_0001}"
    # ):
    # validate.validate_bucket(tv.s3_bucket_name)
################################################
################################################
### STILL WORK IN PROGRESS NOT PROPERLY WORKING
################################################
################################################
"""
    
def test_validate_no_files_or_dirs(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=False, 
                            is_sup=False, 
                            is_meta=False
                        )
        
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
                
    assert validate.validate_bucket(tv.s3_bucket_name) == True
    
    
def test_validate_no_baskets(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=False, 
                            is_sup=False, 
                            is_meta=False
                        )
    
    #adding this lower dir with a .txt file to have the
    # program at least search the directories.
    nested_dir_name = "nest"
    my_nested_dir = tv.add_lower_dir_to_temp_basket(
                                        tmp_basket_dir=tmp_basket_dir, 
                                        new_dir_name=nested_dir_name
                                    )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
                
    assert validate.validate_bucket(tv.s3_bucket_name) == True
    
    
def test_validate_fifty_baskets(set_up_TestValidate):
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=False, 
                            is_sup=False, 
                            is_meta=False
                        )
    
    nested_basket_name = "my_nested_basket"
    nested_basket_dir = tv.set_up_basket(
                            nested_basket_name, 
                            is_man=True, 
                            is_sup=True, 
                            is_meta=False
                        )
    
    invalid_basket_path = tv.upload_basket(
                                    tmp_basket_dir=nested_basket_dir, 
                                    uid='9999'
                                )
    
    for i in range(50):
        uuid = '00' + str(i)
        s3_basket_path = tv.upload_basket(
                                    tmp_basket_dir=tmp_basket_dir, 
                                    uid=uuid
                                )
   
    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Manifest File found in sub "
        f"directory of basket at: {invalid_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)