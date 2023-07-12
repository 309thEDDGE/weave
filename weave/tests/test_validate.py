import os
from pathlib import Path
import pytest
from weave.uploader_functions import upload_basket
from weave import validate, config
from weave.tests.pytest_resources import BucketForTest


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
        self.s3fs_client = config.get_file_system()
        self._set_up_bucket()
        
        
    def _set_up_bucket(self):
        """
        make a temp s3 directory with the bucket name
        """
        try:
            self.s3_bucket_name = 'pytest-temp-bucket'
            self.s3fs_client.mkdir(self.s3_bucket_name)
        except Exception:
            self.cleanup_bucket()
            self._set_up_bucket()
          
        
    def set_up_basket(
            self, 
            tmp_dir_name, 
            is_man=False, 
            is_sup=False, 
            is_meta=False, 
            man_data='', 
            sup_data='', 
            meta_data='',
        ):
        """
        Sets up the basket with a nested basket depending on the values of the
        boolean params when this is called. if the is_man is true, a nested 
        manifest file will be put in the basket, same with is_sup for 
        supplement and is_meta for metadata. We can also input our own data 
        for each of these files is the man_data, sup_data, and meta_data. 
        
        Because I can't directly control the upload_basket function, if you 
        want to modify if there is a manifest, supplement, or metadata in
        the basket, I use these. Same if you want to change the schema to
        something invalid.
        
        Parameters
        ----------
        tmp_dir_name: string
            the directory name of where the nested basket will be
        is_man: boolean (optional)
            a bool that signals if ther should be a manifest file
            defaults to no manifest
        is_sup: boolean (optional)
            a bool that signals if ther should be a supplement file
            defaults to no supplement
        is_meta: boolean (optional)
            a bool that signals if ther should be a metadata file
            defaults to no metadata
        man_data: string (optional)
            the json data we want to be put into the manifest file
            defaults to a valid manifest schema
        sup_data: string (optional)
            the json data we want to be put into the supplement file
            defaults to a valid supplement schema
        meta_data: string (optional)
            the json data we want to be put into the metadata file
            defaults to a valid json object
            
        Returns
        ----------
        A string of the directory where the basket was uploaded
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
            new_dir_name="nested_dir",
            is_basket=False
    ):
        """
        Added the is_basket as a work around to test a deeply nested basket
        because I couldn't get it to upload one using the set_up_basket
        or upload_basket function
        """
        nd = tmp_basket_dir.mkdir(new_dir_name)
        nd.join("nested_file.txt").write(
            "this is a nested file to ensure the directory is created"
        )
        
        if is_basket:
            nd.join("basket_manifest.json").write('''{
                    "uuid": "str",
                    "upload_time": "uploadtime string", 
                    "parent_uuids": [ "string1", "string2", "string3" ],
                    "basket_type": "basket type string",
                    "label": "label string"
                }''')

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
    """
    input a bucket path that does not exist and check that it throws an error
    """
    
    bucket_path = Path("THISisNOTaPROPERbucketNAMEorPATH")
    
    with pytest.raises(
        ValueError, match=f"Invalid Bucket Path. "
        f"Bucket does not exist at: {bucket_path}"
    ):
        validate.validate_bucket(bucket_path)
    

def test_validate_no_supplement_file(set_up_TestValidate):
    """
    make a basket, remove the supplement file, check that it throws an error
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
    
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
    """
    make a basket with no metadata (default) validate that it will still be
    a valid bucket (validate_bucket returns true) 
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
    
    tv.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    
    tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    assert validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_invalid_manifest_schema(set_up_TestValidate):
    """
    make a basket with an invalid manifest schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "bad_man_schema"
    
    # the 'uuid: 100' is supposed to be a string, not a number, 
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

        
def test_validate_manifest_schema_missing_field(set_up_TestValidate):
    """
    make a basket with an invalid manifest schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_man_schema"

    # the manifest is missing the uuid field
    # this is invalid against the schema
    bad_manifest_data = """{
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
        
        
def test_validate_manifest_schema_additional_field(set_up_TestValidate):
    """
    make a basket with an invalid manifest schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_man_schema"

    # the manifest has the additional "error" field
    # this is invalid against the schema
    bad_manifest_data = '''{
                    "uuid": "str",
                    "upload_time": "uploadtime string", 
                    "parent_uuids": [ "string1", "string2", "string3" ],
                    "basket_type": "basket type string",
                    "label": "label string",
                    
                    "error": "this is an additional field"
                }'''

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
    """
    make a basket with a manifest that has an invalid json scheme
    (cannot be loaded into a json object) and check that it throws an error
    """
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
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
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
    
    
def test_validate_supplement_schema_missing_field(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement is missing the integrity_data field
    # this is invalid against the schema
    bad_supplement_data = """{
                    "upload_items":
                    [
                    { "path": "str", "stub": false}
                    ]
                }"""

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
        
    
def test_validate_supplement_schema_missing_array_field(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement is missing the upload_path field inside 
    # the integrity_data array
    # this is invalid against the schema
    bad_supplement_data = '''{
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
                        "stub":false
                        
                    }
                    ]
                }'''

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_supplement_schema_missing_array_field_2(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement is missing the stub field inside 
    # the upload_items array
    # this is invalid against the schema
    bad_supplement_data = '''{
                    "upload_items":
                    [
                    { "path": "str" }
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
                        "upload_path": "string"
                    }
                    ]
                }'''

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_supplement_schema_added_array_field(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement has an additional field of "error" in  
    # the upload_items array
    # this is invalid against the schema
    bad_supplement_data = '''{
                    "upload_items":
                    [
                    { 
                        "path": "str", 
                        "stub": false,
                        "error": "additional field"
                    }
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

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_supplement_schema_added_array_field_2(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement has an additional field of "error" in
    # the integrity_data array
    # this is invalid against the schema
    bad_supplement_data = '''{
                    "upload_items":
                    [
                    { 
                        "path": "str", 
                        "stub": false
                    }
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
                        "upload_path":"string",
                        "error": "additional field"
                    }
                    ]
                }'''

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
    
        
def test_validate_supplement_schema_additional_field(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement has an additional my_extra_field field
    # this is invalid against the schema
    bad_supplement_data = '''{
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
                    ],
                    
                    "my_extra_field":"HAHA-ERROR"
                }'''

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
        
    
def test_validate_supplement_schema_empty_upload_items(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement has an empty array of "upload_items"
    # this is invalid against the schema
    bad_supplement_data = '''{
                    "upload_items": [],

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

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
        
        
def test_validate_supplement_schema_empty_integrity_data(set_up_TestValidate):
    """
    make a basket with an invalid supplement schema, check that it 
    throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "bad_sup_schema"

    # the supplement an empty array of "integrity_data"
    # this is invalid against the schema
    bad_supplement_data = '''{
                    "upload_items":
                    [
                    { "path": "str", "stub": false}
                    ],

                    "integrity_data": []
                }'''

    tmp_basket_dir = tv.set_up_basket(
                            tmp_basket_dir_name, 
                            is_man=True, 
                            sup_data=bad_supplement_data, 
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
        f"Supplement Schema does not match at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
        
        
def test_validate_invalid_supplement_json(set_up_TestValidate):
    """
    make a basket with a supplement that has an invalid json scheme
    (cannot be loaded into a json object) and check that it throws an error
    """
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
    """
    make a basket with a metadata that has an invalid json scheme
    (cannot be loaded into a json object) and check that it throws an error
    """
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
    """
    make a basket with another basket inside it, check that it throws an error
    """
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
    
    
def test_validate_deeply_nested(set_up_TestValidate):
    """
    create a basket with a folder that has a manifest (nested basket)
    10 directories deep to make sure the recursive algorithm will find it
    and check that it throws an error
    """
    tv = set_up_TestValidate

    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(
                        tmp_basket_dir_name, 
                        is_man=False, 
                        is_sup=False, 
                        is_meta=False
                    )
    
    my_nested_dir = tv.add_lower_dir_to_temp_basket(
                                tmp_basket_dir=tmp_basket_dir, 
                                new_dir_name='nest_level'
                            )
    
    #create a deep directory 10 deep that we can use
    for i in range(10):    
        nested_dir_name = "nest_level_" + str(i)
        my_nested_dir = tv.add_lower_dir_to_temp_basket(
                                tmp_basket_dir=my_nested_dir, 
                                new_dir_name=nested_dir_name
                            )
        
    # using the deep directory, upload a manifest to make it a nested basket
    my_nested_dir = tv.add_lower_dir_to_temp_basket(
                                tmp_basket_dir=my_nested_dir, 
                                new_dir_name="deepest_basket", 
                                is_basket=True
                            )
    
    nested_basket_name = "my_nested_basket"
    tv.set_up_basket(
                nested_basket_name, 
                is_man=True, 
                is_sup=True, 
                is_meta=False
            )
    
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    with pytest.raises(
        ValueError, match=
        f"Invalid Basket. "
        f"Manifest File found in sub directory of basket at: {s3_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)

    
def test_validate_no_files_or_dirs(set_up_TestValidate):
    """
    create an empty bucket that has no files or dirs, check that it returns 
    true. True because an empty bucket still follows the file structure
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
        
    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
                
    assert validate.validate_bucket(tv.s3_bucket_name)
    
    
def test_validate_no_baskets(set_up_TestValidate):
    """
    create a bucket with dirs that don't have any manifest.json files.
    check that it returns true, because if a bucket has no baskets, 
    it's still considered valid
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
    
    #adding this lower dir with a .txt file to have the
    # program at least search the directories.
    nested_dir_name = "nest"
    tv.add_lower_dir_to_temp_basket(
                            tmp_basket_dir=tmp_basket_dir, 
                            new_dir_name=nested_dir_name
                        )

    s3_basket_path = tv.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tv.s3fs_client.rm(manifest_path)
    tv.s3fs_client.rm(supplement_path)
                
    assert validate.validate_bucket(tv.s3_bucket_name)

    
    
def test_validate_fifty_baskets(set_up_TestValidate):
    """
    create a bucket with 50 baskets in it and 1 nested basket to ensure it
    it will search all of them. check it throws an error. 
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
    tv.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    
    nested_basket_name = "my_nested_basket"
    nested_basket_dir = tv.set_up_basket(
                            nested_basket_name, 
                            is_man=True, 
                            is_sup=True, 
                            is_meta=False
                        )
    tv.add_lower_dir_to_temp_basket(tmp_basket_dir=nested_basket_dir)
    
    invalid_basket_path = tv.upload_basket(
                                    tmp_basket_dir=nested_basket_dir, 
                                    uid='9999'
                                )
    
    for i in range(50):
        uuid = '00' + str(i)
        tv.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    with pytest.raises(
        ValueError, 
        match=f"Invalid Basket. "
        f"Manifest File found in sub "
        f"directory of basket at: {invalid_basket_path}"
    ):
        validate.validate_bucket(tv.s3_bucket_name)
        
        
def test_validate_call_check_level(set_up_TestValidate):
    """
    create a basket, call _check_level() which is a private function, 
    check that it returns true. it returns true, because the _check_level
    function checks all files an directories of the given dir, so it just 
    acts like we are at a random dir instead of the root of the bucket
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
    tv.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    
    tv.upload_basket(
                tmp_basket_dir=tmp_basket_dir, 
                metadata={"Test":1, "test_bool":True}
            )    
    
    assert validate._check_level(tv.s3_bucket_name)
    
    
def test_validate_call_validate_basket(set_up_TestValidate):
    """
    create a basket, call _validate_basket(), which is a private function. 
    check that it throws an error. it throws an error because _validate_basket
    assumes it is given a basket dir, not a bucket dir. so there is no
    manifest found inside the bucket dir
    """
    tv = set_up_TestValidate
    
    tmp_basket_dir_name = "my_basket"
    tmp_basket_dir = tv.set_up_basket(tmp_basket_dir_name)
    tv.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    
    tv.upload_basket(
            tmp_basket_dir=tmp_basket_dir, 
            metadata={"Test":1, "test_bool":True}
        )    
    
        
    with pytest.raises(
        FileNotFoundError, 
        match=f"Invalid Path. "
        f"No Basket found at: {tv.s3_bucket_name}"
    ):
        validate._validate_basket(tv.s3_bucket_name)