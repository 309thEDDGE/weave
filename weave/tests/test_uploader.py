import uuid
import os
import tempfile
import json
import pytest
import time
from datetime import datetime
from fsspec.implementations.local import LocalFileSystem
from unittest.mock import patch

from weave.uploader import upload_basket
from weave.uploader_functions import (derive_integrity_data,
                                      validate_upload_item)

from weave.tests.pytest_resources import BucketForTest


# class TestValidateUploadItems:
#     def setup_method(self):
#         self.temp_dir = tempfile.TemporaryDirectory()
#         self.temp_dir_path = self.temp_dir.name

#     def teardown_method(self):
#         self.temp_dir.cleanup()

@pytest.fixture
def set_up_tb(tmpdir):
    tb = BucketForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()

def test_validate_upload_item_correct_schema_path_key(set_up_tb):
    file_path = "path/path"

    # Invalid Path Key
    upload_item = {"path_invalid_key": file_path, "stub": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'path_invalid_key'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_path_type(set_up_tb):
    upload_item = {"path": 1234, "stub": True}
    with pytest.raises(
        TypeError, match="Invalid upload_item type: 'path: <class 'int'>'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_stub_key(set_up_tb):
    file_path = "path/path"
    # Invalid Stub Key
    upload_item = {"path": file_path, "invalid_stub_key": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'invalid_stub_key'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_stub_type(set_up_tb):
    # Invalid Stub Type
    file_path = "path/path"
    upload_item = {"path": file_path, "stub": "invalid type"}
    with pytest.raises(
        TypeError, match="Invalid upload_item type: 'stub: <class 'str'>'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_extra_key(set_up_tb):
    file_path = "path/path"
    # Extra Key
    upload_item = {"path": file_path, "stub": True, "extra_key": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'extra_key'"
    ):
        validate_upload_item(upload_item)



def test_validate_upload_item_valid_inputs(set_up_tb):
    """
    Test that no errors are raised when calling validate_upload_item on valid
    inputs.
    """
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    # Test using the FILE path
    tmp_basket_file_path = tmp_basket_dir.join("test.txt")
    valid_upload_item = {"path": tmp_basket_file_path.strpath, "stub": True}
    
    try:
        validate_upload_item(valid_upload_item)
    except Exception as e:
        pytest.fail(f"Unexpected error occurred:{e}")
        

def test_validate_upload_item_file_exists(set_up_tb):
    upload_item = {"path": "i n v a l i d p a t h", "stub": True}
    with pytest.raises(
        FileExistsError,
        match="'path' does not exist: 'i n v a l i d p a t h'",
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_folder_exists(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    # Test using the FOLDER path
    valid_upload_item = {"path": tmp_basket_dir.strpath, "stub": True}
    
    try:
        validate_upload_item(valid_upload_item)
    except Exception as e:
        pytest.fail(f"Unexpected error occurred:{e}")

def test_validate_upload_item_validate_dictionary(set_up_tb):
    upload_item = 5
    with pytest.raises(
        TypeError,
        match="'upload_item' must be a dictionary: 'upload_item = 5'",
    ):
        validate_upload_item(upload_item)

def test_derive_integrity_data_file_doesnt_exist(set_up_tb):
    file_path = "f a k e f i l e p a t h"
    with pytest.raises(
        FileExistsError, match=f"'file_path' does not exist: '{file_path}'"
    ):
        derive_integrity_data(file_path)

def test_derive_integrity_data_path_is_string(set_up_tb):
    file_path = 10
    with pytest.raises(
        TypeError, match=f"'file_path' must be a string: '{file_path}'"
    ):
        derive_integrity_data(file_path)

def test_derive_integrity_data_byte_count_string(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    byte_count_in = "invalid byte count"
    
    with pytest.raises(
        TypeError, match=f"'byte_count' must be an int: '{byte_count_in}'"
    ):
        derive_integrity_data(tmp_basket_file_path, byte_count=byte_count_in)

def test_derive_integrity_data_byte_count_float(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    byte_count_in = 6.5
    
    with pytest.raises(
        TypeError, match=f"'byte_count' must be an int: '{byte_count_in}'"
    ):
        derive_integrity_data(tmp_basket_file_path, byte_count=byte_count_in)

def test_derive_integrity_data_byte_count_0(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    byte_count_in = 0
    
    with pytest.raises(
        ValueError,
        match=f"'byte_count' must be greater than zero: '{byte_count_in}'",
    ):
        derive_integrity_data(tmp_basket_file_path, byte_count=byte_count_in)

def test_derive_integrity_data_large_byte_count(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    
    # Expected sha256 hash of the string "0123456789"
    e_hash = "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882"
    assert e_hash == derive_integrity_data(tmp_basket_file_path, 10**6)["hash"]

def test_derive_integrity_data_small_byte_count(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    
    # Expected sha256 hash of the string "014589". This string is used as the
    # file size is <= 3*byte_count. So checksum is generated using bytes from
    # beginning, middle, and end (instead of whole file content)
    e_hash = "a2a7cb1d7fc8f79e33b716b328e19bb381c3ec96a2dca02a3d1183e7231413bb"
    assert e_hash == derive_integrity_data(tmp_basket_file_path, 2)["hash"]

def test_derive_integrity_data_file_size(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)
    
    tmp_basket_file_path = tmp_basket_dir.join(text_file_name).strpath
    
    # Check the size of the file is accurate to the length of it's contents.
    assert (
        derive_integrity_data(tmp_basket_file_path, 2)["file_size"]
        == len(text_file_content)
    )

def test_derive_integrity_data_date(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    
    access_date = derive_integrity_data(tmp_basket_file_path, 2)["access_date"]
    access_date = datetime.strptime(access_date, "%m/%d/%Y %H:%M:%S")
    access_date_seconds = access_date.timestamp()
    now_seconds = time.time_ns() // 10**9
    diff_seconds = abs(access_date_seconds - now_seconds)
    assert diff_seconds < 60

def test_derive_integrity_data_source_path(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath
    
    assert (
        derive_integrity_data(tmp_basket_file_path, 2)["source_path"]
        == tmp_basket_file_path
    )

def test_derive_integrity_byte_count(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)
    
    tmp_basket_file_path = tmp_basket_dir.join(text_file_name).strpath
    
    assert derive_integrity_data(tmp_basket_file_path, 2)["byte_count"] == 2

def test_derive_integrity_data_max_byte_count_off_by_one(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)
    
    tmp_basket_file_path = tmp_basket_dir.join(text_file_name).strpath
    byte_count_in = 300 * 10**6 + 1
    
    with pytest.raises(
        ValueError,
        match=f"'byte_count' must be less "
        f"than or equal to 300000000 bytes: '{byte_count_in}'",
    ):
        derive_integrity_data(tmp_basket_file_path, byte_count=byte_count_in)

def test_derive_integrity_data_max_byte_count_exact(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)
    
    tmp_basket_file_path = tmp_basket_dir.join(text_file_name).strpath
    byte_count_in = 300 * 10**6 + 1
    
    try:
        derive_integrity_data(
            tmp_basket_file_path, byte_count=(byte_count_in - 1)
        )
    except Exception as e:
        pytest.fail(f"Unexpected error occurred:{e}")


        
        
        
        
        
        
# class TestUploadBasket:
#     def setup_class(cls):
#         cls.fs = LocalFileSystem()
#         cls.basket_type = "test_basket_type"
#         cls.file_system_dir = tempfile.TemporaryDirectory()
#         cls.file_system_dir_path = cls.file_system_dir.name
#         cls.test_bucket = (
#             f"{cls.file_system_dir_path}/pytest-{uuid.uuid1().hex}"
#         )
#         cls.fs.mkdir(cls.test_bucket)
#         cls.basket_path = f"{cls.test_bucket}/{cls.basket_type}"

#     def setup_method(self):
#         self.temp_dir = tempfile.TemporaryDirectory()
#         self.temp_dir_path = self.temp_dir.name
#         if self.fs.exists(f"{self.basket_path}"):
#             self.fs.rm(f"{self.basket_path}", recursive=True)

#     def teardown_method(self):
#         if self.fs.exists(f"{self.basket_path}"):
#             self.fs.rm(f"{self.basket_path}", recursive=True)
#         self.temp_dir.cleanup()

#     def teardown_class(cls):
#         if cls.fs.exists(cls.test_bucket):
#             cls.fs.rm(cls.test_bucket, recursive=True)
#         cls.file_system_dir.cleanup()



def test_upload_basket_upload_items_is_not_a_string(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = "n o t a r e a l p a t h"
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)

    with pytest.raises(
        TypeError,
        match="'upload_items' must be a list of "
        + f"dictionaries: '{upload_items}'",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

def test_upload_basket_upload_items_is_not_a_list_of_strings(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = ["invalid", "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)
    
    with pytest.raises(
        TypeError, match="'upload_items' must be a list of dictionaries:"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

def test_upload_basket_upload_items_is_a_list_of_only_dictionaries(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [{}, "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)
    
    with pytest.raises(
        TypeError, match="'upload_items' must be a list of dictionaries:"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

def test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [{}, "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)
    
    try:
        upload_basket(upload_items, upload_path, unique_id, basket_type)
    except TypeError:
        assert not tb.s3fs_client.exists(upload_path)

def test_upload_basket_upload_items_invalid_dictionary(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_txt_file_name = "test.txt"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=tmp_basket_txt_file_name)
    tmp_basket_txt_file = tmp_basket_dir.join(tmp_basket_txt_file_name)
    
    
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        },
        {"path_invalid_key": tmp_basket_txt_file.strpath, "stub": True},
    ]
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'path_invalid_key'"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

def test_deletion_when_basket_upload_items_is_an_invalid_dictionary(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_txt_file_name = "test.txt"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=tmp_basket_txt_file_name)
    tmp_basket_txt_file = tmp_basket_dir.join(tmp_basket_txt_file_name)
    
    
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        },
        {"path_invalid_key":  tmp_basket_txt_file.strpath, "stub": True},
    ]
    try:
        upload_basket(upload_items, upload_path, unique_id, basket_type)
    except KeyError:
        assert not tb.s3fs_client.exists(f"{upload_path}")

        
        
        
        
def test_upload_basket_upload_items_check_unique_file_folder_names(set_up_tb):
#     unique_id = uuid.uuid1().hex
#     upload_path = f"{self.basket_path}/{unique_id}"

#     temp_dir2 = tempfile.TemporaryDirectory()
#     temp_dir_path2 = temp_dir2.name

#     json_data = {"t": [1, 2, 3]}

#     json_path1 = os.path.join(self.temp_dir_path, "sample.json")
#     json_path2 = os.path.join(temp_dir_path2, "sample.json")

#     with open(json_path1, "w") as outfile:
#         json.dump(json_data, outfile)
#     with open(json_path2, "w") as outfile:
#         json.dump(json_data, outfile)

#     dir_path1 = os.path.join(temp_dir_path2, "directory_name")
#     dir_path2 = os.path.join(self.temp_dir_path, "directory_name")
#     os.mkdir(dir_path1)
#     os.mkdir(dir_path2)

    tb = set_up_tb
    
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    tmp_basket_txt_file_name = "test.txt"
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name1 = "test_basket_tmp_dir"
    tmp_basket_dir1 = tb.set_up_basket(tmp_basket_dir_name1,
                                      file_name=tmp_basket_txt_file_name)
    tmp_basket_txt_file1 = tmp_basket_dir1.join(tmp_basket_txt_file_name)
    
    # Manually add another test file with the same name as the other.
    tmp_basket_dir_name2 = "test_basket_tmp_dir"
    tmp_basket_dir2 = tb.set_up_basket(tmp_basket_dir_name2,
                                      file_name=tmp_basket_txt_file_name)
    tmp_basket_txt_file2 = tmp_basket_dir2.join(tmp_basket_txt_file_name)

    print("\n\n-------------<JA>-------------")
    print(tmp_basket_dir1)
    print(tmp_basket_dir2)
    print("-------------<JA>-------------\n")
    

    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)


    # Test same file names
    upload_items = [
        {
            "path": tmp_basket_txt_file1.strpath,
            "stub": True,
        },
        {"path": tmp_basket_txt_file2.strpath, "stub": True},
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test.txt",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    # Test same dirname
    upload_items = [
        {
            "path": tmp_basket_dir1.strpath,
            "stub": True,
        },
        {"path": tmp_basket_dir2.strpath, "stub": True},
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = directory_name",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

        
        
    dir_path3 = os.path.join(tmp_basket_dir1, "test.txt")
    # Test same dirname same file
    upload_items = [
        {
            "path": tmp_basket_txt_file1.strpath,
            "stub": True,
        },
        {"path": dir_path3, "stub": True},
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test.txt",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert not tb.s3fs_client.exists(f"{basket_path}")

    
    
    
    
    
    
    
    
# check if upload_path is a string
def test_upload_basket_upload_path_is_string(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]
    
    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = 1234

    with pytest.raises(
        TypeError,
        match=f"'upload_directory' must be a string: '{upload_path}'",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

# check if unique_id is an int
def test_upload_basket_unique_id_string(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]
    
    basket_type = "test_basket"
    unique_id = 6
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, f"{unique_id}")

    with pytest.raises(
        TypeError, match=f"'unique_id' must be a string: '{unique_id}'"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

# check if basket_type is a string
def test_upload_basket_type_is_string(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = 1234
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)


    with pytest.raises(
        TypeError, match=f"'basket_type' must be a string: '{basket_type}'"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

# check if parent_ids is a list of ints
def test_upload_basket_parent_ids_list_str(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)
    parent_ids_in = ["a", 3]

    with pytest.raises(
        TypeError, match="'parent_ids' must be a list of strings:"
    ):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            parent_ids=parent_ids_in,
        )

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

# check if parent_ids is a list
def test_upload_basket_parent_ids_is_list(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)
    parent_ids_in = 56

    with pytest.raises(
        TypeError, match="'parent_ids' must be a list of strings:"
    ):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            parent_ids=parent_ids_in,
        )

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

# check if metadata is a dictionary
def test_upload_basket_metadata_is_dictionary(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)
    metadata_in = "invalid"

    with pytest.raises(
        TypeError,
        match=f"'metadata' must be a dictionary: '{metadata_in}'",
    ):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            metadata=metadata_in,
        )

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

# check if label is string
def test_upload_basket_label_is_string(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)
    label_in = 1234

    with pytest.raises(
        TypeError, match=f"'label' must be a string: '{label_in}'"
    ):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            label=label_in,
        )

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")

def test_upload_basket_no_metadata(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)

    upload_basket(upload_items, upload_path, unique_id, basket_type)

    # Assert metadata.json was not written
    assert not tb.s3fs_client.exists(f"{upload_path}/metadata.json")

    
    
    
    
    
    
    
def test_upload_basket_check_existing_upload_path(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)

    # tb.s3fs_client.upload(tmp_basket_dir, f"{upload_path}", recursive=True)
    # upload_basket(upload_items, upload_path, unique_id, basket_type)
    tb.s3fs_client.upload(tmp_basket_dir.strpath, f"{upload_path}", recursive=True)

    with pytest.raises(
        FileExistsError,
        match=f"'upload_directory' already exists: '{upload_path}''",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert tb.s3fs_client.ls(f"{tmp_basket_dir}") == [upload_path]

    
    
    
    
    
    
    
    
def test_upload_basket_check_unallowed_file_names(set_up_tb):
    tb = set_up_tb
    
    json_data = {"t": [1, 2, 3]}
    unallowed_file_names = [
        "basket_manifest.json",
        "basket_metadata.json",
        "basket_supplement.json",
    ]
    for ind, unallowed_file_name in enumerate(unallowed_file_names):
        # Create a temporary basket with a test file.
        tmp_basket_dir_name = f"test_basket_tmp_dir{ind}"
        tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                          file_name=unallowed_file_name,
                                          file_content=json_data)
        
        unallowed_file_path = tmp_basket_dir.join(unallowed_file_name)

        upload_items = [
            {
                "path": unallowed_file_path.strpath,
                "stub": True,
            }
        ]

        basket_type = "test_basket"
        unique_id = uuid.uuid1().hex
        upload_path = os.path.join(tb.s3_bucket_name, 
                                   f"{basket_type}", unique_id)

        with pytest.raises(
            ValueError,
            match=f"'{unallowed_file_name}' filename not allowed",
        ):
            upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert not tb.s3fs_client.exists(f"{tmp_basket_dir}")


    
def test_upload_basket_clean_up_on_error(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)

    with pytest.raises(Exception, match="Test Clean Up"):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            test_clean_up=True,
        )

    assert not tb.s3fs_client.exists(upload_path)

def test_upload_basket_invalid_optional_argument(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)

    with pytest.raises(KeyError, match="Invalid kwargs argument: 'junk'"):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            junk=True,
        )

    assert not tb.s3fs_client.exists(upload_path)

def test_upload_basket_invalid_test_clean_up_datatype(set_up_tb):
    tb = set_up_tb
    
    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    
    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'test_clean_up: "
        "must be type <class 'bool'>'",
    ):
        upload_basket(
            upload_items,
            upload_path,
            unique_id,
            basket_type,
            test_clean_up="a",
        )

    assert not tb.s3fs_client.exists(upload_path)

@patch("weave.config.get_file_system", return_value=LocalFileSystem())
def test_upload_basket_end_to_end_test(self, patch):
    file_path1 = os.path.join(self.temp_dir_path, "file1.txt")
    file1_data = "01234"

    # non stub files
    with open(file_path1, "w") as outfile:
        outfile.write(file1_data)

    # One non stub directory with one file at the base and the other nested
    dir_path1 = os.path.join(self.temp_dir_path, "directory_name")
    os.mkdir(dir_path1)

    file_path2 = os.path.join(dir_path1, "file.txt")
    file2_data = "5678"

    with open(file_path2, "w") as outfile:
        outfile.write(file2_data)

    mid_dir_path = os.path.join(dir_path1, "mid_directory")
    os.mkdir(mid_dir_path)
    file_path3 = os.path.join(mid_dir_path, "file.txt")
    file3_data = "ABCDEFG"
    with open(file_path3, "w") as outfile:
        outfile.write(file3_data)

    # One non stub directory with slash on end and one file at the base
    dir_path2 = self.temp_dir_path + "/directory_name2/"
    os.mkdir(dir_path2)

    file_path4 = os.path.join(dir_path2, "file4.txt")
    file4_data = "HI"

    with open(file_path4, "w") as outfile:
        outfile.write(file4_data)

    # One non stub directory without a file
    empty_dir_path = os.path.join(self.temp_dir_path, "empty_directory")
    os.mkdir(empty_dir_path)

    # one stub file
    file_path_stub1 = os.path.join(self.temp_dir_path, "filestub1.txt")
    file1_stub_data = "JKLMN"
    with open(file_path_stub1, "w") as outfile:
        outfile.write(file1_stub_data)

    # one stub directory with a file
    dir_path_stub = os.path.join(self.temp_dir_path, "directory_stub")
    os.mkdir(dir_path_stub)
    file_path_stub2 = os.path.join(dir_path_stub, "filestub2.txt")
    file2_stub_data = "OPQ"
    with open(file_path_stub2, "w") as outfile:
        outfile.write(file2_stub_data)

    # one stub directory without any data
    empty_dir_path_stub = os.path.join(
        self.temp_dir_path, "empty_directory_stub"
    )
    os.mkdir(empty_dir_path_stub)

    # Test same file names
    upload_items = [
        {
            "path": file_path1,
            "stub": False,
        },
        {"path": dir_path1, "stub": False},
        {"path": dir_path2, "stub": False},
        {"path": empty_dir_path, "stub": False},
        {"path": file_path_stub1, "stub": True},
        {"path": dir_path_stub, "stub": True},
        {"path": empty_dir_path_stub, "stub": True},
    ]

    original_files = os.listdir(self.temp_dir_path)

    # Run upload_basket
    unique_id = uuid.uuid1().hex
    upload_path = f"{self.basket_path}/{unique_id}"

    label_in = "note"
    metadata_in = {"metadata": [1, 2, 3]}
    parent_ids_in = ["e", "d", "c", "b"]

    upload_basket(
        upload_items,
        upload_path,
        unique_id,
        self.basket_type,
        parent_ids=parent_ids_in,
        metadata=metadata_in,
        label=label_in,
    )

    upload_path = f"{upload_path}"
    # Assert original local path hasn't been altered
    assert original_files == os.listdir(self.temp_dir_path)
    with open(file_path1, "r") as file:
        assert file.read() == file1_data
    with open(file_path2, "r") as file:
        assert file.read() == file2_data
    with open(file_path3, "r") as file:
        assert file.read() == file3_data
    with open(file_path4, "r") as file:
        assert file.read() == file4_data
    with open(file_path_stub1, "r") as file:
        assert file.read() == file1_stub_data
    with open(file_path_stub2, "r") as file:
        assert file.read() == file2_stub_data

    # Assert basket.json fields
    with self.fs.open(f"{upload_path}/basket_manifest.json", "rb") as file:
        basket_json = json.load(file)
        assert basket_json["uuid"] == unique_id
        assert basket_json["parent_uuids"] == parent_ids_in
        assert basket_json["basket_type"] == self.basket_type
        assert basket_json["label"] == label_in
        upload_time = basket_json["upload_time"]
        upload_time = datetime.strptime(upload_time, "%m/%d/%Y %H:%M:%S")
        upload_time_seconds = upload_time.timestamp()
        now_seconds = time.time_ns() // 10**9
        diff_seconds = abs(upload_time_seconds - now_seconds)
        assert diff_seconds < 60

    # Assert metadata.json fields
    with self.fs.open(f"{upload_path}/basket_metadata.json", "rb") as file:
        assert json.load(file) == metadata_in

    # Assert uploaded data
    file_path = os.path.join(
        upload_path, os.path.relpath(file_path1, self.temp_dir_path)
    )
    with self.fs.open(file_path, "r") as file:
        assert file.read() == file1_data

    file_path = os.path.join(
        upload_path, os.path.relpath(file_path2, self.temp_dir_path)
    )
    with self.fs.open(file_path, "r") as file:
        assert file.read() == file2_data

    file_path = os.path.join(
        upload_path, os.path.relpath(file_path3, self.temp_dir_path)
    )
    with self.fs.open(file_path, "r") as file:
        assert file.read() == file3_data

    file_path = os.path.join(
        upload_path, os.path.relpath(file_path4, self.temp_dir_path)
    )
    with self.fs.open(file_path, "r") as file:
        assert file.read() == file4_data

    test_upload_path = f"{upload_path}/{os.path.basename(empty_dir_path)}"
    assert not self.fs.exists(test_upload_path)

    test_upload_path = (
        f"{upload_path}/{os.path.basename(empty_dir_path_stub)}"
    )
    assert not self.fs.exists(test_upload_path)

    test_upload_path = f"{upload_path}/{os.path.basename(dir_path_stub)}"
    assert not self.fs.exists(test_upload_path)

    assert not self.fs.exists(
        f"{upload_path}/{os.path.basename(file_path_stub1)}"
    )

    # Assert supplement.json fields
    with self.fs.open(
        f"{upload_path}/basket_supplement.json", "rb"
    ) as file:
        supplement_json = json.load(file)

        test_upload_path = f"{upload_path}/{os.path.basename(file_path1)}"
        count = 0
        for integrity_data in supplement_json["integrity_data"]:
            if integrity_data["source_path"] == file_path1:
                assert integrity_data["upload_path"] == test_upload_path
                assert (
                    integrity_data["hash"] == "c565fe03ca9b6242e01dfddefe"
                    "9bba3d98b270e19cd02fd85ceaf75e2b25bf12"
                )
                assert integrity_data["file_size"] == 5
                assert integrity_data["stub"] is False
                assert len(integrity_data.keys()) == 7
                assert integrity_data["byte_count"] == 10**8
                assert "access_date" in integrity_data.keys()
                break
            count += 1
            # Assert that the upload item exists in the list
            assert count < len(supplement_json["integrity_data"])

        count = 0
        test_upload_path = os.path.join(
            upload_path, os.path.relpath(file_path2, self.temp_dir_path)
        )
        for integrity_data in supplement_json["integrity_data"]:
            if integrity_data["source_path"] == file_path2:
                assert integrity_data["upload_path"] == test_upload_path
                assert (
                    integrity_data["hash"] == "f8638b979b2f4f793ddb6db"
                    "d197e0ee25a7a6ea32b0ae22f5e3c5d119d839e75"
                )
                assert integrity_data["file_size"] == 4
                assert integrity_data["stub"] is False
                assert len(integrity_data.keys()) == 7
                assert integrity_data["byte_count"] == 10**8
                assert "access_date" in integrity_data.keys()
                break
            count += 1
            # Assert that the upload item exists in the list
            assert count < len(supplement_json["integrity_data"])

        count = 0
        test_upload_path = os.path.join(
            upload_path, os.path.relpath(file_path3, self.temp_dir_path)
        )
        for integrity_data in supplement_json["integrity_data"]:
            if integrity_data["source_path"] == file_path3:
                assert integrity_data["upload_path"] == test_upload_path
                assert (
                    integrity_data["hash"] == "e9a92a2ed0d53732ac13b031"
                    "a27b071814231c8633c9f41844ccba884d482b16"
                )
                assert integrity_data["file_size"] == 7
                assert integrity_data["stub"] is False
                assert len(integrity_data.keys()) == 7
                assert integrity_data["byte_count"] == 10**8
                assert "access_date" in integrity_data.keys()
                break
            count += 1
            # Assert that the upload item exists in the list
            assert count < len(supplement_json["integrity_data"])

        count = 0
        test_upload_path = os.path.join(
            upload_path, os.path.relpath(file_path4, self.temp_dir_path)
        )
        for integrity_data in supplement_json["integrity_data"]:
            if integrity_data["source_path"] == file_path4:
                assert integrity_data["upload_path"] == test_upload_path
                assert (
                    integrity_data["hash"] == "cd6f6854353f68f47c9c932"
                    "17c5084bc66ea1af918ae1518a2d715a1885e1fcb"
                )
                assert integrity_data["file_size"] == 2
                assert integrity_data["stub"] is False
                assert len(integrity_data.keys()) == 7
                assert integrity_data["byte_count"] == 10**8
                assert "access_date" in integrity_data.keys()
                break
            count += 1
            # Assert that the upload item exists in the list
            assert count < len(supplement_json["integrity_data"])

        count = 0
        for integrity_data in supplement_json["integrity_data"]:
            if integrity_data["source_path"] == file_path_stub1:
                assert integrity_data["upload_path"] == "stub"
                assert (
                    integrity_data["hash"] == "e61b1cb2ee205f4abff78a060"
                    "42921bae398587780f434e14677c12bd6288a3e"
                )
                assert integrity_data["file_size"] == 5
                assert integrity_data["stub"] is True
                assert len(integrity_data.keys()) == 7
                assert integrity_data["byte_count"] == 10**8
                assert "access_date" in integrity_data.keys()
                break
            count += 1
            # Assert that the upload item exists in the list
            assert count < len(supplement_json["integrity_data"])

        count = 0
        for integrity_data in supplement_json["integrity_data"]:
            if integrity_data["source_path"] == file_path_stub2:
                assert integrity_data["upload_path"] == "stub"
                assert (
                    integrity_data["hash"] == "13615ecb0f24bab4cb4c20a"
                    "7dc9cb3ef3fed6914e4750078493e722a8514e965"
                )
                assert integrity_data["file_size"] == 3
                assert integrity_data["stub"] is True
                assert len(integrity_data.keys()) == 7
                assert integrity_data["byte_count"] == 10**8
                assert "access_date" in integrity_data.keys()
                break
            count += 1
            # Assert that the upload item exists in the list
            assert count < len(supplement_json["integrity_data"])

        assert supplement_json["upload_items"] == upload_items