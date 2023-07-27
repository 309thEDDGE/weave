import uuid
import os
import pytest
import time
from datetime import datetime
from unittest.mock import patch

from weave import upload
from weave.uploader import upload_basket
from weave.uploader_functions import (derive_integrity_data,
                                      validate_upload_item)
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
        b_type = "test-basket"
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
            test_prefix="test-prefix",
        )

        self.uploaded_files = self.s3fs_client.ls(self.upload_path)

        return self.upload_path

@pytest.fixture
def set_up_tu(tmpdir):
    tu = UploadForTest(tmpdir)
    yield tu
    tu.cleanup_bucket()

def test_upload_test_txt_in_uploaded_files(set_up_tu):
    """
    Test that uploaded test files are properly uploaded.
    """
    tu = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tu.set_up_basket(tmp_basket_dir_name)
    upload_path = tu.run_uploader(tmp_basket_dir)

    assert (
        os.path.join(upload_path, "test.txt") in tu.uploaded_files
    )

def test_upload_basket_manifest_in_uploaded_files(set_up_tu):
    """
    Test that basket manifest files are properly uploaded.
    """
    tu = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tu.set_up_basket(tmp_basket_dir_name)
    upload_path = tu.run_uploader(tmp_basket_dir)

    assert (
        os.path.join(upload_path, "basket_manifest.json")
        in tu.uploaded_files
    )

def test_upload_basket_supplement_in_uploaded_files(set_up_tu):
    """
    Test that basket supplement files are properly uploaded.
    """
    tu = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tu.set_up_basket(tmp_basket_dir_name)
    upload_path = tu.run_uploader(tmp_basket_dir)

    assert (
        os.path.join(upload_path, "basket_supplement.json")
        in tu.uploaded_files
    )

def test_upload_basket_metadata_in_uploaded_files(set_up_tu):
    """
    Test that basket metadata files are properly uploaded.
    """
    tu = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tu.set_up_basket(tmp_basket_dir_name)
    upload_path = tu.run_uploader(tmp_basket_dir)

    assert (
        os.path.join(upload_path, "basket_metadata.json")
        in tu.uploaded_files
    )

def test_upload_nothing_else_in_uploaded_files(set_up_tu):
    """
    Test that only basket data and required files are uploaded.
    """
    tu = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tu.set_up_basket(tmp_basket_dir_name)
    tu.run_uploader(tmp_basket_dir)

    assert len(tu.uploaded_files) == 4

def test_upload_bucket_name_is_string(set_up_tu):
    """
    Test that an error is raised when the bucket name is not a string.
    """
    tu = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tu.set_up_basket(tmp_basket_dir_name)
    tu.run_uploader(tmp_basket_dir)

    bucket_name = 7

    upload_items = [{'path': str(os.path.join(tmp_basket_dir, "test.txt")),
                     'stub': False}]

    with pytest.raises(
        TypeError, match=f"'bucket_name' must be a string: '{bucket_name}'"
    ):
        upload(upload_items, "test_basket", bucket_name)

@pytest.fixture
def set_up_tb(tmpdir):
    tb = BucketForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()

def test_validate_upload_item_correct_schema_path_key():
    """
    Test that validate_upload_item raises a KeyError when an invalid path key
    is used.
    """
    file_path = "path/path"

    # Invalid Path Key
    upload_item = {"path_invalid_key": file_path, "stub": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'path_invalid_key'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_path_type():
    """
    Test that validate_upload_item raises a KeyError when an invalid upload 
    item type is used.
    """
    upload_item = {"path": 1234, "stub": True}
    with pytest.raises(
        TypeError, match="Invalid upload_item type: 'path: <class 'int'>'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_stub_key():
    """
    Test that validate_upload_item raises a KeyError when an invalid stub key
    is used.
    """
    file_path = "path/path"
    # Invalid Stub Key
    upload_item = {"path": file_path, "invalid_stub_key": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'invalid_stub_key'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_stub_type():
    """
    Test that validate_upload_item raises a KeyError when an invalid stub value
    type is used.
    """
    # Invalid Stub Type
    file_path = "path/path"
    upload_item = {"path": file_path, "stub": "invalid type"}
    with pytest.raises(
        TypeError, match="Invalid upload_item type: 'stub: <class 'str'>'"
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_correct_schema_extra_key():
    """
    Test that validate_upload_item raises a KeyError when an invalid extra key
    is used.
    """
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

def test_validate_upload_item_file_exists():
    """
    Test that validate_upload_item raises a FileExistsError when an invalid
    path value is used.
    """
    upload_item = {"path": "i n v a l i d p a t h", "stub": True}
    with pytest.raises(
        FileExistsError,
        match="'path' does not exist: 'i n v a l i d p a t h'",
    ):
        validate_upload_item(upload_item)

def test_validate_upload_item_folder_exists(set_up_tb):
    """
    Test that validate_upload_item does not raise an error when using a folder
    path.
    """
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

def test_validate_upload_item_validate_dictionary():
    """
    Test that validate_upload_item raises a TypeError when upload_item is not a
    dictionary.
    """
    upload_item = 5
    with pytest.raises(
        TypeError,
        match="'upload_item' must be a dictionary: 'upload_item = 5'",
    ):
        validate_upload_item(upload_item)


def test_derive_integrity_data_file_doesnt_exist():
    """
    Test that derive_integrity_data raises a FileExistsError when using a file
    path that does not exist.
    """
    file_path = "f a k e f i l e p a t h"
    with pytest.raises(
        FileExistsError, match=f"'file_path' does not exist: '{file_path}'"
    ):
        derive_integrity_data(file_path)

def test_derive_integrity_data_path_is_string():
    """
    Test that derive_integrity_data raises a TypeError when the file path is
    not a string.
    """
    file_path = 10
    with pytest.raises(
        TypeError, match=f"'file_path' must be a string: '{file_path}'"
    ):
        derive_integrity_data(file_path)

def test_derive_integrity_data_byte_count_string(set_up_tb):
    """
    Test that derive_integrity_data raises a TypeError when byte count is not
    an integer.
    """
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
    """
    Test that derive_integrity_data raises a TypeError when byte count is not
    an integer
    """
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
    """
    Test that derive_integrity_data raises a ValueError when byte count is not
    greater than 0.
    """
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
    """
    Test that derive_integrity_data returns the expected hash values when using
    large byte counts.
    """
    tb = set_up_tb

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    text_file_name = "test.txt"
    text_file_content = "0123456789"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=text_file_name,
                                      file_content=text_file_content)

    tmp_basket_file_path = tmp_basket_dir.join("test.txt").strpath

    # Expected sha256 hash of the string "0123456789". The whole file is used
    # as the file size is > 3*byte_count. 
    e_hash = "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882"
    assert e_hash == derive_integrity_data(tmp_basket_file_path, 10**6)["hash"]

def test_derive_integrity_data_small_byte_count(set_up_tb):
    """
    Test that derive_integrity_data returns the expected hash values when using
    small byte counts.
    """
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
    # beginning, middle, and end (instead of whole file content).
    e_hash = "a2a7cb1d7fc8f79e33b716b328e19bb381c3ec96a2dca02a3d1183e7231413bb"
    assert e_hash == derive_integrity_data(tmp_basket_file_path, 2)["hash"]

def test_derive_integrity_data_file_size(set_up_tb):
    """
    Test that derive_integrity_data returns the correct file size value.
    """
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
    """
    Test that derive_integrity_data returns the correct data access date.
    """
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
    """
    Test that derive_integrity_data returns the correct source path value.
    """
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
    """
    Test that derive_integrity_data returns the correct byte count value.
    """
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
    """
    Test that derive_integrity_data raises a ValueError when the passed in byte
    count is > 300,000,000 bytes
    """
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
    """
    Test that derive_integrity_data runs successfully when the passed in byte
    count is exactly 300,000,000 bytes
    """
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


def test_upload_basket_upload_items_is_not_a_string(set_up_tb):
    """
    Test that upload_basket raises a TypeError when upload_items is not a list
    of dictionaries.
    """
    tb = set_up_tb

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tb.set_up_basket(tmp_basket_dir_name)

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
    """
    Test that upload_basket raises a TypeError when upload_items is not a list
    of dictionaries.
    """
    tb = set_up_tb

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tb.set_up_basket(tmp_basket_dir_name)

    upload_items = ["invalid", "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)

    with pytest.raises(
        TypeError, match="'upload_items' must be a list of dictionaries:"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

def test_upload_basket_upload_items_is_a_list_of_only_dictionaries(set_up_tb):
    """
    Test that upload_basket raises a TypeError when upload_items is not a list
    of dictionaries.
    """
    tb = set_up_tb

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tb.set_up_basket(tmp_basket_dir_name)

    upload_items = [{}, "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)

    with pytest.raises(
        TypeError, match="'upload_items' must be a list of dictionaries:"
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

@patch(
    'weave.uploader_functions.UploadBasket.upload_basket_supplement_to_s3fs'
)
def test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails(mocked_obj,
                                                                    set_up_tb):
    """
    Test that upload_basket deletes bad upload items if it fails to upload.
    """
    tb = set_up_tb

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_dir = tb.set_up_basket(tmp_basket_dir_name)

    upload_items = [{"path": tmp_dir.strpath, "stub": False}]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)

    with pytest.raises(
        ValueError,
        match="This error provided for"
        "test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails"
    ):
        mocked_obj.side_effect = ValueError(
            "This error provided for"
            "test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails"
        )
        upload_basket(upload_items, upload_path, unique_id, basket_type)
    assert not tb.s3fs_client.exists(upload_path)

def test_upload_basket_upload_items_invalid_dictionary(set_up_tb):
    """
    Test that upload_basket raises a KeyError when upload_items contains an
    invalid path key.
    """
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
    """
    Test that upload_basket deletes a bad upload basket when upload items is 
    invalid.
    """
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
    """
    Test that upload_basket raises ValueErrors when upload_items does not
    contain unique file and folder names.
    """
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
    tmp_basket_dir_name2 = "test_basket_tmp_dir2"
    tmp_basket_dir2 = tb.set_up_basket(tmp_basket_dir_name2,
                                      file_name=tmp_basket_txt_file_name)
    tmp_basket_txt_file2 = tmp_basket_dir2.join(tmp_basket_txt_file_name)

    upload_path = os.path.join(tb.s3_bucket_name, basket_type, unique_id)

    # Test same file names
    upload_items = [
        {"path": tmp_basket_txt_file1.strpath, "stub": True },
        {"path": tmp_basket_txt_file2.strpath, "stub": True },
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test.txt",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    # Test same dirname
    upload_items = [
        {"path": tmp_basket_dir1.strpath, "stub": True },
        {"path": tmp_basket_dir1.strpath, "stub": True },
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test_basket_tmp_dir",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    # Test same dirname same file
    upload_items = [
        {"path": tmp_basket_txt_file1.strpath, "stub": True },
        {"path": tmp_basket_txt_file1.strpath, "stub": True },
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test.txt",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert not tb.s3fs_client.exists(f"{upload_path}")

def test_upload_basket_upload_path_is_string(set_up_tb):
    """
    Test that upload_basket raises a TypeError when upload_items is not a list
    of dictionaries.
    """
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

def test_upload_basket_unique_id_string(set_up_tb):
    """
    Test that upload_basket raises a TypeError when unique id is not a string.
    """
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

def test_upload_basket_type_is_string(set_up_tb):
    """
    Test that upload_basket raises TypeError when basket type is not a string.
    """
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

def test_upload_basket_parent_ids_list_str(set_up_tb):
    """
    Test that upload_basket raises a TypeError when parent ids is not a list
    of strings.
    """
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

def test_upload_basket_parent_ids_is_list(set_up_tb):
    """
    Test that upload_basket raises a TypeError when parent ids is not a list.
    """
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

def test_upload_basket_metadata_is_dictionary(set_up_tb):
    """
    Test that upload_basket raises a TypeError when metadata is not a
    dictionary.
    """
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

def test_upload_basket_label_is_string(set_up_tb):
    """
    Test that upload_basket raises a TypeError when the label is not a string.
    """
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
    """
    Test that no metadata is created if no metadata is passed to upload_bucket.
    """
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
    """
    Test that upload_basket raises a FileExistsError when the upload directory
    already exists.
    """
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

    tb.s3fs_client.upload(tmp_basket_dir.strpath,
                          f"{upload_path}",
                          recursive=True)

    with pytest.raises(
        FileExistsError,
        match=f"'upload_directory' already exists: '{upload_path}''",
    ):
        upload_basket(upload_items, upload_path, unique_id, basket_type)

    assert (
        tb.s3fs_client.ls(os.path.join(tb.s3_bucket_name, f"{basket_type}")) 
                             == [upload_path]
    )

def test_upload_basket_check_unallowed_file_names(set_up_tb):
    """
    Test that upload_basket raises a ValueError when trying to upload files
    with reserved/unallowed file names.
    """
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
    """
    Test that upload_basket cleans up failed basket uploads when any Exception 
    is encountered and the test_cleanup_flag is passed.
    """
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
    """
    Test that upload_basket raises a KeyError when an invalid optional argument
    is passed.
    """
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
    """
    Test that upload_basket raises a TypeError when the optional test_clean_up 
    argument is not a bool.
    """
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

def test_upload_basket_file_contents_identical(set_up_tb):
    tb = set_up_tb

    # Create a temporary basket with a test file.
    test_file_name = "test.txt"
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name,
                                      file_name=test_file_name)
    tmp_basket_file_path = tmp_basket_dir.join(test_file_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": False,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(tb.s3_bucket_name, f"{basket_type}", unique_id)
    upload_file_path = os.path.join(upload_path,
                                    tmp_basket_dir_name,
                                    test_file_name)

    upload_basket(
        upload_items,
        upload_path,
        unique_id,
        basket_type
    )

    # Read the file data, then assert the uploaded contents are the same.
    with open(tmp_basket_file_path, "r") as r_file:
        local_file_data = r_file.read()

    with tb.s3fs_client.open(upload_file_path, "r") as r_file:
        assert r_file.read() == local_file_data