"""Pytests for the uploader functionality."""

import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

import weave
from weave import Pantry, IndexPandas, IndexSQLite
from weave.tests.pytest_resources import PantryForTest, file_path_in_list
from weave.tests.pytest_resources import get_file_systems
from weave.upload import (
    UploadBasket,
    derive_integrity_data,
    validate_upload_item,
)

# This module is long and has many tests. Pylint is complaining that it is too
# long. This isn't necessarily bad in this case, as the alternative
# would be to write the tests continuuing in a different script, which would
# be unnecesarily complex.
# Disabling this warning for this script.
# pylint: disable=too-many-lines


class UploadForTest(PantryForTest):
    """Test class extended from PantryForTest to include custom call for
    upload.
    """

    def __init__(self, tmpdir, file_system):
        super().__init__(tmpdir, file_system)
        self.uploaded_files = None

    def run_uploader(self, tmp_basket_dir):
        """Wrapper to call the weave upload function.
        """

        upload_items = [
            {
                "path": str(os.path.join(tmp_basket_dir, "test.txt")),
                "stub": False,
            }
        ]
        basket_type = "test-basket"
        metadata = {"oh": "I don't know", "something": "stupid"}
        label = "my label"
        parent_ids = [uuid.uuid1().hex]

        upload_path = weave.upload.UploadBasket(
            upload_items=upload_items,
            basket_type=basket_type,
            pantry_path=self.pantry_path,
            parent_ids=parent_ids,
            metadata=metadata,
            label=label,
            file_system=self.file_system,
        ).get_upload_path()

        self.uploaded_files = self.file_system.ls(upload_path)

        return upload_path


# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    params=file_systems,
    ids=file_systems_ids,
)
def set_up_tu(request, tmpdir):
    """Sets up the test uploader."""
    file_system = request.param
    test_upload = UploadForTest(tmpdir, file_system)
    yield test_upload
    test_upload.cleanup_pantry()


# Ignoring pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name


def test_upload_test_txt_in_uploaded_files(set_up_tu):
    """Test that uploaded test files are properly uploaded."""
    test_uploader = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_uploader.set_up_basket(tmp_basket_dir_name)
    upload_path = test_uploader.run_uploader(tmp_basket_dir)

    assert file_path_in_list(
        os.path.join(upload_path, "test.txt"), test_uploader.uploaded_files
    )


def test_upload_basket_manifest_in_uploaded_files(set_up_tu):
    """Test that basket manifest files are properly uploaded."""
    test_uploader = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_uploader.set_up_basket(tmp_basket_dir_name)
    upload_path = test_uploader.run_uploader(tmp_basket_dir)

    assert file_path_in_list(
        os.path.join(upload_path, "basket_manifest.json"),
        test_uploader.uploaded_files,
    )


def test_upload_basket_supplement_in_uploaded_files(set_up_tu):
    """Test that basket supplement files are properly uploaded."""
    test_uploader = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_uploader.set_up_basket(tmp_basket_dir_name)
    upload_path = test_uploader.run_uploader(tmp_basket_dir)

    assert file_path_in_list(
        os.path.join(upload_path, "basket_supplement.json"),
        test_uploader.uploaded_files,
    )


def test_upload_basket_metadata_in_uploaded_files(set_up_tu):
    """Test that basket metadata files are properly uploaded."""
    test_uploader = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_uploader.set_up_basket(tmp_basket_dir_name)
    upload_path = test_uploader.run_uploader(tmp_basket_dir)

    assert file_path_in_list(
        os.path.join(upload_path, "basket_metadata.json"),
        test_uploader.uploaded_files,
    )


def test_upload_nothing_else_in_uploaded_files(set_up_tu):
    """Test that only basket data and required files are uploaded."""
    test_uploader = set_up_tu

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_uploader.set_up_basket(tmp_basket_dir_name)
    test_uploader.run_uploader(tmp_basket_dir)

    assert len(test_uploader.uploaded_files) == 4


def test_upload_pantry_path_is_string():
    """Test that an error is raised when the pantry name is not a string.
    """

    pantry_path = 7
    upload_items = [
        {"path": "this/doesnt/actually/matter/here", "stub": False}
    ]

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'pantry_path: must be type <class 'str'>'",
    ):
        weave.upload.UploadBasket(
            upload_items, basket_type="test_basket", pantry_path=pantry_path
        )


def test_validate_upload_item_correct_schema_path_key():
    """Test that validate_upload_item raises a KeyError when an invalid
    path key is used.
    """

    file_path = "path/path"

    # Invalid Path Key
    upload_item = {"path_invalid_key": file_path, "stub": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'path_invalid_key'"
    ):
        validate_upload_item(upload_item)


def test_validate_upload_item_correct_schema_path_type():
    """Test that validate_upload_item raises a KeyError when an invalid upload
    item type is used.
    """

    upload_item = {"path": 1234, "stub": True}
    with pytest.raises(
        TypeError, match="Invalid upload_item type: 'path: <class 'int'>'"
    ):
        validate_upload_item(upload_item)


def test_validate_upload_item_correct_schema_stub_key():
    """Test that validate_upload_item raises a KeyError when an invalid stub
    key is used.
    """

    file_path = "path/path"
    # Invalid Stub Key
    upload_item = {"path": file_path, "invalid_stub_key": True}
    with pytest.raises(
        KeyError, match="Invalid upload_item key: 'invalid_stub_key'"
    ):
        validate_upload_item(upload_item)


def test_validate_upload_item_correct_schema_stub_type():
    """Test that validate_upload_item raises a KeyError when an invalid stub
    value type is used.
    """

    # Invalid Stub Type
    file_path = "path/path"
    upload_item = {"path": file_path, "stub": "invalid type"}
    with pytest.raises(
        TypeError, match="Invalid upload_item type: 'stub: <class 'str'>'"
    ):
        validate_upload_item(upload_item)


def test_validate_upload_item_correct_schema_extra_key():
    """Test that validate_upload_item raises a KeyError when an invalid
    extra key is used.
    """

    file_path = "path/path"
    # Extra Key
    upload_item = {"path": file_path, "stub": True, "extra_key": True}
    with pytest.raises(KeyError, match="Invalid upload_item key: 'extra_key'"):
        validate_upload_item(upload_item)


def test_validate_upload_item_valid_inputs(tmp_path):
    """Test that no errors are raised when calling validate_upload_item on
    valid inputs.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    valid_upload_item = {"path": str(test_file), "stub": True}

    validate_upload_item(valid_upload_item)


def test_validate_upload_item_file_exists():
    """Test that validate_upload_item raises a FileExistsError when an invalid
    path value is used.
    """

    upload_item = {"path": "i n v a l i d p a t h", "stub": True}
    with pytest.raises(
        FileExistsError,
        match="'path' does not exist: 'i n v a l i d p a t h'",
    ):
        validate_upload_item(upload_item)


def test_validate_upload_item_folder_exists(tmp_path):
    """Test that validate_upload_item does not raise an error when using a
    folder path.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    # Test using the FOLDER path
    valid_upload_item = {"path": str(tmp_path), "stub": True}

    validate_upload_item(valid_upload_item)


def test_validate_upload_item_validate_dictionary():
    """Test that validate_upload_item raises a TypeError when upload_item is
    not a dictionary.
    """

    upload_item = 5
    with pytest.raises(
        TypeError,
        match="'upload_item' must be a dictionary: 'upload_item = 5'",
    ):
        validate_upload_item(upload_item)


def test_derive_integrity_data_file_doesnt_exist():
    """Test that derive_integrity_data raises a FileExistsError when using
    a file path that does not exist.
    """

    file_path = "f a k e f i l e p a t h"
    with pytest.raises(
        FileExistsError, match=f"'file_path' does not exist: '{file_path}'"
    ):
        derive_integrity_data(file_path)


def test_derive_integrity_data_path_is_string():
    """Test that derive_integrity_data raises a TypeError when the file path is
    not a string.
    """

    file_path = 10
    with pytest.raises(
        TypeError, match=f"'file_path' must be a string: '{file_path}'"
    ):
        derive_integrity_data(file_path)


def test_derive_integrity_data_byte_count_string(tmp_path):
    """Test that derive_integrity_data raises a TypeError when byte count is
    not an integer.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)
    byte_count_in = "invalid byte count"

    with pytest.raises(
        TypeError, match=f"'byte_count' must be an int: '{byte_count_in}'"
    ):
        derive_integrity_data(str(test_file), byte_count=byte_count_in)


def test_derive_integrity_data_byte_count_float(tmp_path):
    """Test that derive_integrity_data raises a TypeError when byte count is
    not an integer.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)
    byte_count_in = 6.5

    with pytest.raises(
        TypeError, match=f"'byte_count' must be an int: '{byte_count_in}'"
    ):
        derive_integrity_data(str(test_file), byte_count=byte_count_in)


def test_derive_integrity_data_byte_count_0(tmp_path):
    """Test that derive_integrity_data raises a ValueError when byte count is
    not greater than 0.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)
    byte_count_in = 0

    with pytest.raises(
        ValueError,
        match=f"'byte_count' must be greater than zero: '{byte_count_in}'",
    ):
        derive_integrity_data(str(test_file), byte_count=byte_count_in)


def test_derive_integrity_data_large_byte_count(tmp_path):
    """Test that derive_integrity_data returns the expected hash values
    when using large byte counts.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    # Expected sha256 hash of the string "0123456789". The whole file is used
    # as the file size is > 3*byte_count.
    e_hash = "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882"
    assert e_hash == derive_integrity_data(str(test_file), 10**6)["hash"]


def test_derive_integrity_data_small_byte_count(tmp_path):
    """Test that derive_integrity_data returns the expected hash values when
    using small byte counts.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    # Expected sha256 hash of the string "014589". This string is used as the
    # file size is <= 3*byte_count. So checksum is generated using bytes from
    # beginning, middle, and end (instead of whole file content).
    e_hash = "a2a7cb1d7fc8f79e33b716b328e19bb381c3ec96a2dca02a3d1183e7231413bb"
    assert e_hash == derive_integrity_data(str(test_file), 2)["hash"]


def test_derive_integrity_data_file_size(tmp_path):
    """Test that derive_integrity_data returns the correct file size value."""
    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    # Check the size of the file is accurate to the length of it's contents.
    assert derive_integrity_data(str(test_file), 2)["file_size"] == len(
        text_file_content
    )


def test_derive_integrity_data_date(tmp_path):
    """Test that derive_integrity_data returns the correct data access date."""
    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    access_date = derive_integrity_data(str(test_file), 2)["access_date"]
    access_date = datetime.fromisoformat(access_date)
    access_date_seconds = access_date.timestamp()
    now_seconds = time.time_ns() // 10**9
    diff_seconds = abs(access_date_seconds - now_seconds)
    assert diff_seconds < 60


def test_derive_integrity_data_source_path(tmp_path):
    """Test that derive_integrity_data returns the correct source path value.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    assert derive_integrity_data(str(test_file), 2)["source_path"] == str(
        test_file
    )


def test_derive_integrity_byte_count(tmp_path):
    """Test that derive_integrity_data returns the correct byte count value."""
    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    assert derive_integrity_data(str(test_file), 2)["byte_count"] == 2


def test_derive_integrity_data_max_byte_count_off_by_one(tmp_path):
    """Test that derive_integrity_data raises a ValueError when the passed in
    byte count is > 300,000,000 bytes.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    byte_count_in = 300 * 10**6 + 1

    with pytest.raises(
        ValueError,
        match=f"'byte_count' must be less "
        f"than or equal to 300000000 bytes: '{byte_count_in}'",
    ):
        derive_integrity_data(str(test_file), byte_count=byte_count_in)


def test_derive_integrity_data_max_byte_count_exact(tmp_path):
    """Test that derive_integrity_data runs successfully when the passed in
    byte count is exactly 300,000,000 bytes.
    """

    text_file_name = "test.txt"
    text_file_content = "0123456789"

    test_file = tmp_path / text_file_name
    test_file.write_text(text_file_content)

    byte_count_in = 300 * 10**6 + 1

    derive_integrity_data(str(test_file), byte_count=byte_count_in - 1)


# Test with two different fsspec file systems (top of file).
@pytest.fixture(params=file_systems, ids=file_systems_ids)
def test_basket(request, tmpdir):
    """Sets up pytest fixture."""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


def test_upload_basket_without_uuid_creates_uuid(test_basket):
    """Test that upload_basket creates a uuid when unique_id is not
    initialized.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    #Initialize all kwargs except unique_id
    upload_items = [{"path": tmp_dir.strpath, "stub": False}]
    basket_type = "test_basket"
    upload_path = os.path.join(test_basket.pantry_path, basket_type)

    uploading_basket = weave.upload.UploadBasket(
        upload_items=upload_items,
        upload_directory=upload_path,
        basket_type=basket_type,
        file_system=test_basket.file_system,
    )

    assert uploading_basket.kwargs.get("unique_id") is not None

    tmp_files = test_basket.file_system.ls(upload_path)
    manifest_path = [s for s in tmp_files if s.endswith('manifest.json')][0]
    with test_basket.file_system.open(manifest_path, "r", encoding="utf-8")\
            as outfile:
        manifest_data = json.load(outfile)

    assert manifest_data["uuid"] != "null"


def test_upload_basket_upload_items_is_not_a_string(test_basket):
    """Test that upload_basket raises a TypeError when upload_items is not a
    list of dictionaries.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = "n o t a r e a l p a t h"
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(test_basket.pantry_path, basket_type, unique_id)

    with pytest.raises(
        TypeError,
        match="'upload_items' must be a list of "
        + f"dictionaries: '{upload_items}'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(upload_path)


def test_upload_basket_upload_items_is_not_a_list_of_strings(test_basket):
    """Test that upload_basket raises a TypeError when upload_items is not a
    list of dictionaries.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = ["invalid", "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(test_basket.pantry_path, basket_type, unique_id)

    with pytest.raises(
        TypeError, match="'upload_items' must be a list of dictionaries:"
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(upload_path)


def test_upload_basket_upload_items_is_a_list_of_only_dictionaries(
    test_basket,
):
    """Test that upload_basket raises a TypeError when upload_items is not a
    list of dictionaries.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [{}, "invalid2"]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(test_basket.pantry_path, basket_type, unique_id)

    with pytest.raises(
        TypeError, match="'upload_items' must be a list of dictionaries:"
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(upload_path)


@patch("weave.upload.UploadBasket.upload_basket_supplement_to_fs")
def test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails(
    mocked_obj, test_basket
):
    """Test that upload_basket deletes bad upload items if it fails to upload.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [{"path": tmp_dir.strpath, "stub": False}]
    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(test_basket.pantry_path, basket_type, unique_id)

    with pytest.raises(
        ValueError,
        match="This error provided for"
        "test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails",
    ):
        mocked_obj.side_effect = ValueError(
            "This error provided for"
            "test_upload_basket_with_bad_upload_items_is_deleted_if_it_fails"
        )
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(upload_path)


def test_upload_basket_upload_items_invalid_dictionary(test_basket):
    """Test that upload_basket raises a KeyError when upload_items contains an
    invalid path key.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_txt_file_name = "test.txt"
    tmp_basket_dir = test_basket.set_up_basket(
        tmp_basket_dir_name, file_name=tmp_basket_txt_file_name
    )
    tmp_basket_txt_file = tmp_basket_dir.join(tmp_basket_txt_file_name)

    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    upload_path = os.path.join(test_basket.pantry_path, basket_type, unique_id)

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
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )


def test_upload_basket_upload_items_check_unique_file_folder_names(
    test_basket,
):
    """Test that upload_basket raises ValueErrors when upload_items does not
    contain unique file and folder names.
    """

    unique_id = uuid.uuid1().hex
    basket_type = "test_basket"
    tmp_basket_txt_file_name = "test.txt"

    # Create a temporary basket with a test file.
    tmp_basket_dir_name1 = "test_basket_tmp_dir"
    tmp_basket_dir1 = test_basket.set_up_basket(
        tmp_basket_dir_name1, file_name=tmp_basket_txt_file_name
    )
    tmp_basket_txt_file1 = tmp_basket_dir1.join(tmp_basket_txt_file_name)

    # Manually add another test file with the same name as the other.
    tmp_basket_dir_name2 = "test_basket_tmp_dir2"
    tmp_basket_dir2 = test_basket.set_up_basket(
        tmp_basket_dir_name2, file_name=tmp_basket_txt_file_name
    )
    tmp_basket_txt_file2 = tmp_basket_dir2.join(tmp_basket_txt_file_name)

    upload_path = os.path.join(test_basket.pantry_path, basket_type, unique_id)

    # Test same file names
    upload_items = [
        {"path": tmp_basket_txt_file1.strpath, "stub": True},
        {"path": tmp_basket_txt_file2.strpath, "stub": True},
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test.txt",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    # Test same dirname
    upload_items = [
        {"path": tmp_basket_dir1.strpath, "stub": True},
        {"path": tmp_basket_dir1.strpath, "stub": True},
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test_basket_tmp_dir",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    # Test same dirname same file
    upload_items = [
        {"path": tmp_basket_txt_file1.strpath, "stub": True},
        {"path": tmp_basket_txt_file1.strpath, "stub": True},
    ]
    with pytest.raises(
        ValueError,
        match="'upload_item' folder and file names must be unique:"
        " Duplicate Name = test.txt",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_upload_path_is_string(test_basket):
    """Test that upload_basket raises a TypeError when upload_items is not a
    list of dictionaries.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

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
        match="Invalid datatype: 'upload_directory: "
        "must be type <class 'str'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )


def test_upload_basket_unique_id_string(test_basket):
    """Test that upload_basket raises a TypeError when unique id is not a
    string.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = 6
    upload_path = os.path.join(
        test_basket.pantry_path, basket_type, f"{unique_id}"
    )

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'unique_id: must be " "type <class 'str'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_type_is_string(test_basket):
    """Test that upload_basket raises TypeError when basket type is not a
    string.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = 1234
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'basket_type: must be type <class 'str'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_parent_ids_list_str(test_basket):
    """Test that upload_basket raises a TypeError when parent ids is not a list
    of strings.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )
    parent_ids_in = ["a", 3]

    with pytest.raises(
        TypeError, match="'parent_ids' must be a list of strings:"
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            parent_ids=parent_ids_in,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_parent_ids_is_list(test_basket):
    """Test that upload_basket raises a TypeError when parent ids is
    not a list.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )
    parent_ids_in = 56

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'parent_ids: must be type <class 'list'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            parent_ids=parent_ids_in,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_metadata_is_dictionary(test_basket):
    """Test that upload_basket raises a TypeError when metadata is not a
    dictionary.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )
    metadata_in = "invalid"

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'metadata: must be type <class 'dict'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            metadata=metadata_in,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_label_is_string(test_basket):
    """Test that upload_basket raises a TypeError when the label is
    not a string.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )
    label_in = 1234

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'label: must be type <class 'str'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            label=label_in,
            file_system=test_basket.file_system,
        )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_no_metadata(test_basket):
    """Test that no metadata is created if no metadata is passed to 
    upload_pantry.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )

    UploadBasket(
        upload_items=upload_items,
        upload_directory=upload_path,
        unique_id=unique_id,
        basket_type=basket_type,
        file_system=test_basket.file_system,
    )

    # Assert metadata.json was not written
    assert not test_basket.file_system.exists(
        os.path.join(upload_path, "metadata.json")
    )


def test_upload_basket_check_existing_upload_path(test_basket):
    """Test that upload_basket raises a FileExistsError when the upload
    directory already exists.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )

    test_basket.file_system.upload(
        tmp_basket_dir.strpath, f"{upload_path}", recursive=True
    )

    with pytest.raises(
        FileExistsError,
        match="'upload_directory' already exists: ",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
        )

    assert Path(test_basket.file_system.ls(
        os.path.join(test_basket.pantry_path, f"{basket_type}")
    )[0]).match(upload_path)


def test_upload_basket_check_unallowed_file_names(test_basket):
    """Test that upload_basket raises a ValueError when trying to upload files
    with reserved/unallowed file names.
    """

    json_data = {"t": [1, 2, 3]}
    unallowed_file_names = [
        "basket_manifest.json",
        "basket_metadata.json",
        "basket_supplement.json",
    ]
    for ind, unallowed_file_name in enumerate(unallowed_file_names):
        # Create a temporary basket with a test file.
        tmp_basket_dir_name = f"test_basket_tmp_dir{ind}"
        tmp_basket_dir = test_basket.set_up_basket(
            tmp_basket_dir_name,
            file_name=unallowed_file_name,
            file_content=json_data,
        )

        unallowed_file_path = tmp_basket_dir.join(unallowed_file_name)

        upload_items = [
            {
                "path": unallowed_file_path.strpath,
                "stub": True,
            }
        ]

        basket_type = "test_basket"
        unique_id = uuid.uuid1().hex
        upload_path = os.path.join(
            test_basket.pantry_path, f"{basket_type}", unique_id
        )

        with pytest.raises(
            ValueError,
            match=f"'{unallowed_file_name}' filename not allowed",
        ):
            UploadBasket(
                upload_items=upload_items,
                upload_directory=upload_path,
                unique_id=unique_id,
                basket_type=basket_type,
                file_system=test_basket.file_system,
            )

    assert not test_basket.file_system.exists(f"{upload_path}")


def test_upload_basket_clean_up_on_error(test_basket):
    """Test that upload_basket cleans up failed basket uploads when any
    Exception is encountered and the test_cleanup_flag is passed.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )

    with pytest.raises(Exception, match="Test Clean Up"):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
            test_clean_up=True,
        )

    assert not test_basket.file_system.exists(upload_path)


def test_upload_basket_invalid_optional_argument(test_basket):
    """Test that upload_basket raises a KeyError when an invalid optional
    argument is passed.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )

    with pytest.raises(KeyError, match="Invalid kwargs argument: 'junk'"):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
            junk=True,
        )

    assert not test_basket.file_system.exists(upload_path)


def test_upload_basket_invalid_test_clean_up_datatype(test_basket):
    """Test that upload_basket raises a TypeError when the optional
    test_clean_up argument is not a bool.
    """

    # Create a temporary basket with a test file.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": True,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )

    with pytest.raises(
        TypeError,
        match="Invalid datatype: 'test_clean_up: must be type <class 'bool'>'",
    ):
        UploadBasket(
            upload_items=upload_items,
            upload_directory=upload_path,
            unique_id=unique_id,
            basket_type=basket_type,
            file_system=test_basket.file_system,
            test_clean_up="a",
        )

    assert not test_basket.file_system.exists(upload_path)


def test_upload_basket_file_contents_identical(test_basket):
    """Test that files uploaded using upload_basket are the same as local."""
    # Create a temporary basket with a test file.
    test_file_name = "test.txt"
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(
        tmp_basket_dir_name, file_name=test_file_name
    )
    tmp_basket_file_path = tmp_basket_dir.join(test_file_name)

    upload_items = [
        {
            "path": tmp_basket_dir.strpath,
            "stub": False,
        }
    ]

    basket_type = "test_basket"
    unique_id = uuid.uuid1().hex
    upload_path = os.path.join(
        test_basket.pantry_path, f"{basket_type}", unique_id
    )
    upload_file_path = os.path.join(
        upload_path, tmp_basket_dir_name, test_file_name
    )

    UploadBasket(
        upload_items=upload_items,
        upload_directory=upload_path,
        unique_id=unique_id,
        basket_type=basket_type,
        file_system=test_basket.file_system,
    )

    # Read the file data, then assert the uploaded contents are the same.
    with open(tmp_basket_file_path, "r", encoding="UTF-8") as r_file:
        local_file_data = r_file.read()

    with test_basket.file_system.open(upload_file_path, "r") as r_file:
        assert r_file.read() == local_file_data


def test_upload_correct_version_number(test_basket):
    """Test that when a basket is uploaded, the manifest contains the
    correct version of weave.
    """

    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_basket.set_up_basket(tmp_basket_dir_name)
    upload_path = test_basket.upload_basket(tmp_basket_dir)

    manifest_path = os.path.join(upload_path, "basket_manifest.json")
    with test_basket.file_system.open(manifest_path, "r") as file:
        manifest_dict = json.load(file)

    assert manifest_dict["weave_version"] == weave.__version__


def test_upload_metadata_only_basket(test_basket):
    """Try to upload a valid metadata-only basket
    """
    pantry = Pantry(IndexPandas,
                    pantry_path=test_basket.pantry_path,
                    file_system=test_basket.file_system)

    basket = pantry.upload_basket(upload_items=[],
                         basket_type="metadata_only",
                         parent_ids=["1"],
                         metadata={"metadata":"only"})

    assert len(basket) == 1


def test_upload_basket_no_files(test_basket):
    """Upload a basket with no files included, ensure an error is thrown.
    """
    pantry = Pantry(IndexPandas,
                    pantry_path=test_basket.pantry_path,
                    file_system=test_basket.file_system)

    with pytest.raises(
        ValueError,
        match=(r"Files are required to upload a basket. If you want a metadata"
               r"-only basket, please include metadata and parent uid\(s\)"),
    ):
        pantry.upload_basket(upload_items=[], basket_type="no_files",)


@pytest.mark.skipif(
    os.environ.get("S3_ENDPOINT", None) is None,
    reason="S3_ENDPOINT must be set to run this test.",
)
def test_upload_from_s3fs(test_basket):
    """Test that a basket can be uploaded from s3fs to the local file
    system or s3fs.
    """
    pantry_path = os.path.join(test_basket.pantry_path, "test-pantry-1")
    local_fs = LocalFileSystem()
    s3 = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )

    test_basket.file_system.mkdir(pantry_path)

    tmp_txt_file = test_basket.tmpdir.join("test.txt")
    tmp_txt_file.write("this is a test")

    nested_path = os.path.join(pantry_path, "text.txt")
    file_to_move = os.path.join(test_basket.pantry_path, "text.txt")

    if test_basket.file_system == local_fs:
        minio_path = "test-source-file-system"
        try:
            s3.mkdir(minio_path)
        except FileExistsError:
            s3.rm(minio_path,recursive=True)
            s3.mkdir(minio_path)
        file_to_move = os.path.join(minio_path, "test.txt")

    # Must upload a file because Minio will remove empty directories
    if test_basket.file_system == local_fs:
        s3.upload(str(tmp_txt_file.realpath()), file_to_move)
    else:
        test_basket.file_system.upload(str(tmp_txt_file.realpath()),
                                       file_to_move)
    test_basket.file_system.upload(str(tmp_txt_file.realpath()),
                                   nested_path)
    # Make the Pantries.
    pantry_1 = Pantry(
        IndexPandas,
        pantry_path=pantry_path,
        file_system=test_basket.file_system,
    )
    pantry_2 = Pantry(
        IndexSQLite,
        pantry_path=pantry_path,
        file_system=test_basket.file_system,
    )
    basket_uuid = pantry_1.upload_basket(
                           upload_items=[{'path': file_to_move,'stub':False}],
                           source_file_system=s3,
                           basket_type="test_s3fs_upload"
                           )["uuid"][0]
    basket_uuid2 = pantry_2.upload_basket(
                       upload_items=[{'path': file_to_move,'stub':False}],
                       source_file_system=s3,
                       basket_type="test_s3fs_upload"
                       )["uuid"][0]
    basket = pantry_1.get_basket(basket_uuid)
    basket2 = pantry_2.get_basket(basket_uuid2)
    if not test_basket.file_system == local_fs:
        assert (basket.ls()[0].endswith('text.txt') and
                basket2.ls()[0].endswith('text.txt'))
    else:
        assert (basket.ls()[0].endswith('test.txt') and
                basket2.ls()[0].endswith('test.txt'))
        s3.rm(minio_path,recursive=True)
    if local_fs.exists(pantry_2.index.db_path):
        local_fs.rm(pantry_2.index.db_path)
