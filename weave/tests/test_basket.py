import os
import json
from pathlib import Path

import pytest

from weave.basket import Basket
from weave.index import create_index_from_s3
from weave.tests.pytest_resources import BucketForTest

"""Pytest Fixtures Documentation:
https://docs.pytest.org/en/7.3.x/how-to/fixtures.html

https://docs.pytest.org/en/7.3.x/how-to
/fixtures.html#teardown-cleanup-aka-fixture-finalization"""

@pytest.fixture
def set_up_tb(tmpdir):
    tb = BucketForTest(tmpdir)
    yield tb
    tb.cleanup_bucket()

def test_basket_basket_path_is_pathlike(set_up_tb):
    """
    Test that we get an error when trying to instantiate a basket with invalid
    basket address type.
    """
    basket_path = 1
    with pytest.raises(
        TypeError,
        match="expected str, bytes or os.PathLike object, not int",
    ):
        Basket(basket_path)     

def test_basket_address_does_not_exist(set_up_tb):
    """
    Test that an error is raised when trying to instantiate a basket with an
    invalid basket address.
    """
    basket_path = Path("i n v a l i d p a t h")
    with pytest.raises(
        ValueError, match=f"Basket does not exist: {basket_path}"
    ):
        Basket(Path(basket_path))

def test_basket_no_manifest_file(set_up_tb):
    """
    Test that an error is raised when attempting to instantiate a basket with a
    missing basket manifest file.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)
    
    # Manually remove the basket_manifest file.
    manifest_path = os.path.join(s3_basket_path, "basket_manifest.json")
    tb.s3fs_client.rm(manifest_path)

    # Attempt to create a Basket from the malformed basket (missing manifest)
    with pytest.raises(
        FileNotFoundError,
        match=(
            "Invalid Basket, basket_manifest.json "
            + f"does not exist: {manifest_path}"
        ),
    ):
        Basket(Path(s3_basket_path))

def test_basket_no_suppl_file(set_up_tb):
    """
    Test that an error is raised when attempting to instantiate a basket with a
    missing basket supplement file.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # Manually remove the basket_supplement file.
    supplement_path = os.path.join(s3_basket_path, "basket_supplement.json")
    tb.s3fs_client.rm(supplement_path)

    # Attempt to create a Basket from the malformed basket (missing supplement)
    with pytest.raises(
        FileNotFoundError,
        match=(
            "Invalid Basket, basket_supplement.json "
            + f"does not exist: {supplement_path}"
        ),
    ):
        Basket(Path(s3_basket_path))

def test_basket_get_manifest(set_up_tb):
    """
    Test that the manifest of an uploaded basket is correctly retrieved using 
    the get_manifest function.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(Path(s3_basket_path))
    manifest = basket.get_manifest()
    assert manifest == {
        "uuid": "0000",
        "parent_uuids": [],
        "basket_type": "test_basket",
        "label": "",
        "upload_time": manifest["upload_time"],
    }

def test_basket_get_manifest_cached(set_up_tb):
    """
    Test that the get_manifest function retreives the cached copy.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(Path(s3_basket_path))

    # Read the basket_manifest.json file and store as a dictionary
    # in the object for later access.
    manifest = basket.get_manifest()
    manifest_path = basket.manifest_path

    # Manually replace the manifest file
    tb.s3fs_client.rm(manifest_path)
    with tb.s3fs_client.open(manifest_path, "w") as outfile:
        json.dump({"junk": "b"}, outfile)

    # Manifest should already be stored and the new file shouldn't be read.
    manifest = basket.get_manifest()
    assert manifest == {
        "uuid": "0000",
        "parent_uuids": [],
        "basket_type": "test_basket",
        "label": "",
        "upload_time": manifest["upload_time"],
    }

def test_basket_get_supplement(set_up_tb):
    """
    Test that the get_supplement function returns the expected values.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(Path(s3_basket_path))
    
    # Create a copy of the basket's expected upload_items.
    upload_items = [{"path": str(tmp_basket_dir.realpath()), "stub": False}]

    # Check the expected values are the same as the actual values.
    supplement = basket.get_supplement()
    assert supplement == {
        "upload_items": upload_items,
        "integrity_data": supplement["integrity_data"],
    }

def test_basket_get_supplement_cached(set_up_tb):
    """
    Test that the get_supplement function retrieves cached copies of a basket's
    supplement.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(Path(s3_basket_path))
    
    # Save the original basket supplement as a dictionary.
    original_supplement = basket.get_supplement()
    supplement_path = basket.supplement_path

    # Manually replace the Supplement file.
    tb.s3fs_client.rm(supplement_path)
    with tb.s3fs_client.open(supplement_path, "w") as outfile:
        json.dump({"junk": "b"}, outfile)

    # Supplement should already be cached and the new copy shouldn't be read.
    supplement = basket.get_supplement()
    assert supplement == {
        "upload_items": original_supplement["upload_items"],
        "integrity_data": original_supplement["integrity_data"],
    }

def test_basket_get_metadata(set_up_tb):
    """
    Test that the get_metadata function returns the expected values.
    """
    tb = set_up_tb
    
    metadata_in = {"test": 1}
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir, metadata=metadata_in)

    basket = Basket(Path(s3_basket_path))

    # Check get_metadata returns the same values we used during the upload.
    metadata = basket.get_metadata()
    assert metadata_in == metadata

def test_basket_get_metadata_cached(set_up_tb):
    """
    Test that the get_metadata function retrieves cached copies of a basket's
    metadata.
    """
    tb = set_up_tb
    
    metadata_in = {"test": 1}
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir, metadata=metadata_in)

    basket = Basket(Path(s3_basket_path))

    # Save the original basket metadata as a dictionary.
    metadata = basket.get_metadata()
    metadata_path = basket.metadata_path

    # Manually replace the metadata file
    tb.s3fs_client.rm(metadata_path)
    with tb.s3fs_client.open(metadata_path, "w") as outfile:
        json.dump({"junk": "b"}, outfile)

    # Metadata should already be cached and the new copy shouldn't be read.
    metadata = basket.get_metadata()
    assert metadata_in == metadata

def test_basket_get_metadata_none(set_up_tb):
    """
    Test that get_metadata returns None when no metadata was uploaded.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir)

    basket = Basket(Path(s3_basket_path))
    metadata = basket.get_metadata()
    
    # No metadata was added to the upload, so it should be None.
    assert metadata is None

def test_basket_ls(set_up_tb):
    """
    Test that the basket ls function returns the expected values.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir)
    
    basket = Basket(Path(s3_basket_path))
    
    uploaded_dir_path = f"{s3_basket_path}/{tmp_basket_dir_name}"
    assert basket.ls() == [uploaded_dir_path]

def test_basket_ls_relpath(set_up_tb):
    """
    Test that the basket ls function works when using relative paths.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir)
    
    basket = Basket(Path(s3_basket_path))
    
    uploaded_file_path = f"{s3_basket_path}/{tmp_basket_dir_name}/test.txt"    
    assert basket.ls(Path(tmp_basket_dir_name)) == [uploaded_file_path]

def test_basket_ls_relpath_period(set_up_tb):
    """
    Test that the basket ls function works when using the relative path '.'
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir)
    
    basket = Basket(Path(s3_basket_path))
    
    uploaded_dir_path = f"{s3_basket_path}/{tmp_basket_dir_name}"
    assert basket.ls(".") == [uploaded_dir_path]

def test_basket_ls_is_pathlike(set_up_tb):
    """
    Test that the basket ls function only works with the expected value types.
    """
    tb = set_up_tb
    
    # Create a tmp basket and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    s3_basket_path = tb.upload_basket(tmp_basket_dir)
    
    basket = Basket(Path(s3_basket_path))

    with pytest.raises(
        TypeError,
        match="expected str, bytes or os.PathLike object, not int",
    ):
        basket.ls(1)

def test_basket_ls_after_find(set_up_tb):
    """The s3fs.S3FileSystem.ls() func is broken after running {}.find()

    s3fs.S3FileSystem.find() function is called during index creation. The
    solution to this problem is to ensure Basket.ls() uses the argument
    refresh=True when calling s3fs.ls(). This ensures that cached results
    from s3fs.find() (which is called during create_index_from_s3() and do not
    include directories) do not affect the s3fs.ls() function used to enable
    the Basket.ls() function.
    """
    # set_up_tb is at this point a class object, but it's a weird name
    # because it looks like a function name (because it was before pytest
    # did weird stuff to it) so I just rename it to tb for reading purposes
    tb = set_up_tb
    tmp_basket_dir_name = "test_basket_temp_dir"
    tmp_basket_dir = tb.set_up_basket(tmp_basket_dir_name)
    tmp_basket_dir = tb.add_lower_dir_to_temp_basket(tmp_basket_dir)
    s3_basket_path = tb.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # create index on bucket
    create_index_from_s3(tb.s3_bucket_name)

    # run find in case index creation changes
    tb.s3fs_client.find(tb.s3_bucket_name)

    # set up basket
    test_b = Basket(s3_basket_path)
    what_should_be_in_base_dir_path = {
        os.path.join(s3_basket_path, tmp_basket_dir_name, i)
        for i in ["nested_dir", "test.txt"]
    }
    ls = test_b.ls(tmp_basket_dir_name)
    assert set(ls) == what_should_be_in_base_dir_path

def test_basket_init_from_uuid(set_up_tb):
    """
    Test that we can successfully initialize a basket from a UUID.
    """
    tb = set_up_tb
    
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    uuid = "0000"
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    test_b = Basket(basket_address=uuid, bucket_name=tb.s3_bucket_name)
    assert test_b.ls("basket_one") == [
        "pytest-temp-bucket/test_basket/0000/basket_one/test.txt"
    ]

def test_basket_init_fails_if_uuid_does_not_exist(set_up_tb):
    """
    Test that an error is raised when trying to initialize a basket using a
    UUID that does not have an associated basket.
    """
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    uuid = "0000"
    bad_uuid = "a bad uuid"
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    with pytest.raises(
        ValueError, match=f"Basket does not exist: {bad_uuid}"
    ):
        Basket(basket_address=bad_uuid, bucket_name=tb.s3_bucket_name)

def test_basket_bucket_name_does_not_exist(set_up_tb):
    """
    Test than an error is raised when trying to initialize a basket using a
    UUID, but using a bucket name that does not exist.
    """
    tb = set_up_tb
    # Put basket in the temporary bucket
    tmp_basket_dir_one = tb.set_up_basket("basket_one")
    uuid = "0000"
    tb.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    with pytest.raises(
        ValueError, match=f"Basket does not exist: {uuid}"
    ):
        Basket(basket_address=uuid, bucket_name="the wrong basket 007")

def test_basket_from_uuid_with_many_baskets(set_up_tb):
    """
    Test that we can initialize many baskets using UUIDs.
    """
    tb = set_up_tb
    # Set up ten baskets
    for uuid in range(10):
        uuid = str(uuid)
        tmp_basket_dir = tb.set_up_basket(f"temp_basket_{uuid}")
        tb.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)
    test_b = Basket(basket_address=uuid, bucket_name=tb.s3_bucket_name)
    assert test_b.ls(f"temp_basket_{uuid}") == [
        f"pytest-temp-bucket/test_basket/{uuid}/temp_basket_{uuid}/test.txt"
    ]