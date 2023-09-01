"""PyTest Tests for basket.py related functionality"""
import json
import os
from pathlib import Path

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave.basket import Basket
from weave.index.create_index import create_index_from_fs
from weave.pantry import Pantry
from weave.tests.pytest_resources import BucketForTest

###############################################################################
#                      Pytest Fixtures Documentation:                         #
#            https://docs.pytest.org/en/7.3.x/how-to/fixtures.html            #
#                                                                             #
#                  https://docs.pytest.org/en/7.3.x/how-to/                   #
#          fixtures.html#teardown-cleanup-aka-fixture-finalization            #
#                                                                             #
#  https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize  #
###############################################################################

# Pylint doesn't like that we are redefining the test fixture here from
# test_basket, but I think this is the right way to do this in case at some
# point in the future we need to differentiate the two.
# pylint: disable=duplicate-code

s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(params=[s3fs, local_fs])
def test_pantry(request, tmpdir):
    """Fixture to set up and tear down test_basket"""
    file_system = request.param
    test_bucket = BucketForTest(tmpdir, file_system)
    yield test_bucket
    test_bucket.cleanup_bucket()


def test_basket_basket_path_is_pathlike():
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


# We need to ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name


def test_basket_address_does_not_exist(test_pantry):
    """
    Test that an error is raised when trying to instantiate a basket with an
    invalid basket address.
    """
    basket_path = Path("i n v a l i d p a t h")
    with pytest.raises(
        ValueError, match=f"Basket does not exist: {basket_path}"
    ):
        Basket(
            Path(basket_path),
            file_system=test_pantry.file_system
        )


def test_basket_no_manifest_file(test_pantry):
    """
    Test that an error is raised when attempting to instantiate a basket with a
    missing basket manifest file.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # Manually remove the basket_manifest file.
    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    test_pantry.file_system.rm(manifest_path)

    # Attempt to create a Basket from the malformed basket (missing manifest)
    with pytest.raises(
        FileNotFoundError,
        match=(
            "Invalid Basket, basket_manifest.json "
            + f"does not exist: {manifest_path}"
        ),
    ):
        Basket(
            Path(basket_path),
            file_system=test_pantry.file_system
        )


def test_basket_no_suppl_file(test_pantry):
    """
    Test that an error is raised when attempting to instantiate a basket with a
    missing basket supplement file.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # Manually remove the basket_supplement file.
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_pantry.file_system.rm(supplement_path)

    # Attempt to create a Basket from the malformed basket (missing supplement)
    with pytest.raises(
        FileNotFoundError,
        match=(
            "Invalid Basket, basket_supplement.json "
            + f"does not exist: {supplement_path}"
        ),
    ):
        Basket(
            Path(basket_path),
            file_system=test_pantry.file_system
        )


def test_basket_get_manifest(test_pantry):
    """
    Test that the manifest of an uploaded basket is correctly retrieved using
    the get_manifest function.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )
    manifest = basket.get_manifest()
    assert manifest == {
        "uuid": "0000",
        "parent_uuids": [],
        "basket_type": "test_basket",
        "label": "",
        "upload_time": manifest["upload_time"],
    }


def test_basket_get_manifest_cached(test_pantry):
    """
    Test that the get_manifest function retreives the cached copy.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    # Read the basket_manifest.json file and store as a dictionary for later.
    manifest = basket.get_manifest()
    manifest_path = basket.manifest_path

    # Manually replace the manifest file.
    test_pantry.file_system.rm(manifest_path)
    with test_pantry.file_system.open(manifest_path, "w") as outfile:
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


def test_basket_get_supplement(test_pantry):
    """
    Test that the get_supplement function returns the expected values.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    # Create a copy of the basket's expected upload_items.
    upload_items = [{"path": str(tmp_basket_dir.realpath()), "stub": False}]

    # Check the expected values are the same as the actual values.
    supplement = basket.get_supplement()
    assert supplement == {
        "upload_items": upload_items,
        "integrity_data": supplement["integrity_data"],
    }


def test_basket_get_supplement_cached(test_pantry):
    """
    Test that the get_supplement function retrieves cached copies of a basket's
    supplement.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    # Save the original basket supplement as a dictionary.
    original_supplement = basket.get_supplement()
    supplement_path = basket.supplement_path

    # Manually replace the Supplement file.
    test_pantry.file_system.rm(supplement_path)
    with test_pantry.file_system.open(supplement_path, "w") as outfile:
        json.dump({"junk": "b"}, outfile)

    # Supplement should already be cached and the new copy shouldn't be read.
    supplement = basket.get_supplement()
    assert supplement == {
        "upload_items": original_supplement["upload_items"],
        "integrity_data": original_supplement["integrity_data"],
    }


def test_basket_get_metadata(test_pantry):
    """
    Test that the get_metadata function returns the expected values.
    """
    metadata_in = {"test": 1}

    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(
        tmp_basket_dir, metadata=metadata_in
    )

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    # Check get_metadata returns the same values we used during the upload.
    metadata = basket.get_metadata()
    assert metadata_in == metadata


def test_basket_get_metadata_cached(test_pantry):
    """
    Test that the get_metadata function retrieves cached copies of a basket's
    metadata.
    """
    metadata_in = {"test": 1}

    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(
        tmp_basket_dir, metadata=metadata_in
    )

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    # Save the original basket metadata as a dictionary.
    metadata = basket.get_metadata()
    metadata_path = basket.metadata_path

    # Manually replace the metadata file
    test_pantry.file_system.rm(metadata_path)
    with test_pantry.file_system.open(metadata_path, "w") as outfile:
        json.dump({"junk": "b"}, outfile)

    # Metadata should already be cached and the new copy shouldn't be read.
    metadata = basket.get_metadata()
    assert metadata_in == metadata


def test_basket_get_metadata_none(test_pantry):
    """
    Test that get_metadata returns None when no metadata was uploaded.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )
    metadata = basket.get_metadata()

    # No metadata was added to the upload, so it should be None.
    assert metadata is None


def test_basket_ls(test_pantry):
    """
    Test that the basket ls function returns the expected values.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    uploaded_dir_path = f"{basket_path}/{tmp_basket_dir_name}"
    assert basket.ls()[0].endswith(uploaded_dir_path)


def test_basket_ls_relpath(test_pantry):
    """
    Test that the basket ls function works when using relative paths.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    uploaded_file_path = f"{basket_path}/{tmp_basket_dir_name}/test.txt"
    assert basket.ls(Path(tmp_basket_dir_name))[0].endswith(uploaded_file_path)


def test_basket_ls_relpath_period(test_pantry):
    """
    Test that the basket ls function works when using the relative path '.'
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    uploaded_dir_path = f"{basket_path}/{tmp_basket_dir_name}"
    assert basket.ls(".")[0].endswith(uploaded_dir_path)


def test_basket_ls_is_pathlike(test_pantry):
    """
    Test that the basket ls function only works with the expected value types.
    """
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    with pytest.raises(
        TypeError, match="expected str, bytes or os.PathLike object, not int"
    ):
        basket.ls(1)


def test_basket_ls_after_find(test_pantry):
    """The s3fs.S3FileSystem.ls() func is broken after running {}.find()

    This function is primarily to test s3fs file systems, but we test on local
    file systems as well, and should yeild the same results regardless of FS.

    s3fs.S3FileSystem.find() function is called during index creation. The
    solution to this problem is to ensure Basket.ls() uses the argument
    refresh=True when calling s3fs.ls(). This ensures that cached results
    from s3fs.find() (which is called during create_index_from_fs() and do not
    include directories) do not affect the s3fs.ls() function used to enable
    the Basket.ls() function.
    """
    tmp_basket_dir_name = "test_basket_temp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    tmp_basket_dir = test_pantry.add_lower_dir_to_temp_basket(tmp_basket_dir)
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # Create index on bucket
    create_index_from_fs(test_pantry.pantry_name, test_pantry.file_system)

    # Run find in case index creation changes
    test_pantry.file_system.find(test_pantry.pantry_name)

    # Set up basket
    test_basket = Basket(
        basket_path,
        file_system=test_pantry.file_system
    )

    expected_base_dir_paths = [
        os.path.join(basket_path, tmp_basket_dir_name, i)
        for i in ["nested_dir", "test.txt"]
    ]
    expected_base_dir_paths.sort()  # Sort to zip in same order

    ls_test = test_basket.ls(tmp_basket_dir_name)
    ls_test.sort()

    # Get the actual base dir paths (essentially stripping any FS specific
    # prefixes or conventions, ie in local file systems, the path to where
    # the script was called might be prepended, we clean stuff like that here)
    actual_bdp = [
        x.endswith(z)
        for x, z in zip(ls_test, expected_base_dir_paths, strict=True)
    ]

    # Check false is not in actual_bdp--which is a list of booleans that
    # indicates if the indices match.
    assert False not in actual_bdp


def test_basket_init_from_uuid(test_pantry):
    """
    Test that we can successfully initialize a basket from a UUID.
    """
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    uuid = "0000"
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    # TODO: Implement PandasIndex
    pantry = Pantry(PandasIndex, 
                    pantry_name=test_pantry.pantry_name, 
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    test_basket = Basket(
        basket_address=uuid,
        pantry=pantry,
    )
    assert test_basket.ls("basket_one")[0].endswith(
        f"{test_pantry.pantry_name}/test_basket/0000/basket_one/test.txt"
    )


def test_basket_init_fails_if_uuid_does_not_exist(test_pantry):
    """
    Test that an error is raised when trying to initialize a basket using a
    UUID that does not have an associated basket.
    """
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    uuid = "0000"
    bad_uuid = "a bad uuid"
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    # TODO: Implement PandasIndex
    pantry = Pantry(PandasIndex, 
                    pantry_name=test_pantry.pantry_name, 
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    with pytest.raises(ValueError, match=f"Basket does not exist: {bad_uuid}"):
        Basket(
            basket_address=bad_uuid,
            pantry=pantry,
        )


def test_basket_pantry_name_does_not_exist(test_pantry):
    """
    Test than an error is raised when trying to initialize a basket using a
    UUID, but using a bucket name that does not exist.
    """
    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    uuid = "0000"
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    # TODO: Implement PandasIndex
    pantry = Pantry(PandasIndex, 
            pantry_name="the wrong bucket 007", 
            file_system=test_pantry.file_system)
    pantry.index.generate_index()
    with pytest.raises(ValueError, match=f"Basket does not exist: {uuid}"):
        Basket(
            basket_address=uuid,
            pantry=pantry,
        )


def test_basket_from_uuid_with_many_baskets(test_pantry):
    """
    Test that we can initialize many baskets using UUIDs.
    """
    # Set up ten baskets
    for uuid in range(10):
        uuid = str(uuid)
        tmp_basket_dir = test_pantry.set_up_basket(f"temp_basket_{uuid}")
        test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    # TODO: Implement PandasIndex
    pantry = Pantry(PandasIndex, 
        pantry_name=test_pantry.pantry_name,
        file_system=test_pantry.file_system,
    )
    pantry.index.generate_index()
    test_basket = Basket(
        basket_address=uuid,
        pantry=pantry,
    )
    assert test_basket.ls(f"temp_basket_{uuid}")[0].endswith(
        f"{test_pantry.pantry_name}/test_basket/{uuid}"
        f"/temp_basket_{uuid}/test.txt"
    )
