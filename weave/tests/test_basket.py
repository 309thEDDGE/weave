"""Pytest tests for basket.py related functionality."""

import json
import os
import re
import tempfile
import shutil
from pathlib import Path

import pytest
import s3fs
import pandas as pd
import fsspec
from fsspec.implementations.local import LocalFileSystem

from weave.__init__ import __version__ as weave_version
from weave.basket import Basket
from weave.index.create_index import create_index_from_fs
from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.tests.pytest_resources import PantryForTest, get_file_systems

###############################################################################
#                      Pytest Fixtures Documentation:                         #
#            https://docs.pytest.org/en/7.3.x/how-to/fixtures.html            #
#                                                                             #
#                  https://docs.pytest.org/en/7.3.x/how-to/                   #
#          fixtures.html#teardown-cleanup-aka-fixture-finalization            #
#                                                                             #
#  https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize  #
###############################################################################

# Pylint doesn't like redefining the test fixture here from
# test_basket, but this is the right way to do this if at some
# point in the future the two need to be differentiated.
# pylint: disable=duplicate-code

# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    name="test_pantry",
    params=file_systems,
    ids=file_systems_ids,
)
def fixture_test_pantry(request, tmpdir):
    """Fixture to set up and tear down test_basket."""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


def test_basket_basket_path_is_pathlike():
    """Test that an error is returned when trying to instantiate a basket with
    invalid basket address type.
    """

    basket_path = 1
    with pytest.raises(
        TypeError,
        match="expected str, bytes or os.PathLike object, not int",
    ):
        Basket(
            basket_path,
            file_system=file_systems[0],
        )


def test_basket_address_does_not_exist(test_pantry):
    """Test that an error is raised when trying to instantiate a basket with an
    invalid basket address.
    """

    basket_path = Path("i n v a l i d p a t h")
    pantry = Pantry(IndexPandas,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    with pytest.raises(
        ValueError, match=f"Basket does not exist: {basket_path}"
    ):
        Basket(
            Path(basket_path),
            file_system=test_pantry.file_system,
            pantry=pantry
        )


def test_make_basket_with_uuid_stays_in_pantry(test_pantry):
    """Tests the pantry does not access baskets outside of itself."""
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system
    )

    # Save and delete the basket.
    pantry.index.generate_index()
    index = pantry.index.to_pandas_df()
    pantry.index.untrack_basket(index.iloc[0].uuid)

    # Modify the basket address to a new (fake) pantry.
    address = index.iloc[0].address
    address = address.split(os.path.sep)
    address[0] += "-2"
    new_address = (os.path.sep).join(address)
    index.at[0,"address"] = new_address

    # Track the new basket
    pantry.index.track_basket(index)

    error_msg = f"Attempting to access basket outside of pantry: {new_address}"
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        Basket(index.iloc[0].uuid, pantry=pantry)


def test_basket_no_manifest_file(test_pantry):
    """Test that an error is raised when attempting to instantiate a basket
    with a missing basket manifest file.
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
        match=re.escape(
            "Invalid Basket, basket_manifest.json "
            f"does not exist: {manifest_path}"
        ),
    ):
        Basket(
            Path(basket_path),
            file_system=test_pantry.file_system
        )


def test_basket_no_supplement_file(test_pantry):
    """Test that an error is raised when attempting to instantiate a basket
    with a missing basket supplement file.
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
        match=re.escape(
            "Invalid Basket, basket_supplement.json "
            f"does not exist: {supplement_path}"
        ),
    ):
        Basket(
            Path(basket_path),
            file_system=test_pantry.file_system
        )


def test_basket_get_manifest(test_pantry):
    """Test that the manifest of an uploaded basket is correctly retrieved
    using the get_manifest function.
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
        "weave_version": weave_version,
    }


def test_basket_get_manifest_cached(test_pantry):
    """Test that the get_manifest function retreives the cached copy."""
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
        "weave_version": weave_version,
    }


def test_basket_get_supplement(test_pantry):
    """Test that the get_supplement function returns the expected values."""
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
    """Test that the get_supplement function retrieves cached copies of a
    basket's supplement.
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
    """Test that the get_metadata function returns the expected values."""
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

    # Check get_metadata returns the same values used during the upload.
    metadata = basket.get_metadata()
    assert metadata_in == metadata


def test_basket_get_metadata_cached(test_pantry):
    """Test that the get_metadata function retrieves cached copies of a
    basket's metadata.
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
    """Test that get_metadata returns None when no metadata was uploaded."""
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
    """Test that the basket ls function returns the expected values."""
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    uploaded_dir_path = os.path.join(basket_path, tmp_basket_dir_name)
    assert Path(basket.ls()[0]).match(uploaded_dir_path)


def test_basket_ls_relpath(test_pantry):
    """Test that the basket ls function works when using relative paths."""
    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    uploaded_file_path = os.path.join(basket_path,
                                      tmp_basket_dir_name,
                                      "test.txt")
    assert Path(basket.ls(tmp_basket_dir_name)[0]).match(uploaded_file_path)


def test_basket_ls_relpath_period(test_pantry):
    """Test that the basket ls function works when using the relative path '.'
    """

    # Create a temporary basket with a test file, and upload it.
    tmp_basket_dir_name = "test_basket_tmp_dir"
    tmp_basket_dir = test_pantry.set_up_basket(tmp_basket_dir_name)
    basket_path = test_pantry.upload_basket(tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        file_system=test_pantry.file_system
    )

    uploaded_dir_path = os.path.join(basket_path, tmp_basket_dir_name)
    assert Path(basket.ls(".")[0]).match(uploaded_dir_path)


def test_basket_ls_is_pathlike(test_pantry):
    """Test that the basket ls function only works with the expected
    value types.
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
    """The s3fs.S3FileSystem.ls() func is broken after running {}.find().

    This function is primarily to test s3fs file systems, but local
    file systems should yield the same results.

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

    # Create index on pantry
    create_index_from_fs(test_pantry.pantry_path, test_pantry.file_system)

    # Run find in case index creation changes
    test_pantry.file_system.find(test_pantry.pantry_path)

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
    # the script was called might be prepended, clean up stuff like that here)
    actual_bdp = [
        Path(x).match(z)
        for x, z in zip(ls_test, expected_base_dir_paths, strict=True)
    ]

    # Check false is not in actual_bdp--which is a list of booleans that
    # indicates if the indices match.
    assert False not in actual_bdp


def test_basket_init_from_uuid(test_pantry):
    """Test that a basket can be successfully initialized from a UUID.
    """

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    uuid = "0000"
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    pantry = Pantry(IndexPandas,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    test_basket = Basket(
        basket_address=uuid,
        pantry=pantry,
    )
    assert Path(test_basket.ls("basket_one")[0]).match(
        os.path.join(test_pantry.pantry_path, "test_basket",
                     "0000", "basket_one", "test.txt")
    )


def test_basket_init_fails_if_uuid_does_not_exist(test_pantry):
    """Test that an error is raised when trying to initialize a basket using a
    UUID that does not have an associated basket.
    """

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    uuid = "0000"
    bad_uuid = "a bad uuid"
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    pantry = Pantry(IndexPandas,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    with pytest.raises(ValueError, match=f"Basket does not exist: {bad_uuid}"):
        Basket(
            basket_address=bad_uuid,
            pantry=pantry,
        )


def test_basket_pantry_name_does_not_exist(test_pantry):
    """Test than an error is raised when trying to initialize a basket using a
    UUID, but using a bucket name that does not exist.
    """

    # Put basket in the temporary bucket
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    uuid = "0000"
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid=uuid)
    pantry_path = "the wrong pantry 007"
    if isinstance(test_pantry.file_system, s3fs.S3FileSystem):
        error_msg = "Connection to s3fs failed."
        with pytest.raises(ConnectionError, match=error_msg):
            pantry = Pantry(IndexPandas,
                pantry_path=pantry_path,
                file_system=test_pantry.file_system)
            pantry.index.generate_index()
    else:
        error_msg = f"Invalid pantry Path. Pantry does not exist at: " \
            f"{pantry_path}"
        with pytest.raises(ValueError, match=error_msg):
            pantry = Pantry(IndexPandas,
                pantry_path=pantry_path,
                file_system=test_pantry.file_system)
            pantry.index.generate_index()

def test_basket_from_uuid_with_many_baskets(test_pantry):
    """Test that many baskets can be initialized using UUIDs."""
    # Set up ten baskets
    for uuid in range(10):
        uuid = str(uuid)
        tmp_basket_dir = test_pantry.set_up_basket(f"temp_basket_{uuid}")
        test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    pantry = Pantry(IndexPandas,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    test_basket = Basket(
        basket_address=uuid,
        pantry=pantry,
    )
    assert Path(test_basket.ls(f"temp_basket_{uuid}")[0]).match(
        os.path.join(test_pantry.pantry_path, "test_basket", uuid,
            f"temp_basket_{uuid}", "test.txt")
    )


def test_basket_correct_weave_version_member_variable(test_pantry):
    """Test that basket has the correct weave version as a member variable
    """
    tmp_basket_dir = test_pantry.set_up_basket("basket_one")
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir)

    basket = Basket(
        Path(basket_path),
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system
    )

    assert basket.weave_version == weave_version


def test_basket_check_member_variables(test_pantry):
    """Check that you can access member variables in the basket"""
    # Upload a basket
    tmp_basket_dir = test_pantry.set_up_basket("basket")
    uuid = "0000"
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir,
                                            uid=uuid)

    pantry = Pantry(IndexPandas,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    my_basket = Basket(
        basket_address=uuid,
        pantry=pantry,
    )

    # Open the manifest to get the file system data
    manifest_path = os.path.join(basket_path, "basket_manifest.json")

    with test_pantry.file_system.open(manifest_path, "rb") as file:
        manifest_dict = json.load(file)

    # Validate the basket object's member variables match the file system data
    assert manifest_dict["uuid"] == my_basket.uuid
    assert manifest_dict["upload_time"] == my_basket.upload_time
    assert manifest_dict["parent_uuids"] == my_basket.parent_uuids
    assert manifest_dict["basket_type"] == my_basket.basket_type
    assert manifest_dict["label"] == my_basket.label
    assert manifest_dict["weave_version"] == weave_version
    assert my_basket.address.endswith(basket_path)
    assert test_pantry.file_system.__class__.__name__ == my_basket.storage_type


def test_basket_to_pandas_df(test_pantry):
    """Check that to_pandas_df returns the proper dataframe for the basket"""
    # Upload a basket
    tmp_basket_dir = test_pantry.set_up_basket("basket")
    uuid = "0000"
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir,
                                            uid=uuid)

    pantry = Pantry(IndexPandas,
                    pantry_path=test_pantry.pantry_path,
                    file_system=test_pantry.file_system)
    pantry.index.generate_index()
    my_basket = Basket(
        basket_address=uuid,
        pantry=pantry,
    )

    basket_df = my_basket.to_pandas_df()

    # Open the manifest to get the file system data
    manifest_path = os.path.join(basket_path, "basket_manifest.json")

    with test_pantry.file_system.open(manifest_path, "rb") as file:
        manifest_dict = json.load(file)

    # Collect the data and build a dataframe for testing
    data = [manifest_dict["uuid"],
            manifest_dict["upload_time"],
            manifest_dict["parent_uuids"],
            manifest_dict["basket_type"],
            manifest_dict["label"],
            manifest_dict["weave_version"],
            basket_path,
            test_pantry.file_system.__class__.__name__]

    columns = ["uuid", "upload_time", "parent_uuids",
               "basket_type", "label", "weave_version",
               "address", "storage_type"]

    answer_df = pd.DataFrame(data=[data], columns=columns)

    # Addresses can have different prefixes in the paths, check that it ends
    # with the correct relative path
    assert basket_df["address"][0].endswith(answer_df["address"][0])

    # Drop the addresses that could be slightly different and compare the rest
    # of the dataframe
    basket_df.drop(columns="address", inplace=True)
    answer_df.drop(columns="address", inplace=True)
    assert basket_df.equals(answer_df)

def test_basket_time_is_utc(test_pantry):
    """Make sure time data is in UTC format"""
    # Upload a basket
    uuid = "0000"
    tmp_basket_dir = test_pantry.set_up_basket("basket")
    basket_path = test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir,
                                            uid=uuid)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")

    with test_pantry.file_system.open(manifest_path, "rb") as file:
        manifest_dict = json.load(file)

    # Regex to match ISO 8601 (UTC)
    regex = (
        r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-"
        r"(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):"
        r"([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?"
        r"(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"
    )

    match_iso8601 = re.compile(regex).match
    assert match_iso8601(str(manifest_dict['upload_time'])) is not None


def test_read_only_get_data():
    """Make a read-only pantry, retrieve a basket, and check that you can read
    the data
    """
    with tempfile.TemporaryDirectory(dir=".") as tmpdir:
        tmp_pantry = Pantry(IndexPandas,
                            pantry_path=tmpdir,
                            file_system=LocalFileSystem())
        tmp_file_path = os.path.join(tmpdir, "temp_basket.txt")
        with open(tmp_file_path, "w", encoding="utf-8") as tmp_file:
            basket_uuid = tmp_pantry.upload_basket(
                upload_items=[{"path":tmp_file.name, "stub":False}],
                basket_type="read_only",
            )["uuid"][0]

        zip_path = shutil.make_archive(
            os.path.join(tmpdir, "test_pantry"), "zip", tmpdir
        )

        read_only_fs = fsspec.filesystem("zip", fo=zip_path, mode="r")
        read_only_pantry = Pantry(IndexPandas,
                                  pantry_path="",
                                  file_system=read_only_fs)

        my_basket = Basket(os.path.join("read_only", basket_uuid),
                           pantry=read_only_pantry)

        # Check that manifest and supplement are returned and metadata is empty
        assert my_basket.get_manifest()
        assert my_basket.get_supplement()
        assert my_basket.get_metadata() == {}

        del read_only_pantry
        del read_only_fs
        del my_basket
