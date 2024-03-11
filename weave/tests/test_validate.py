"""Pytests for the validate functionality."""

import json
import os
from pathlib import Path

import pytest

from weave import validate
from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.tests.pytest_resources import PantryForTest, get_file_systems

# This module is long and has many tests. Pylint is complaining that it is too
# long, and has too many local variables throughout each test.
# This isn't necessarily bad in this case, as the alternative
# would be to write the tests continuing in a different script, which would
# be unnecessarily complex.
# Disabling this warning for this script.
# pylint: disable=too-many-lines
# pylint: disable=too-many-locals


class ValidateForTest(PantryForTest):
    """A class to test functions in validate.py"""
    # The arguments for the below function should be changed,
    # as it is over-riding PantryForTest.set_up_basket. Pylint hates it.
    # pylint: disable-next=arguments-differ
    def set_up_basket(self, tmp_dir_name, **kwargs):
        """Overrides PantryForTest's set_up_basket to better test_validate.py

        Sets up the basket with a nested basket depending on the values of
        the boolean params when this is called. if the is_man is True, a
        nested manifest file will be put in the basket, same with is_sup for
        supplement and is_meta for metadata. Custom data can also be input
        for each of these files is the man_data, sup_data, and meta_data.

        Because the upload_basket function can't be directly controlled,
        if you want to change whether there is a manifest, supplement, or
        metadata file in the basket, use these set-up functions instead.
        Same if you want to change the schema to something invalid.

        Parameters
        ----------
        tmp_dir_name: str
            The directory name of where the nested basket will be.
        **is_man: bool (optional)
            A bool that signals if there should be a manifest file,
            defaults to no manifest.
        **is_sup: bool (optional)
            A bool that signals if there should be a supplement file,
            defaults to no supplement.
        **is_meta: bool (optional)
            A bool that signals if there should be a metadata file,
            defaults to no metadata.
        **man_data: str (optional)
            The json data to be put into the manifest file,
            defaults to a valid manifest schema.
        **sup_data: str (optional)
            The json data to be put into the supplement file,
            defaults to a valid supplement schema.
        **meta_data: str (optional)
            The json data to be put into the metadata file,
            defaults to a valid json object.

        Returns
        ----------
        A string of the directory where the basket was uploaded.
        """

        is_man=kwargs.get("is_man", False)
        is_sup=kwargs.get("is_sup", False)
        is_meta=kwargs.get("is_meta", False)
        man_data=kwargs.get("man_data", "")
        sup_data=kwargs.get("sup_data", "")
        meta_data=kwargs.get("meta_data", "")
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)

        tmp_basket_txt_file = tmp_basket_dir.join("test.txt")
        tmp_basket_txt_file.write("This is a test")

        if is_man:
            tmp_manifest = tmp_basket_dir.join("basket_manifest.json")

            # This gives a default valid manifest json schema
            if man_data == "":
                man_data = """{
                    "uuid": "str",
                    "upload_time": "1970-01-01T01:01:12+0:00",
                    "parent_uuids": [],
                    "basket_type": "basket type string",
                    "label": "label string",
                    "weave_version": "0.1.1"
                }"""

            tmp_manifest.write(man_data)

        if is_sup:
            tmp_supplement = tmp_basket_dir.join("basket_supplement.json")

            # This gives a default valid supplement json schema
            if sup_data == "":
                sup_data = """{
                    "upload_items":
                    [
                    { "path": "test.txt", "stub": false}
                    ],

                    "integrity_data":
                    [
                    {
                        "file_size": 33,
                        "hash": "string",
                        "access_date": "string",
                        "source_path": "string",
                        "byte_count": 1,
                        "stub": false,
                        "upload_path": "test.txt"
                    }
                    ]
                }"""

            tmp_supplement.write(sup_data)

        if is_meta:
            tmp_metadata = tmp_basket_dir.join("basket_metadata.json")

            # This gives a default valid metadata json structure
            if meta_data == "":
                meta_data = """{"Test":1, "test_bool":55}"""

            tmp_metadata.write(meta_data)

        return tmp_basket_dir

    def add_lower_dir_to_temp_basket(
        self, tmp_basket_dir, new_dir_name="nested_dir", is_basket=False
    ):
        """Added the is_basket as a work around to test a deeply nested basket
        because the set_up_basket and upload_basket functions
        would not upload it.

        Parameters
        ----------
        tmp_basket_dir: str
            The directory path of where the nested basket will be.
        new_dir_name: str (default="nested_dir")
            The name of the new nested basket.
        is_basket: bool (default=False)
            Boolean to determine whether the new lower directory is a basket.

        Returns
        ----------
        Dictionary with the specified directory and data.
        """

        new_directory = tmp_basket_dir.mkdir(new_dir_name)
        new_directory.join("nested_file.txt").write(
            "this is a nested file to ensure the directory is created"
        )

        if is_basket:
            new_directory.join("basket_manifest.json").write(
                """{
                "uuid": "str",
                "upload_time": "1970-01-01T01:01:12+0:00",
                "parent_uuids": [],
                "basket_type": "basket type string",
                "label": "label string",
                "weave_version": "0.1.1"
            }"""
            )

        return new_directory

# Pylint doesn't like that the test fixture is being redefined here from
# test_basket, but this is the right way to do this if at some
# point in the future the two need to differentiated.
# pylint: disable=duplicate-code

# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    name="test_validate",
    params=file_systems,
    ids=file_systems_ids,
)
def fixture_test_validate(request, tmpdir):
    """Pytest fixture for testing validate."""
    file_system = request.param
    test_validate_obj = ValidateForTest(tmpdir, file_system)
    yield test_validate_obj
    test_validate_obj.cleanup_pantry()


def test_validate_pantry_does_not_exist(test_validate):
    """Give a pantry path that does not exist and check that it throws
       an error.
    """

    pantry_path = Path("THISisNOTaPROPERpantryNAMEorPATH")

    # Check that the correct error is raised
    with pytest.raises(
        ValueError,
        match=f"Invalid pantry Path. "
        f"Pantry does not exist at: {pantry_path}"
    ):
        pantry = Pantry(
            IndexPandas,
            pantry_path=test_validate.pantry_path,
            file_system=test_validate.file_system
        )
        pantry.pantry_path = pantry_path
        validate.validate_pantry(pantry)


def test_validate_no_supplement_file(test_validate):
    """Make a basket, remove the supplement file, check that it collects one
       warning.
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    basket_path = test_validate.upload_basket(
        tmp_basket_dir=tmp_basket_dir, metadata={"Test":1, "test_bool":True}
    )

    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. No Supplement file found at: "
    )
    # Check the invalid basket path is what is expected
    # (ignoring File System prefix)
    assert Path(warning_1.args[1]).match(basket_path)


def test_validate_no_metadata_file(test_validate):
    """Make a basket with no metadata, validate that it returns
       an empty list (valid).
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that no warnings are collected
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 0


def test_validate_invalid_manifest_schema(test_validate):
    """Make basket with invalid manifest schema, check that it colllects one
       warning.
    """

    # The 'uuid: 100' is supposed to be a string, not a number,
    # this is invalid against the schema.
    bad_manifest_data = """{
        "uuid": 100,
        "upload_time": "1970-01-01T01:01:12+0:00",
        "parent_uuids": [ "str1", "str2", "str3" ],
        "basket_type": "str",
        "label": "str",
        "weave_version": "0.1.1"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that three warnings are raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 3

    # Check that a specific warning is in the list.
    warn_manifest = [
        warn for warn in warning_list if
        str(warn.args[0]).startswith(
            "Invalid Basket. Manifest Schema does not match at: "
        )
    ]
    assert warn_manifest != []
    warn_manifest = warn_manifest[0]

    # Check the invalid basket path is what is expected
    # (ignoring File System prefix)
    assert Path(warn_manifest.args[1]).match(os.path.join(basket_path,
                                                   "bad_man_schema",
                                                   "basket_manifest.json"))


def test_validate_manifest_schema_missing_field(test_validate):
    """Make basket with invalid manifest schema, check that it collects one
       warning.
    """

    # The manifest is missing the uuid field
    # This is invalid against the schema.
    bad_manifest_data = """{
        "upload_time": "1970-01-01T01:01:12+0:00",
        "parent_uuids": [  ],
        "basket_type": "str",
        "label": "str",
        "weave_version": "0.1.1"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # Get paths ready to use and for removal
    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")

    # In this next section, find the supplement, which has a bad path in it,
    # pull it down, and modify it to have the proper path inside the
    # supplement's integrity data. Do this because the custom supplement
    # dictionary didn't have the correct upload path
    with test_validate.file_system.open(supplement_path, "rb",) as file:
        supplement_dict = json.load(file)

    for integrity_data in supplement_dict["integrity_data"]:
        if integrity_data["upload_path"].endswith("basket_supplement.json"):
            nested_supp_path = integrity_data["upload_path"]
        if integrity_data["upload_path"].endswith(".txt"):
            test_txt_path = integrity_data["upload_path"]

    with test_validate.file_system.open(nested_supp_path, "rb",) as supp_file:
        nested_supp_dict = json.load(supp_file)

    nested_supp_dict["integrity_data"][0]["upload_path"] = test_txt_path

    with open("basket_supplement.json", "w", encoding="utf-8") as file:
        json.dump(nested_supp_dict, file)

    test_validate.file_system.upload("basket_supplement.json",
                                     nested_supp_path)

    os.remove("basket_supplement.json")

    # Remove the default manifest and supplement
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Catch warnings and validate it throws 1 and is correct
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Manifest Schema does not match at: "
    )
    # Check the invalid basket path is what is expected
    # (ignoring File System prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_man_schema",
                                                   "basket_manifest.json"))


def test_validate_manifest_schema_additional_field(test_validate):
    """Make basket with invalid manifest schema, check that it collects one
       warning.
    """

    # The manifest has the additional "error" field
    # This is invalid against the schema.
    bad_manifest_data = """{
        "uuid": "str",
        "upload_time": "1970-01-01T01:01:12+0:00",
        "parent_uuids": [],
        "basket_type": "basket type string",
        "label": "label string",
        "weave_version": "0.1.1",

        "error": "this is an additional field"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")

    # In this next section, find the supplement, which has a bad path in it,
    # pull it down, and modify it to have the proper path inside the
    # supplement's integrity data. Do this because the custom supplement
    # dictionary didn't have the correct upload path
    with test_validate.file_system.open(supplement_path, "rb",) as file:
        supplement_dict = json.load(file)

    for integrity_data in supplement_dict["integrity_data"]:
        if integrity_data["upload_path"].endswith("basket_supplement.json"):
            nested_supp_path = integrity_data["upload_path"]
        if integrity_data["upload_path"].endswith(".txt"):
            test_txt_path = integrity_data["upload_path"]

    with test_validate.file_system.open(nested_supp_path, "rb",) as supp_file:
        nested_supp_dict = json.load(supp_file)

    nested_supp_dict["integrity_data"][0]["upload_path"] = test_txt_path

    with open("basket_supplement.json", "w", encoding="utf-8") as file:
        json.dump(nested_supp_dict, file)

    test_validate.file_system.upload("basket_supplement.json",
                                     nested_supp_path)

    os.remove("basket_supplement.json")

    # Remove the default manifest and supplement
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Manifest Schema does not match at: "
    )
    # Check the invalid basket path is what is expected
    # (ignoring File System prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_man_schema",
                                                   "basket_manifest.json"))


def test_validate_invalid_manifest_json(test_validate):
    """Make a basket with invalid manifest json, check an error is thrown.
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man",
        is_man=True,
        man_data='{"Bad":1,}',
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    with pytest.raises(
        ValueError
    ) as err:
        validate.validate_pantry(pantry)

    assert str(err.value) == ("Pantry could not be loaded into index: "
                              "Expecting property name enclosed in double "
                              "quotes: line 1 column 10 (char 9)")


def test_validate_invalid_supplement_schema(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The stub ('1231231') is supposed to be a boolean, not a number,
    # This is invalid against the schema.
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
            "access_date": "str",
            "source_path": "str",
            "byte_count": 1,
            "stub": false,
            "upload_path": "test.txt"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        is_sup=True,
        sup_data=bad_supplement_data,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected
    # (ignoring File System prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_supplement_schema_missing_field(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The supplement is missing the integrity_data field
    # This is invalid against the schema.
    bad_supplement_data = """{
        "upload_items":
        [
        { "path": "str", "stub": false}
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_supplement_schema_missing_array_field(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The supplement is missing the upload_path field inside
    # the integrity_data array, this is invalid against the schema.
    bad_supplement_data = """{
        "upload_items":
        [
        { "path": "str", "stub": false}
        ],

        "integrity_data":
        [
        {
            "file_size": 33,
            "hash": "string",
            "access_date": "string",
            "source_path": "string",
            "byte_count": 1,
            "stub": false
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_supplement_schema_missing_array_field_2(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The supplement is missing the stub field inside
    # the upload_items array, this is invalid against the schema.
    bad_supplement_data = """{
        "upload_items":
        [
        { "path": "str" }
        ],

        "integrity_data":
        [
        {
            "file_size": 33,
            "hash": "string",
            "access_date": "string",
            "source_path": "string",
            "byte_count": 1,
            "stub": false,
            "upload_path": "test.txt"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_supplement_schema_added_array_field(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The supplement has an additional field of "error" in
    # the upload_items array, this is invalid against the schema.
    bad_supplement_data = """{
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
            "access_date": "string",
            "source_path": "string",
            "byte_count": 1,
            "stub": false,
            "upload_path": "test.txt"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_supplement_schema_added_array_field_2(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The supplement has an additional field of "error" in
    # the integrity_data array, this is invalid against the schema.
    bad_supplement_data = """{
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
            "access_date": "string",
            "source_path": "string",
            "byte_count": 1,
            "stub": false,
            "upload_path": "test.txt",
            "error": "additional field"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_supplement_schema_additional_field(test_validate):
    """Make a basket with invalid supplement schema, check that it collects
       one warning.
    """

    # The supplement has an additional my_extra_field field
    # This is invalid against the schema.
    bad_supplement_data = """{
        "upload_items":
        [
        { "path": "str", "stub": false}
        ],

        "integrity_data":
        [
        {
            "file_size": 33,
            "hash": "string",
            "access_date": "string",
            "source_path": "string",
            "byte_count": 1,
            "stub": false,
            "upload_path": "test.txt"
        }
        ],

        "my_extra_field": "HAHA-ERROR"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement Schema does not match at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_schema",
                                                   "basket_supplement.json"))


def test_validate_invalid_supplement_json(test_validate):
    """Make a basket with invalid supplement json check that it collects
       one warning.
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup",
        is_man=True,
        sup_data='{"Bad": 1}}',
        is_sup=True,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Supplement could not be loaded into json at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup",
                                                   "basket_supplement.json"))


def test_validate_invalid_metadata_json(test_validate):
    """Make a basket with invalid metadata json, check that it collects
       one warning.
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_meta",
        is_man=True,
        meta_data='{"Bad": 1}}',
        is_sup=True,
        is_meta=True
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")

    # In this next section, find the supplement, which has a bad path in it,
    # pull it down, and modify it to have the proper path inside the
    # supplement's integrity data. Do this because the custom supplement
    # dictionary didn't have the correct upload path
    with test_validate.file_system.open(supplement_path, "rb",) as file:
        supplement_dict = json.load(file)

    for integrity_data in supplement_dict["integrity_data"]:
        if integrity_data["upload_path"].endswith("basket_supplement.json"):
            nested_supp_path = integrity_data["upload_path"]
        if integrity_data["upload_path"].endswith(".txt"):
            test_txt_path = integrity_data["upload_path"]

    with test_validate.file_system.open(nested_supp_path, "rb",) as supp_file:
        nested_supp_dict = json.load(supp_file)

    nested_supp_dict["integrity_data"][0]["upload_path"] = test_txt_path

    with open("basket_supplement.json", "w", encoding="utf-8") as file:
        json.dump(nested_supp_dict, file)

    test_validate.file_system.upload("basket_supplement.json",
                                     nested_supp_path)

    os.remove("basket_supplement.json")

    # Remove the default manifest and supplement
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Metadata could not be loaded into json at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "bad_meta",
                                                   "basket_metadata.json"))


def test_validate_nested_basket(test_validate):
    """Make a basket with nested basket, check that it collects one warning."""
    tmp_basket_dir = test_validate.set_up_basket(
        "my_nested_basket",
        is_man=True,
        is_sup=True,
        is_meta=True
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Manifest File found in sub directory of basket at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(basket_path)


def test_validate_deeply_nested(test_validate):
    """Create basket with basket in a deep sub-dir, check that it collects
       one warning.
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "my_basket", is_man=False, is_sup=False, is_meta=False
    )

    my_nested_dir = test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=tmp_basket_dir,
        new_dir_name="nest_level"
    )

    # Create a 10 directory deep basket
    for i in range(10):
        nested_dir_name = "nest_level_" + str(i)
        my_nested_dir = test_validate.add_lower_dir_to_temp_basket(
            tmp_basket_dir=my_nested_dir,
            new_dir_name=nested_dir_name
        )

    # Using the deep directory, upload a manifest to make it a nested basket
    my_nested_dir = test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=my_nested_dir,
        new_dir_name="deepest_basket",
        is_basket=True
    )

    test_validate.set_up_basket("my_nested_basket",
                                is_man=True,
                                is_sup=True,
                                is_meta=False)

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Manifest File found in sub directory of basket at: "
    )
    # Check the invalid basket path is what is expected (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(basket_path)


def test_validate_no_files_or_dirs(test_validate):
    """Create an empty pantry with no files, make sure it returns
       an empty list (valid).
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that no warnings are collected
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 0


def test_validate_no_baskets(test_validate):
    """Create a pantry with no baskets, but with files, test that it
       returns an empty list (valid).
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")

    # Adding this lower dir with a .txt file to have the
    # program at least search the directories.
    nested_dir_name = "nest"
    test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=tmp_basket_dir,
        new_dir_name=nested_dir_name
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that no warnings are collected
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 0


def test_validate_twenty_baskets_invalid(test_validate):
    """Create pantry with 20 baskets, and 1 nested, check that it collects
       one warning.
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    nested_basket_name = "my_nested_basket"
    nested_basket_dir = test_validate.set_up_basket(
        nested_basket_name, is_man=True, is_sup=True, is_meta=False
    )
    test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=nested_basket_dir
    )

    invalid_basket_path = test_validate.upload_basket(
        tmp_basket_dir=nested_basket_dir, uid="9999"
    )

    for i in range(20):
        uuid = "00" + str(i)
        test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is only one warning raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings so they are in proper order
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Check that the correct warning is raised
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "Invalid Basket. Manifest File found in sub directory of basket at: "
    )
    # Check the invalid basket path is what is expect (ignoring FS prefix)
    assert Path(warning_1.args[1]).match(invalid_basket_path)


def test_validate_twenty_baskets_valid(test_validate):
    """Create pantry with 20 baskets, and 0 nested, check that it
       returns an empty list (valid).
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    nested_basket_name = "my_nested_basket"
    nested_basket_dir = test_validate.set_up_basket(
        nested_basket_name, is_man=False, is_sup=False, is_meta=False
    )
    test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=nested_basket_dir
    )

    test_validate.upload_basket(
        tmp_basket_dir=nested_basket_dir, uid="9999"
    )

    for i in range(20):
        uuid = "00" + str(i)
        test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that no warnings are collected
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 0


def test_validate_call_check_level(test_validate):
    """Create a basket, call _check_level(), which is a private function, and
    check that it returns True. It returns True, because the _check_level
    function checks all files an directories of the given dir, so it just
    acts like it is at a random dir instead of the root of the pantry.
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    test_validate.upload_basket(
        tmp_basket_dir=tmp_basket_dir, metadata={"Test":1, "test_bool":True}
    )

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # pantry_name is input twice because _check_level wants the pantry name
    # and the current working directory
    # Testing a protected access class
    # pylint: disable-next=protected-access
    assert validate._check_level(
        test_validate.pantry_path,
        pantry=pantry,
    )


def test_validate_call_validate_basket(test_validate):
    """Create basket, call _validate_basket, a private function.

    Create a basket, call _validate_basket(), which is a private function.
    check that an error is thrown. it throws an error because
    _validate_basket assumes it is given a basket dir, not a pantry dir.
    so there is no manifest found inside the pantry dir.
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    test_validate.upload_basket(
        tmp_basket_dir=tmp_basket_dir, metadata={"Test":1, "test_bool":True}
    )

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    with pytest.raises(
        FileNotFoundError,
        match=f"Invalid Path. "
        f"No Basket found at: {test_validate.pantry_path}"
    ):
        # pantry_name is input twice because _check_level wants the pantry name
        # and the current working directory
        # Testing a protected access class
        # pylint: disable-next=protected-access
        validate._validate_basket(
            test_validate.pantry_path,
            pantry=pantry,
        )


def test_validate_bad_manifest_and_supplement_schema(test_validate):
    """Create a basket with invalid manifest and supplement schema,
       and check that two warnings are collected.
    """

    # The manifest is missing the uuid field
    # this is invalid against the schema.
    bad_manifest_data = """{
        "upload_time": "1970-01-01T01:01:12+0:00",
        "parent_uuids": [ "str1", "str2", "str3" ],
        "basket_type": "str",
        "label": "str",
        "weave_version": "0.1.1"
    }"""

    # The supplement has an additional my_extra_field field,
    # this is invalid against the schema.
    bad_supplement_data = """{
        "upload_items":
        [
        { "path": "str", "stub": false}
        ],

        "integrity_data":
        [
        {
            "file_size": 33,
            "hash": "string",
            "access_date": "string",
            "source_path": "string",
            "byte_count": 1,
            "stub": false,
            "upload_path": "test.txt"
        }
        ],

        "my_extra_field": "HAHA-ERROR"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_and_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        sup_data=bad_supplement_data,
        is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there are two warnings raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 2

    warn_manifest = [
        warn for warn in warning_list if
        str(warn.args[0]).startswith(
            "Invalid Basket. Manifest Schema does not match at: "
        )
    ]
    assert warn_manifest != []
    warn_manifest = warn_manifest[0]
    # Check that the warning raised is correct with the correct
    # invalid basket path (disregarding FS prefix)
    assert Path(warn_manifest.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_and_man_schema",
                                                   "basket_manifest.json"))

    warn_supplement = [
        warn for warn in warning_list if
        str(warn.args[0]).startswith(
            "Invalid Basket. Supplement Schema does not match at: "
        )
    ]
    assert warn_supplement != []
    warn_supplement = warn_supplement[0]
    # Check that the second warning raised is correct with the correct
    # invalid basket path (disregarding FS prefix)
    assert Path(warn_supplement.args[1]).match(os.path.join(basket_path,
                                                   "bad_sup_and_man_schema",
                                                   "basket_supplement.json"))


def test_validate_bad_metadata_and_supplement_schema_with_nested_basket(
                                                        test_validate):
    """Create a basket with invalid metadata and supplement schemas, along
       with an additional manifest file in a nested basket. Check that
       three warnings are collected.
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "my_basket",
        is_man=True,
        is_sup=True,
        sup_data='{"Bad": 1}}',
        is_meta=True,
        meta_data='{"Bad": 1}}'
    )

    test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=tmp_basket_dir,
        new_dir_name="nested_basket",
        is_basket=True
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there are three warnings raised
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 3

    # Sort the errors because they return differently for different fs
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    warning_1 = warning_list[1]
    warning_2 = warning_list[2]
    warning_3 = warning_list[0]

    # Check that the first warning raised is correct with the correct
    # invalid basket path (disregarding FS prefix)
    assert warning_1.args[0] == (
        "Invalid Basket. Metadata could not be loaded into json at: "
    )
    assert Path(warning_1.args[1]).match(os.path.join(basket_path,
                                                   "my_basket",
                                                   "basket_metadata.json"))

    # Check that the second warning raised is correct with the correct
    # invalid basket path (disregarding FS prefix)
    assert warning_2.args[0] == (
        "Invalid Basket. Supplement could not be loaded into json at: "
    )
    assert Path(warning_2.args[1]).match(os.path.join(basket_path,
                                                   "my_basket",
                                                   "basket_supplement.json"))

    # Check that the third warning raised is correct with the correct
    # invalid basket path (disregarding FS prefix)
    assert warning_3.args[0] == (
        "Invalid Basket. Manifest File found in sub directory of basket at: "
    )
    assert Path(warning_3.args[1]).match(
        os.path.join(basket_path, "my_basket")
    )


def test_validate_check_parent_uuids_missing_basket(test_validate):
    """Create 3 baskets, 2 with invalid parent_ids, and check that it
    raises a warning.
    This also checks that valid ones are safe because of the uuid "002" in the
    manifest_data_1's "parent_uuids".
    """

    tmp_basket_dir = test_validate.set_up_basket("with_parents_1")
    tmp_basket_dir2 = test_validate.set_up_basket("with_parents_2")

    test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir,
                                uid="001",
                                parent_ids=["002", "BAD123123"])
    test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir2,
                                uid="002",
                                parent_ids=["003", "BAD!", "BAD2", "BAD323"])

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system,
    )

    # Check that there are two warning raised.
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 2

    warning_1 = [
        warn for warn in warning_list if
        str(warn.args[0]) == (
            "The uuids: ['BAD123123'] were not found in the index, "
            "which was found inside basket: 001"
        )
    ]
    assert warning_1 != [], "Expected warning is not in warning_list."

    warning_2 = [
        warn for warn in warning_list if
        str(warn.args[0]) == (
            "The uuids: ['003', 'BAD!', 'BAD2', 'BAD323'] were not "
            "found in the index, which was found inside basket: 002"
        )
    ]
    assert warning_2 != [], "Expected warning is not in warning_list."


def test_validate_file_not_in_supplement(test_validate):
    """Add a file to the file system that is not listed in the supplement file.
    Validate that a warning is thrown.
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    temp = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    # Make a file and upload it to the file system
    upload_file_path = os.path.join(temp, "MY_UNFOUND_FILE.txt")

    with open("MY_UNFOUND_FILE.txt", "w", encoding="utf-8") as file:
        json.dump("TEST FAKE FILE", file)

    test_validate.file_system.upload("MY_UNFOUND_FILE.txt", upload_file_path)

    # Remove the local file that we created
    os.remove("MY_UNFOUND_FILE.txt")

    # Call validate_pantry, see that it returns a list of basket errors
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there is one warning raised.
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    # Sort the warnings
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    warning_msg = warning_list[0].args[0]
    warning_path = warning_list[0].args[1]

    assert warning_msg == ("File found in the file system is not listed in "
                           "the basket_supplement.json: ")
    assert Path(warning_path).match(upload_file_path)


def test_validate_file_not_in_file_system(test_validate):
    """Add a file to the supplement data and validate that a warning is thrown
    because it does not exist in the file system.
    """
    # Make a basket
    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    temp = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    supplement_to_change = os.path.join(temp, "basket_supplement.json")

    # Modify the supplement file
    with test_validate.file_system.open(supplement_to_change, "rb",) as file:
        supplement_dict = json.load(file)

    error_file_path = os.path.join(temp, "MY_FAIL_FILE.TXT")
    error_file_path_2 = os.path.join(temp, "ANOTHER_FAKE.txt")

    # New supplement data with the fake file
    supplement_dict['integrity_data'] += [
        {
            "file_size": 100,
            "hash": "fakehash",
            "access_date": "2023‐09‐05T17:22:15Z",
            "source_path": "/tmp/pytest-of-jovyan/pytest-17/MY_FAIL_FILE.TXT",
            "byte_count": 456456,
            "stub": False,
            "upload_path": error_file_path
        },
        {
            "file_size": 100,
            "hash": "fakehash",
            "access_date": "2023‐09‐05T17:22:15Z",
            "source_path": "/tmp/pytest-of-jovyan/pytest-17/ANOTHER_FAKE.TXT",
            "byte_count": 456456,
            "stub": False,
            "upload_path": error_file_path_2
        }
    ]

    # Add the new supplement data to the current file
    with open("basket_supplement.json", "w", encoding="utf-8") as file:
        json.dump(supplement_dict, file)

    # Upload the supplement file to the file system, and remove the local one
    test_validate.file_system.upload("basket_supplement.json",
                                     supplement_to_change)

    # Remove the local basket_supplement file
    os.remove("basket_supplement.json")

    # Call validate_pantry, see that it returns warnings
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system
    )

    # Check that there are two warnings raised.
    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 2

    # Sort the warnings
    warning_list = sorted(warning_list, key=lambda x: x.args[1])

    # Validate all the warnings are correct
    warning_msg_1 = warning_list[1].args[0]
    warning_path_1 = warning_list[1].args[1]

    assert warning_msg_1 == ("File listed in the basket_supplement.json does "
                             "not exist in the file system: ")
    assert Path(warning_path_1).match(error_file_path)

    warning_msg_2 = warning_list[0].args[0]
    warning_path_2 = warning_list[0].args[1]

    assert warning_msg_2 == ("File listed in the basket_supplement.json does "
                             "not exist in the file system: ")
    assert Path(warning_path_2).match(error_file_path_2)


def test_validate_check_metadata_only_basket(test_validate):
    """Upload a metadata-only basket, validate that no warnings are thrown
    """
    regular_bask_dir = test_validate.set_up_basket("regular_basket")
    metadata_bask_dir = test_validate.set_up_basket("metadata_only")

    os.remove(os.path.join(metadata_bask_dir, "test.txt"))

    test_validate.upload_basket(tmp_basket_dir=regular_bask_dir,
                                uid="regbasket")
    test_validate.upload_basket(
        tmp_basket_dir=metadata_bask_dir,
        uid="metadataonlybasket",
        parent_ids=["regbasket"],
        metadata={"bad":1},
    )

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system,
    )
    warnings = validate.validate_pantry(pantry)
    assert len(warnings) == 0


def test_validate_check_invalid_metadata_only_basket(test_validate):
    """Upload an invalid metadata-only basket (basket does not include parent-
    uuids) and validate a warning is thrown.
    """
    regular_bask_dir = test_validate.set_up_basket("regular_basket")
    metadata_bask_dir = test_validate.set_up_basket("metadata_only")

    os.remove(os.path.join(metadata_bask_dir, "test.txt"))

    test_validate.upload_basket(tmp_basket_dir=regular_bask_dir,
                                uid="regbasket")

    test_validate.upload_basket(
        tmp_basket_dir=metadata_bask_dir,
        uid="metadataonlybasket",
        metadata={"bad":1},
    )

    pantry = Pantry(
        IndexPandas,
        pantry_path=test_validate.pantry_path,
        file_system=test_validate.file_system,
    )

    warning_list = validate.validate_pantry(pantry)
    assert len(warning_list) == 1

    warning_msg = warning_list[0].args[0]
    warning_uuid = warning_list[0].args[1]

    assert warning_msg == ("Invalid Basket. No files in basket and criteria "
                           "not met for metadata-only basket. ")
    assert warning_uuid == "metadataonlybasket"
