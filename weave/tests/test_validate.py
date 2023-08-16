"""Pytests for the validate functionality."""
import os
from pathlib import Path

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave import validate
from weave.tests.pytest_resources import BucketForTest

# This module is long and has many tests. Pylint is complaining that it is too
# long. I don't necessarily think that is bad in this case, as the alternative
# would be to write the tests continuuing in a different script, which I think
# is unnecesarily complex. Therefor, I am disabling this warning for this
# script.
# pylint: disable=too-many-lines


class ValidateForTest(BucketForTest):
    """A class to test functions in validate.py"""

    # Well I think we probably need different args for the below function,
    # which is over-riding BucketForTest.set_up_basket. Pylint hates it, but
    # here we are:
    # pylint: disable-next=arguments-differ
    def set_up_basket(
        self,
        tmp_dir_name,
        **kwargs
    ):
        """Overrides BucketForTest's set_up_basket to better test validate.py

        Sets up the basket with a nested basket depending on the values of
        the boolean params when this is called. if the is_man is true, a
        nested manifest file will be put in the basket, same with is_sup for
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
        
        Key-word Arguments:
        -------------------
        is_man: boolean
            a bool that signals if ther should be a manifest file
            defaults to no manifest
        is_sup: boolean
            a bool that signals if ther should be a supplement file
            defaults to no supplement
        is_meta: boolean
            a bool that signals if ther should be a metadata file
            defaults to no metadata
        man_data: string
            the json data we want to be put into the manifest file
            defaults to a valid manifest schema
        sup_data: string
            the json data we want to be put into the supplement file
            defaults to a valid supplement schema
        meta_data: string
            the json data we want to be put into the metadata file
            defaults to a valid json object

        Returns
        ----------
        A string of the directory where the basket was uploaded
        """
        is_man=kwargs.get("is_man", False)
        is_sup=kwargs.get("is_sup", False)
        is_meta=kwargs.get("is_meta", False)
        man_data=kwargs.get("man_data", "")
        sup_data=kwargs.get("sup_data", "")
        meta_data=kwargs.get("meta_data", "")
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)

        if is_man:
            tmp_manifest = tmp_basket_dir.join("basket_manifest.json")

            # This gives a default valid manifest json schema
            if man_data == "":
                man_data = """{
                    "uuid": "str",
                    "upload_time": "uploadtime string",
                    "parent_uuids": [ "string1", "string2", "string3" ],
                    "basket_type": "basket type string",
                    "label": "label string"
                }"""

            tmp_manifest.write(man_data)

        if is_sup:
            tmp_supplement = tmp_basket_dir.join("basket_supplement.json")

            # This gives a default valid supplement json schema
            if sup_data == "":
                sup_data = """{
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
        because I couldn't get it to upload one using the set_up_basket
        or upload_basket function
        """
        new_directory = tmp_basket_dir.mkdir(new_dir_name)
        new_directory.join("nested_file.txt").write(
            "this is a nested file to ensure the directory is created"
        )

        if is_basket:
            new_directory.join("basket_manifest.json").write(
                """{
                "uuid": "str",
                "upload_time": "uploadtime string",
                "parent_uuids": [ "string1", "string2", "string3" ],
                "basket_type": "basket type string",
                "label": "label string"
            }"""
            )

        return new_directory

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
def test_validate(request, tmpdir):
    """Pytest fixture for testing validate"""
    file_system = request.param
    test_validate_obj = ValidateForTest(tmpdir, file_system)
    yield test_validate_obj
    test_validate_obj.cleanup_bucket()


# We need to ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name


def test_validate_bucket_does_not_exist(test_validate):
    """Give a bucket path that does not exist and check that it throws an
    error
    """

    bucket_path = Path("THISisNOTaPROPERbucketNAMEorPATH")

    with pytest.raises(
        ValueError,
        match=f"Invalid Bucket Path. "
        f"Bucket does not exist at: {bucket_path}",
    ):
        validate.validate_bucket(bucket_path, test_validate.file_system)


def test_validate_no_supplement_file(test_validate):
    """Make a basket, remove the supplement file, check that it throws an
    error
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    basket_path = test_validate.upload_basket(
        tmp_basket_dir=tmp_basket_dir, metadata={"Test": 1, "test_bool": True}
    )

    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        FileNotFoundError,
        match="Invalid Basket. No Supplement file found at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid basket path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(basket_path)


def test_validate_no_metadata_file(test_validate):
    """Make a basket with no metadata, validate that it returns true (valid)"""

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)
    test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    assert validate.validate_bucket(
        test_validate.bucket_name, test_validate.file_system
    )


def test_validate_invalid_manifest_schema(test_validate):
    """Make basket with invalid manifest schema, check that it throws an
    error
    """

    # The 'uuid: 100' is supposed to be a string, not a number,
    # this is invalid against the schema.
    bad_manifest_data = """{
        "uuid": 100,
        "upload_time": "str",
        "parent_uuids": [ "str1", "str2", "str3" ],
        "basket_type": "str",
        "label": "str"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError, match="Invalid Basket. Manifest Schema does not match at: "
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_man_schema", "basket_manifest.json")
    )


def test_validate_manifest_schema_missing_field(test_validate):
    """Make basket with invalid manifest schema, check that it throws an
    error
    """

    # The manifest is missing the uuid field
    # this is invalid against the schema.
    bad_manifest_data = """{
        "upload_time": "str",
        "parent_uuids": [ "str1", "str2", "str3" ],
        "basket_type": "str",
        "label": "str"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError, match="Invalid Basket. Manifest Schema does not match at: "
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_man_schema", "basket_manifest.json")
    )


def test_validate_manifest_schema_additional_field(test_validate):
    """Make basket with invalid manifest schema, check that it throws an
    error
    """

    # The manifest has the additional "error" field
    # this is invalid against the schema.
    bad_manifest_data = """{
        "uuid": "str",
        "upload_time": "uploadtime string",
        "parent_uuids": [ "string1", "string2", "string3" ],
        "basket_type": "basket type string",
        "label": "label string",

        "error": "this is an additional field"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man_schema",
        is_man=True,
        man_data=bad_manifest_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError, match="Invalid Basket. Manifest Schema does not match at: "
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_man_schema", "basket_manifest.json")
    )


def test_validate_invalid_manifest_json(test_validate):
    """Make a basket with invalid manifest json, check that it throws an
    error
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_man",
        is_man=True,
        man_data='{"Bad":1}}',
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Manifest could not be loaded into json at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_man", "basket_manifest.json")
    )


def test_validate_invalid_supplement_schema(test_validate):
    """Make a basket with invalid supplement schema, check that it throws
    error
    """

    # The stub ('1231231') is supposed to be a boolean, not a number,
    # this is invalid against the schema.
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

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        is_sup=True,
        sup_data=bad_supplement_data,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_missing_field(test_validate):
    """Make a basket with invalid supplement schema, check that it throws
    error
    """

    # The supplement is missing the integrity_data field
    # this is invalid against the schema.
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
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_missing_array_field(test_validate):
    """Make a basket with invalid supplement schema, check that it throws
    error
    """

    # The supplement is missing the upload_path field inside
    # the integrity_data array this is invalid against the schema.
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
            "access_date":"string",
            "source_path": "string",
            "byte_count": 1,
            "stub":false
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_missing_array_field_2(test_validate):
    """Make a basket with invalid supplement schema, check that it throws
    error
    """

    # The supplement is missing the stub field inside
    # the upload_items array this is invalid against the schema.
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
            "access_date":"string",
            "source_path": "string",
            "byte_count": 1,
            "stub":false,
            "upload_path": "string"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_added_array_field(test_validate):
    """Make a basket with invalid supplement schema, check that it throws
    error
    """

    # The supplement has an additional field of "error" in
    # the upload_items array this is invalid against the schema.
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
            "access_date":"string",
            "source_path": "string",
            "byte_count": 1,
            "stub":false,
            "upload_path":"string"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_added_array_field_2(test_validate):
    """Make a basket with invalid supplement schema, check that it throws error
    """

    # The supplement has an additional field of "error" in
    # the integrity_data array this is invalid against the schema.
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
            "access_date":"string",
            "source_path": "string",
            "byte_count": 1,
            "stub":false,
            "upload_path":"string",
            "error": "additional field"
        }
        ]
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_additional_field(test_validate):
    """Make a basket with invalid supplement schema, check that it throws error
    """

    # The supplement has an additional my_extra_field field
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
            "access_date":"string",
            "source_path": "string",
            "byte_count": 1,
            "stub":false,
            "upload_path":"string"
        }
        ],

        "my_extra_field":"HAHA-ERROR"
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_empty_upload_items(test_validate):
    """Make a basket with invalid supplement schema, check that it throws error
    """

    # The supplement has an empty array of "upload_items"
    # this is invalid against the schema.
    bad_supplement_data = """{
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
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_supplement_schema_empty_integrity_data(test_validate):
    """Make a basket with invalid supplement schema, check that it throws error
    """

    # The supplement an empty array of "integrity_data"
    # this is invalid against the schema.
    bad_supplement_data = """{
        "upload_items":
        [
        { "path": "str", "stub": false}
        ],

        "integrity_data": []
    }"""

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup_schema",
        is_man=True,
        sup_data=bad_supplement_data,
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement Schema does not match at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup_schema", "basket_supplement.json")
    )


def test_validate_invalid_supplement_json(test_validate):
    """Make a basket with invalid supplement json check that it throws an error
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_sup",
        is_man=True,
        sup_data='{"Bad":1}}',
        is_sup=True,
        is_meta=False,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Supplement could not be loaded into json at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_sup", "basket_supplement.json")
    )


def test_validate_invalid_metadata_json(test_validate):
    """Make a basket with invalid metadata json, check that it throws an error
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "bad_meta",
        is_man=True,
        meta_data='{"Bad":1}}',
        is_sup=True,
        is_meta=True,
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. Metadata could not be loaded into json at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(
        os.path.join(basket_path, "bad_meta", "basket_metadata.json")
    )


def test_validate_nested_basket(test_validate):
    """Make a basket with nested basket, check that it throws an error"""

    tmp_basket_dir = test_validate.set_up_basket(
        "my_nested_basket", is_man=True, is_sup=True, is_meta=True
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. "
        "Manifest File found in sub directory of basket at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid file path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(os.path.join(basket_path))


def test_validate_deeply_nested(test_validate):
    """Create basket with basket in a deep sub-dir, check that error is thrown
    """

    tmp_basket_dir = test_validate.set_up_basket(
        "my_basket", is_man=False, is_sup=False, is_meta=False
    )

    my_nested_dir = test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=tmp_basket_dir, new_dir_name="nest_level"
    )

    # Create a deep directory 10 deep that we can use
    for i in range(10):
        nested_dir_name = "nest_level_" + str(i)
        my_nested_dir = test_validate.add_lower_dir_to_temp_basket(
            tmp_basket_dir=my_nested_dir, new_dir_name=nested_dir_name
        )

    # Using the deep directory, upload a manifest to make it a nested basket
    my_nested_dir = test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=my_nested_dir,
        new_dir_name="deepest_basket",
        is_basket=True,
    )

    test_validate.set_up_basket(
        "my_nested_basket", is_man=True, is_sup=True, is_meta=False
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. "
        "Manifest File found in sub directory of basket at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    assert e_info.value.args[1].endswith(os.path.join(basket_path))


def test_validate_no_files_or_dirs(test_validate):
    """Create an empty bucket with no files, make sure it returns true (valid).
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    assert validate.validate_bucket(
        test_validate.bucket_name, test_validate.file_system
    )


def test_validate_no_baskets(test_validate):
    """Create a bucket with no baskets, but with files, test that it's valid.
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")

    # Adding this lower dir with a .txt file to have the
    # program at least search the directories.
    nested_dir_name = "nest"
    test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=tmp_basket_dir, new_dir_name=nested_dir_name
    )

    basket_path = test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir)

    manifest_path = os.path.join(basket_path, "basket_manifest.json")
    supplement_path = os.path.join(basket_path, "basket_supplement.json")
    test_validate.file_system.rm(manifest_path)
    test_validate.file_system.rm(supplement_path)

    assert validate.validate_bucket(
        test_validate.bucket_name, test_validate.file_system
    )


def test_validate_fifty_baskets_invalid(test_validate):
    """Create bucket with 50 baskets, and 1 nested, check that it throws error.
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

    for i in range(50):
        uuid = "00" + str(i)
        test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    with pytest.raises(
        ValueError,
        match="Invalid Basket. "
        "Manifest File found in sub directory of basket at: ",
    ) as e_info:
        validate.validate_bucket(
            test_validate.bucket_name, test_validate.file_system
        )

    # Check the invalid basket path is what we expect (disregarding FS prefix)
    assert e_info.value.args[1].endswith(invalid_basket_path)


def test_validate_fifty_baskets_valid(test_validate):
    """Create bucket with 50 baskets, and 0 nested, check that its valid."""

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    nested_basket_name = "my_nested_basket"
    nested_basket_dir = test_validate.set_up_basket(
        nested_basket_name, is_man=False, is_sup=False, is_meta=False
    )
    test_validate.add_lower_dir_to_temp_basket(
        tmp_basket_dir=nested_basket_dir
    )

    test_validate.upload_basket(tmp_basket_dir=nested_basket_dir, uid="9999")

    for i in range(50):
        uuid = "00" + str(i)
        test_validate.upload_basket(tmp_basket_dir=tmp_basket_dir, uid=uuid)

    assert validate.validate_bucket(
        test_validate.bucket_name, test_validate.file_system
    )


def test_validate_call_check_level(test_validate):
    """Create basket, call _check_level()

    Create a basket, call _check_level() which is a private function,
    check that it returns true. it returns true, because the _check_level
    function checks all files an directories of the given dir, so it just
    acts like we are at a random dir instead of the root of the bucket
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    test_validate.upload_basket(
        tmp_basket_dir=tmp_basket_dir, metadata={"Test": 1, "test_bool": True}
    )
    # Obviously we need to test a protected access var here.
    # pylint: disable-next=protected-access
    assert validate._check_level(
        test_validate.bucket_name, test_validate.file_system
    )


def test_validate_call_validate_basket(test_validate):
    """Create basket, call _validate_basket, a private function

    Create a basket, call _validate_basket(), which is a private function.
    check that it throws an error. it throws an error because _validate_basket
    assumes it is given a basket dir, not a bucket dir. so there is no
    manifest found inside the bucket dir
    """

    tmp_basket_dir = test_validate.set_up_basket("my_basket")
    test_validate.add_lower_dir_to_temp_basket(tmp_basket_dir=tmp_basket_dir)

    test_validate.upload_basket(
        tmp_basket_dir=tmp_basket_dir, metadata={"Test": 1, "test_bool": True}
    )

    with pytest.raises(
        FileNotFoundError,
        match=f"Invalid Path. "
        f"No Basket found at: {test_validate.bucket_name}",
    ):
        # Obviously we need to test a protected access var here.
        # pylint: disable-next=protected-access
        validate._validate_basket(
            test_validate.bucket_name, test_validate.file_system
        )
