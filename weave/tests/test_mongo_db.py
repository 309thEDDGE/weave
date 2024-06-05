"""Pytests for the metadata_db functionality."""

import os
import sys
import tempfile

import pandas as pd
import pytest

import weave
from weave import Pantry, IndexPandas
from weave.mongo_loader import MongoLoader
from weave.tests.pytest_resources import PantryForTest, get_file_systems


class MongoForTest(PantryForTest):
    """Extend the PantryForTest class to support mongodb and custom data
    loader.
    """

    def __init__(self, tmpdir, file_system):
        super().__init__(tmpdir, file_system)
        self.database_name = "test_mongo_db"
        self.metadata_collection = "test_metadata"
        self.manifest_collection = "test_manifest"
        self.supplement_collection = "test_supplement"
        self.mongodb = weave.config.get_mongo_db()[self.database_name]
        self.load_data()
        self.pantry = weave.Pantry(
            IndexPandas,
            pantry_path=self.pantry_path,
            file_system=self.file_system
        )
        self.pantry.index.generate_index()

    def load_data(self):
        """Loads data into the file_system."""
        # Create a temporary basket with a test file.
        tmp_basket_dir_name = "test_basket_tmp_dir"
        tmp_basket_dir = self.set_up_basket(tmp_basket_dir_name)

        # Upload the basket with different uuids and metadata.
        self.upload_basket(
            tmp_basket_dir, uid="1234", metadata={"key1": "value1"}
        )

        tmp_nested_dir = self.add_lower_dir_to_temp_basket(tmp_basket_dir)
        self.upload_basket(
            tmp_nested_dir, uid="4321", metadata={"key2": "value2"}
        )

        self.upload_basket(tmp_basket_dir, uid="nometadata")

    def cleanup(self):
        """Cleans up the pantry and mongodb."""
        self.cleanup_pantry()
        for collection in self.mongodb.list_collection_names():
            self.mongodb[collection].drop()


# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    params=file_systems,
    ids=file_systems_ids,
)
def set_up(request, tmpdir):
    """Sets up the fixture for testing usage."""
    file_system = request.param
    database = MongoForTest(tmpdir, file_system)
    yield database
    database.cleanup()


# Ignoring pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name

@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_metadata(set_up):
    """Test that load_mongo_metadata successfully loads valid metadata to
    the set_up.
    """
    uuids = ["1234", "4321", "nometadata"]
    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo_metadata(
        uuids=uuids,
        collection=set_up.metadata_collection,
    )

    truth_db = [
        {"uuid": "1234", "basket_type": "test_basket", "key1": "value1"},
        {"uuid": "4321", "basket_type": "test_basket", "key2": "value2"},
    ]

    db_data = list(set_up.mongodb[set_up.metadata_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop("_id")
        compared_data.append(item)
    assert truth_db == compared_data


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_manifest(set_up):
    """Test that load_mongo_manifest successfully loads valid manifest to
    the set_up.
    """
    uuids = ["1234", "4321", "nometadata"]
    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo_manifest(
        uuids=uuids,
        collection=set_up.manifest_collection,
    )

    truth_db = [
        {"uuid": "1234", "parent_uuids": [], "basket_type": "test_basket",
         "label": ""},
        {"uuid": "4321", "parent_uuids": [], "basket_type": "test_basket",
         "label": ""},
        {"uuid": "nometadata", "parent_uuids": [],
         "basket_type": "test_basket", "label": ""}
    ]
    db_data = list(set_up.mongodb[set_up.manifest_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop("_id")
        item.pop("upload_time")
        item.pop("weave_version")
        compared_data.append(item)
    assert truth_db == compared_data


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_supplement(set_up):
    """Test that load_mongo_supplement successfully loads valid supplement to
    the set_up.
    """
    uuids = ["1234", "4321", "nometadata"]
    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo_supplement(
        uuids=uuids,
        collection=set_up.supplement_collection,
    )

    truth_db = [
        {"uuid": "1234", "basket_type": "test_basket"},
        {"uuid": "4321", "basket_type": "test_basket"},
        {"uuid": "nometadata", "basket_type": "test_basket"}
    ]
    db_data = list(set_up.mongodb[set_up.supplement_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop("_id")
        item.pop("integrity_data")
        item.pop("upload_items")
        compared_data.append(item)
    assert truth_db == compared_data


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo(set_up):
    """Test that load_mongo successfully loads valid metadata, manifest, and
    supplement to the set_up.
    """
    uuids = ["1234", "4321", "nometadata"]
    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo(
        uuids=uuids,
        metadata_collection=set_up.metadata_collection,
        manifest_collection=set_up.manifest_collection,
        supplement_collection=set_up.supplement_collection,
    )

    metadata_truth_db = [
        {"uuid": "1234", "basket_type": "test_basket", "key1": "value1"},
        {"uuid": "4321", "basket_type": "test_basket", "key2": "value2"},
    ]
    metadata = list(set_up.mongodb[set_up.metadata_collection].find({}))
    compared_metadata = []
    for item in metadata:
        item.pop("_id")
        compared_metadata.append(item)

    manifest_truth_db = [
        {"uuid": "1234", "parent_uuids": [], "basket_type": "test_basket",
         "label": ""},
        {"uuid": "4321", "parent_uuids": [], "basket_type": "test_basket",
         "label": ""},
        {"uuid": "nometadata", "parent_uuids": [],
         "basket_type": "test_basket", "label": ""}
    ]
    manifest = list(set_up.mongodb[set_up.manifest_collection].find({}))
    compared_manifest = []
    for item in manifest:
        item.pop("_id")
        item.pop("upload_time")
        item.pop("weave_version")
        compared_manifest.append(item)

    supplement_truth_db = [
        {"uuid": "1234", "basket_type": "test_basket"},
        {"uuid": "4321", "basket_type": "test_basket"},
        {"uuid": "nometadata", "basket_type": "test_basket"}
    ]
    supplement = list(set_up.mongodb[set_up.supplement_collection].find({}))
    compared_supplement = []
    for item in supplement:
        item.pop("_id")
        item.pop("integrity_data")
        item.pop("upload_items")
        compared_supplement.append(item)

    assert metadata_truth_db == compared_metadata
    assert manifest_truth_db == compared_manifest
    assert supplement_truth_db == compared_supplement


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_metadata_check_collection_for_string(set_up):
    """Test that load_mongo_metadata prevents loading data with an invalid
    set_up collection.
    """
    with pytest.raises(
        TypeError, match="Invalid datatype for metadata collection: "
                         "must be a string"
    ):
        mongo_loader = MongoLoader(pantry=set_up.pantry)
        mongo_loader.load_mongo_metadata(uuids=["1234"], collection=1)


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_mongodb_check_dataframe_for_uuid(set_up):
    """Test that MongoDB prevents loading data with missing uuid.
    """
    with pytest.raises(
        TypeError, match="Invalid datatype for uuids: "
                         "must be a list of strings [str]"
    ):
        mongo_loader = MongoLoader(pantry=set_up.pantry)
        mongo_loader.load_mongo_metadata(
            uuids=pd.DataFrame(
                {"uuid": ["1234"],
                 "basket_type": ["type"],
                 "address": ["path"]}
            )
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_metadata_check_for_duplicate_uuid(set_up):
    """Test duplicate metadata won't be uploaded to mongoDB, based on the UUID.
    """
    test_uuid = "1234"

    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo_metadata([test_uuid], set_up.metadata_collection)
    mongo_loader.load_mongo_metadata([test_uuid], set_up.metadata_collection)

    count = set_up.mongodb[set_up.metadata_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_manifest_check_for_duplicate_uuid(set_up):
    """Test duplicate manifest won't be uploaded to mongoDB, based on the UUID.
    """
    test_uuid = "1234"

    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo_manifest([test_uuid], set_up.manifest_collection)
    mongo_loader.load_mongo_manifest([test_uuid], set_up.manifest_collection)

    count = set_up.mongodb[set_up.manifest_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_load_mongo_supplement_check_for_duplicate_uuid(set_up):
    """Test duplicate supplement won't be uploaded to mongoDB, based on the
    UUID.
    """
    test_uuid = "1234"

    mongo_loader = MongoLoader(pantry=set_up.pantry)
    mongo_loader.load_mongo_supplement([test_uuid], set_up.supplement_collection)
    mongo_loader.load_mongo_supplement([test_uuid], set_up.supplement_collection)

    count = set_up.mongodb[set_up.supplement_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"


@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_check_file_already_exists(set_up):
    """Make a file, upload it to the pantry, check if that file already exists.
    """
    pantry = Pantry(
        IndexPandas,
        pantry_path=set_up.pantry_path,
        file_system=set_up.file_system
    )

    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file.write(b'This is my temporary file that we will hash')
        tmp_file.flush()
        pantry.upload_basket(
            upload_items=[{'path':tmp_file.name, 'stub':False}],
            basket_type='filealreadyexists',
            unique_id='file_already_exists_uuid',
        )

    mongo_loader = MongoLoader(pantry=pantry)
    mongo_loader.load_mongo_supplement(uuids=["file_already_exists_uuid"])
    uuids = pantry.does_file_exist(tmp_file.name)

    assert len(uuids) == 1
    assert uuids[0] == 'file_already_exists_uuid'

@pytest.mark.skipif(
    "pymongo" not in sys.modules, reason="Pymongo required for this test"
)
def test_check_pantries_have_discrete_mongodbs(set_up):
    pantry_basename = (
        "pytest-temp-pantry"
        f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )
    pantry1 = Pantry(
        IndexPandas,
        pantry_path=pantry_basename+"pantry1",
        file_system=set_up.file_system,
    )
    pantry2 = Pantry(
        IndexPandas,
        pantry_path=pantry_basename+"pantry2",
        file_system=set_up.file_system,
    )

    p1_mongo_loader = MongoLoader(pantry=pantry1)
    p2_mongo_loader = MongoLoader(pantry=pantry2)

    with tempfile.NamedTemporaryFile() as tmp_file1:
        tmp_file1.write(b'This is temporary file that we will hash for p1')
        tmp_file1.flush()
        with tempfile.NamedTemporaryFile() as tmp_file2:
            tmp_file2.write(b'This is temporary file that we will hash for p2')
            tmp_file2.flush()

            pantry1.upload_basket(
                upload_items=[{'path':tmp_file1.name, 'stub':False}],
                basket_type='test_basket',
                unique_id='pantry1fileuuid',
            )
            p1_mongo_loader.load_mongo_supplement(uuids=["pantry1fileuuid"])

            pantry2.upload_basket(
                upload_items=[{'path':tmp_file2.name, 'stub':False}],
                basket_type='test_basket',
                unique_id='pantry2fileuuid',
            )
            p2_mongo_loader.load_mongo_supplement(uuids=["pantry2fileuuid"])

            # Check the files only exist and are tracked according to their
            # respective pantries.
            assert (
                pantry1.does_file_exist(tmp_file1.name) == ["pantry1fileuuid"]
            )
            assert pantry1.does_file_exist(tmp_file2.name) == []
            assert pantry2.does_file_exist(tmp_file1.name) == []
            assert (
                pantry2.does_file_exist(tmp_file2.name) == ["pantry2fileuuid"]
            )

    for collection in p1_mongo_loader.database.list_collection_names():
            p1_mongo_loader.database[collection].drop()
    for collection in p2_mongo_loader.database.list_collection_names():
            p2_mongo_loader.database[collection].drop()
