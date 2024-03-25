"""Pytests for the metadata_db functionality."""

import os
import sys
import tempfile

import pandas as pd
import pytest

import weave
from weave import Pantry, IndexPandas
from weave.metadata_db import load_mongo
from weave.mongo_db import MongoDB
from weave.tests.pytest_resources import PantryForTest, get_file_systems


class MongoForTest(PantryForTest):
    """Extend the PantryForTest class to support mongodb and custom data
    loader.
    """

    def __init__(self, tmpdir, file_system):
        super().__init__(tmpdir, file_system)
        self.database = "test_mongo_db"
        self.metadata_collection = "test_metadata"
        self.manifest_collection = "test_manifest"
        self.supplement_collection = "test_supplement"
        self.mongodb = weave.config.get_mongo_db()[self.database]
        self.load_data()

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
        # self.mongodb[self.col].drop()


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

# Skip tests if pymongo is not installed.
@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_from_metadata_db(set_up):
    """Test that load_mongo successfully loads valid metadata to
    the set_up.
    """
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    load_mongo(
        index_table,
        set_up.metadata_collection,
        database=set_up.database,
        file_system=set_up.file_system
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
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_metadata(set_up):
    """Test that load_mongo_metadata successfully loads valid metadata to
    the set_up.
    """
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo_metadata(set_up.metadata_collection)

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
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_manifest(set_up):
    """Test that load_mongo_manifest successfully loads valid manifest to
    the set_up.
    """
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo_manifest(set_up.manifest_collection)

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
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_supplement(set_up):
    """Test that load_mongo_supplement successfully loads valid supplement to
    the set_up.
    """
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo_supplement(set_up.supplement_collection)

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
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo(set_up):
    """Test that load_mongo successfully loads valid metadata, manifest, and
    supplement to the set_up.
    """
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo(
        metadata_collection=set_up.metadata_collection,
        manifest_collection=set_up.manifest_collection,
        supplement_collection=set_up.supplement_collection
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
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_mongodb_check_for_dataframe(set_up):
    """Test that MongoDB prevents loading data with an invalid index_table.
    """
    with pytest.raises(
        TypeError,
        match="Invalid datatype for index_table: " "must be Pandas DataFrame",
    ):
        MongoDB(
            index_table="",
            database=set_up.database,
            file_system=set_up.file_system
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_metadata_check_collection_for_string(set_up):
    """Test that load_mongo_metadata prevents loading data with an invalid
    set_up collection.
    """
    with pytest.raises(
        TypeError, match="Invalid datatype for metadata collection: "
                         "must be a string"
    ):
        mongo_instance = MongoDB(
            index_table=pd.DataFrame(
                {"uuid": ["1234"], "basket_type": ["type"],
                 "address": ["path"]}
            ),
            database=set_up.database,
            file_system=set_up.file_system
        )
        mongo_instance.load_mongo_metadata(collection=1)


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_mongodb_check_dataframe_for_uuid(set_up):
    """Test that MongoDB prevents loading data with missing uuid.
    """
    with pytest.raises(
        ValueError, match="Invalid index_table: " "missing uuid column"
    ):
        MongoDB(
            index_table=pd.DataFrame(
                {"basket_type": ["type"], "address": ["path"]}
            ),
            database=set_up.database,
            file_system=set_up.file_system
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_mongodb_check_dataframe_for_address(set_up):
    """Test that MongoDB prevents loading data with missing address."""
    with pytest.raises(
        ValueError, match="Invalid index_table: " "missing address column"
    ):
        MongoDB(
            index_table=pd.DataFrame(
                {"uuid": ["1234"], "basket_type": ["type"]}
            ),
            database=set_up.database,
            file_system=set_up.file_system
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_mongodb_check_dataframe_for_basket_type(set_up):
    """Test that MongoDB prevents loading data with missing basket type."""
    with pytest.raises(
        ValueError, match="Invalid index_table: " "missing basket_type column"
    ):
        MongoDB(
            index_table=pd.DataFrame({"uuid": ["1234"], "address": ["path"]}),
            database=set_up.database,
            file_system=set_up.file_system,
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_metadata_check_for_duplicate_uuid(set_up):
    """Test duplicate metadata won't be uploaded to mongoDB, based on the UUID.
    """
    test_uuid = "1234"

    # Load metadata twice and ensure there's only one instance
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo_metadata(set_up.metadata_collection)
    mongo_instance.load_mongo_metadata(set_up.metadata_collection)

    count = set_up.mongodb[set_up.metadata_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_manifest_check_for_duplicate_uuid(set_up):
    """Test duplicate manifest won't be uploaded to mongoDB, based on the UUID.
    """
    test_uuid = "1234"

    # Load manifest twice and ensure there's only one instance
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo_manifest(set_up.manifest_collection)
    mongo_instance.load_mongo_manifest(set_up.manifest_collection)

    count = set_up.mongodb[set_up.manifest_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_supplement_check_for_duplicate_uuid(set_up):
    """Test duplicate supplement won't be uploaded to mongoDB, based on the
    UUID.
    """
    test_uuid = "1234"

    # Load supplement twice and ensure there's only one instance
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    mongo_instance = MongoDB(
        index_table=index_table,
        database=set_up.database,
        file_system=set_up.file_system
    )
    mongo_instance.load_mongo_supplement(set_up.supplement_collection)
    mongo_instance.load_mongo_supplement(set_up.supplement_collection)

    count = set_up.mongodb[set_up.supplement_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
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

        index_table = weave.index.create_index.create_index_from_fs(
            set_up.pantry_path, set_up.file_system
        )
        mongo_instance = MongoDB(
            index_table=index_table,
            database=set_up.database,
            file_system=set_up.file_system
        )
        mongo_instance.load_mongo_supplement(set_up.supplement_collection)
        uuids = pantry.does_file_exist(tmp_file.name)

    assert len(uuids) == 1
    assert uuids[0] == 'file_already_exists_uuid'
