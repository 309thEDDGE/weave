"""Pytests for the metadata_db functionality."""

import os
import sys

import pandas as pd
import pytest

import weave
from weave.tests.pytest_resources import PantryForTest, get_file_systems


class MongoForTest(PantryForTest):
    """Extend the PantryForTest class to support mongodb and custom data
    loader.
    """

    def __init__(self, tmpdir, file_system):
        super().__init__(tmpdir, file_system)
        self.test_collection = "test_collection"
        self.mongodb = weave.config.get_mongo_db().mongo_metadata
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
        self.mongodb[self.test_collection].drop()


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
def test_load_mongo(set_up):
    """Test that load_mongo successfully loads valid metadata to the set_up.
    """

    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    weave.load_mongo(
        index_table,
        file_system=set_up.file_system,
        collection=set_up.test_collection,
    )

    truth_db = [
        {"uuid": "1234", "basket_type": "test_basket", "key1": "value1"},
        {"uuid": "4321", "basket_type": "test_basket", "key2": "value2"},
    ]

    db_data = list(set_up.mongodb[set_up.test_collection].find({}))
    compared_data = []
    for item in db_data:
        item.pop("_id")
        compared_data.append(item)
    assert truth_db == compared_data


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_check_for_dataframe(set_up):
    """Test that load_mongo prevents loading data with an invalid index_table.
    """

    with pytest.raises(
        TypeError,
        match="Invalid datatype for index_table: " "must be Pandas DataFrame",
    ):
        weave.load_mongo(
            "",
            file_system=set_up.file_system,
            collection=set_up.test_collection,
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_check_collection_for_string(set_up):
    """Test that load_mongo prevents loading data with an invalid set_up
    collection.
    """

    with pytest.raises(
        TypeError, match="Invalid datatype for collection: " "must be a string"
    ):
        weave.load_mongo(
            pd.DataFrame(), file_system=set_up.file_system, collection=1
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_check_dataframe_for_uuid(set_up):
    """Test that load_mongo prevents loading data with missing uuid.
    """

    with pytest.raises(
        ValueError, match="Invalid index_table: " "missing uuid column"
    ):
        weave.load_mongo(
            pd.DataFrame({"basket_type": ["type"], "address": ["path"]}),
            file_system=set_up.file_system,
            collection=set_up.test_collection,
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_check_dataframe_for_address(set_up):
    """Test that load_mongo prevents loading data with missing address."""
    with pytest.raises(
        ValueError, match="Invalid index_table: " "missing address column"
    ):
        weave.load_mongo(
            pd.DataFrame({"uuid": ["1234"], "basket_type": ["type"]}),
            file_system=set_up.file_system,
            collection=set_up.test_collection,
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_check_dataframe_for_basket_type(set_up):
    """Test that load_mongo prevents loading data with missing basket type."""
    with pytest.raises(
        ValueError, match="Invalid index_table: " "missing basket_type column"
    ):
        weave.load_mongo(
            pd.DataFrame({"uuid": ["1234"], "address": ["path"]}),
            file_system=set_up.file_system,
            collection=set_up.test_collection,
        )


@pytest.mark.skipif(
    "pymongo" not in sys.modules or not os.environ.get("MONGODB_HOST", False),
    reason="Pymongo required for this test",
)
def test_load_mongo_check_for_duplicate_uuid(set_up):
    """Test duplicate metadata won't be uploaded to mongoDB, based on the UUID.
    """

    test_uuid = "1234"

    # Load metadata twice and ensure there's only one instance
    index_table = weave.index.create_index.create_index_from_fs(
        set_up.pantry_path, set_up.file_system
    )
    weave.load_mongo(
        index_table,
        file_system=set_up.file_system,
        collection=set_up.test_collection,
    )
    weave.load_mongo(
        index_table,
        file_system=set_up.file_system,
        collection=set_up.test_collection,
    )
    count = set_up.mongodb[set_up.test_collection].count_documents(
        {"uuid": test_uuid}
    )
    assert count == 1, "duplicate uuid inserted"
