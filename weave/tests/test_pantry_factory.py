"""Pytest tests for the pantry factory."""
import json
import os
import sys
import tempfile

import pytest

import weave
from weave import IndexSQLite
from weave import IndexSQL
from weave.pantry_factory import create_pantry
from weave.index.index_pandas import IndexPandas
from weave.tests.pytest_resources import PantryForTest, IndexForTest
from weave.tests.pytest_resources import get_file_systems

###############################################################################
#                      Pytest Fixtures Documentation:                         #
#            https://docs.pytest.org/en/7.3.x/how-to/fixtures.html            #
#                                                                             #
#                  https://docs.pytest.org/en/7.3.x/how-to/                   #
#          fixtures.html#teardown-cleanup-aka-fixture-finalization            #
#                                                                             #
#  https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize  #
###############################################################################


# Create fsspec objects to be tested, and add to file_systems list.
file_systems, _ = get_file_systems()

# Create Index CONSTRUCTORS of Indexes to be tested, and add to indexes list.
indexes = [IndexPandas, IndexSQLite]
indexes_ids = ["Pandas", "SQLite"]

# Only add IndexSQL if the env variables are set and dependencies are present.
if "WEAVE_SQL_HOST" in os.environ and "sqlalchemy" in sys.modules:
    indexes.append(IndexSQL)
    indexes_ids.append("SQL")

# Create combinations of the above parameters to pass into the fixture.
params = []
params_ids = []
for iter_file_system in file_systems:
    for iter_index, iter_index_id in zip(indexes, indexes_ids):
        params.append((iter_file_system, iter_index))
        params_ids.append(f"{iter_file_system.__class__.__name__}-"
                          f"-{iter_index_id}")

@pytest.fixture(name="test_pantry", params=params, ids=params_ids)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests."""
    file_system = request.param[0]
    pantry_path = (
        "pytest-temp-pantry" f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    test_pantry = PantryForTest(tmpdir, file_system, pantry_path=pantry_path)

    index_constructor = request.param[1]
    test_index = IndexForTest(
        index_constructor=index_constructor,
        file_system=file_system,
        pantry_path=pantry_path,
    )
    index_name = test_index.index.__class__.__name__

    yield test_pantry, index_name, index_constructor
    test_pantry.cleanup_pantry()
    test_index.cleanup_index()


def test_pantry_factory_default_args(test_pantry):
    """Test the pantry factory with the default create pantry args."""
    test_pantry, _, index_constructor = test_pantry

    pantry = create_pantry(index=index_constructor,
                           pantry_path=test_pantry.pantry_path,
                           file_system=test_pantry.file_system)

    assert isinstance(pantry, weave.Pantry)


def test_pantry_factory_local_config(test_pantry):
    """Create a pantry using the pantry factory with a locally saved config."""
    test_pantry, index_name, _ = test_pantry
    file_system_type = test_pantry.file_system.__class__.__name__

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "config.json")

        with open(config_path, "w", encoding="utf-8") as config_file:
            s3_endpoint = os.environ.get("S3_ENDPOINT", None)
            json.dump(
                {"index":index_name,
                 "pantry_path":test_pantry.pantry_path,
                 "file_system":file_system_type,
                 "S3_ENDPOINT":s3_endpoint},
                config_file
            )
        pantry = create_pantry(config_file=config_path)

    assert isinstance(pantry, weave.Pantry)


def test_pantry_factory_existing_pantry_config(test_pantry):
    """Create a pantry using the pantry factory with a config file saved in the
    pantry path."""
    test_pantry, index_name, _ = test_pantry
    file_system_type = test_pantry.file_system.__class__.__name__
    config_path = os.path.join(test_pantry.pantry_path, "config.json")

    with test_pantry.file_system.open(
        config_path,
        "w",
        encoding="utf-8"
    ) as config_file:
        s3_endpoint = os.environ.get("S3_ENDPOINT", None)
        json.dump(
            {"index": index_name,
             "pantry_path": test_pantry.pantry_path,
             "file_system": file_system_type,
             "S3_ENDPOINT": s3_endpoint},
            config_file
        )

    pantry = create_pantry(pantry_path=test_pantry.pantry_path,
                           file_system=test_pantry.file_system)

    assert isinstance(pantry, weave.Pantry)


def test_pantry_factory_invalid_index(test_pantry):
    """Ensure error will be raised if incorrect index is in the config.json"""
    test_pantry, _, _ = test_pantry
    invalid_index = "BadIndex"
    file_system_type = test_pantry.file_system.__class__.__name__
    config_path = os.path.join(test_pantry.pantry_path, "config.json")

    with test_pantry.file_system.open(
        config_path,
        "w",
        encoding="utf-8"
    ) as config_file:
        json.dump(
            {"index": invalid_index,
             "pantry_path": test_pantry.pantry_path,
             "file_system": file_system_type},
            config_file
        )

    with pytest.raises(
        ValueError,
        match=f"Index Type '{invalid_index}' is not supported",
    ):
        create_pantry(pantry_path=test_pantry.pantry_path,
                      file_system=test_pantry.file_system)


def test_pantry_factory_invalid_file_system(test_pantry):
    """Ensure error will be raised if incorrect file_system is in the
    config.json"""
    test_pantry, index_name, _ = test_pantry
    invalid_fs = "PRISMFileSystem"
    config_path = os.path.join(test_pantry.pantry_path, "config.json")

    with test_pantry.file_system.open(
        config_path,
        "w",
        encoding="utf-8"
    ) as config_file:
        json.dump(
            {"index":index_name,
             "pantry_path":test_pantry.pantry_path,
             "file_system":invalid_fs},
            config_file
        )
    with pytest.raises(
        ValueError,
        match=f"File System Type: '{invalid_fs}' is"
        f"not supported by this factory",
    ):
        create_pantry(pantry_path=test_pantry.pantry_path,
                      file_system=test_pantry.file_system)


def test_pantry_factory_invalid_args():
    """Ensure error will be raised if there is an incorrect combination of
    params."""
    with pytest.raises(
        ValueError,
        match="Invalid kwargs passed, unable to make pantry",
    ):
        create_pantry()


def test_pantry_factory_loads_mongo_config(test_pantry):
    """Ensure custom mongo connections are used when the proper keys exist in a
    config file."""
    test_pantry, _, index_constructor = test_pantry

    # Create a config for custom connections and settings for mongo.
    test_config = {
        "mongodb_host": "mongodb",
        "mongodb_port": 27017,
        "mongodb_username": "root",
        "mongodb_password": "example",
        "metadata_collection": "test-custom-metadata",
        "supplement_collection": "test-custom-supplement",
        "manifest_collection": "test-custom-manifest",
    }

    # Create a pantry using the 'default' method, but give it additional mongo
    # config settings.
    pantry = create_pantry(
        index=index_constructor,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        mongo_config=test_config,
    )

    # Have the pantry save its config settings to the global pantry config.
    pantry.save_setup_config()

    # Check that the config was written correctly and contains all the
    # additional mongo config settings.
    with test_pantry.file_system.open(
        os.path.join(test_pantry.pantry_path, "config.json")
    ) as config_file:
        pantry_config = json.load(config_file)
        assert all(pantry_config.get(key, None) == val for key, val
            in test_config.items())

    # Create a pantry using the saved config.
    pantry2 = create_pantry(
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        mongo_config=test_config,
    )

    # Ensure previously provided config settings are loaded from the config
    # when a new pantry is constructed from the global config.
    assert all(pantry2.setup_config.get(key, None) == val for key, val
                in test_config.items())

    # Also check the index creation settings are in the loaded config.
    assert pantry2.setup_config.get("index") == str(pantry2.index)
    assert pantry2.setup_config.get("file_system") == (
        pantry2.file_system.__class__.__name__
    )
    assert pantry2.setup_config.get("pantry_path") == pantry2.pantry_path
