"""Pytest tests for the pantry factory."""
import json
import os
import tempfile

import pytest

import weave
from weave.pantry_factory import create_pantry
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

# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    name="test_pantry",
    params=file_systems,
    ids=file_systems_ids,
)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests."""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


def test_pantry_factory_default_args(test_pantry):
    """Test the pantry factory with the default create pantry args."""
    pantry = create_pantry(index=IndexPandas,
                           pantry_path=test_pantry.pantry_path,
                           file_system=test_pantry.file_system)

    assert isinstance(pantry, weave.Pantry)


def test_pantry_factory_local_config(test_pantry):
    """Create a pantry using the pantry factory with a locally saved config."""
    file_system_type = test_pantry.file_system.__class__.__name__

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "config.json")

        with open(config_path, "w", encoding="utf-8") as config_file:
            json.dump({"index":"IndexPandas",
                       "pantry_path":test_pantry.pantry_path,
                       "file_system":file_system_type,
                       "S3_ENDPOINT":os.environ["S3_ENDPOINT"]},
                      config_file)
        pantry = create_pantry(config_file=config_path)


    assert isinstance(pantry, weave.Pantry)


def test_pantry_factory_existing_pantry_config(test_pantry):
    """Create a pantry using the pantry factory with a config file saved in the
    pantry path."""
    file_system_type = test_pantry.file_system.__class__.__name__
    config_path = os.path.join(test_pantry.pantry_path, "config.json")

    with test_pantry.file_system.open(config_path,
                                      "w",
                                      encoding="utf-8") as config_file:
            json.dump({"index":"IndexPandas",
                       "pantry_path":test_pantry.pantry_path,
                       "file_system":file_system_type,
                       "S3_ENDPOINT":os.environ["S3_ENDPOINT"]},
                      config_file)

    pantry = create_pantry(pantry_path=test_pantry.pantry_path,
                           file_system=test_pantry.file_system)

    assert isinstance(pantry, weave.Pantry)


def test_pantry_factory_invalid_args(test_pantry):
    """Ensure error will be raised if incorrect params are given to pantry
    factory."""
    with pytest.raises(
        ValueError,
        match="Invalid kwargs passed, unable to make pantry",
    ):
        create_pantry()





