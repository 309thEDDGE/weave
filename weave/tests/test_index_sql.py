"""This script contains tests that test that pytest is deleting baskets
correctly."""

import os
from unittest import mock

# Try-Except required to make sqlalchemy an optional dependency.
try:
    # For the sake of explicitly showing that sqlalchemy is optional, import
    # it here, even though it is not currently used in this file.
    # Pylint ignore the next unused-import pylint warning.
    # Also inline ruff ignore unused import (F401)
    # pylint: disable=unused-import
    # pyodbc is imported here because sqlalchemy requires it.
    import pyodbc # noqa: F401
    import sqlalchemy as sqla # noqa: F401
    # pylint: enable=unused-import
except ImportError:
    _HAS_REQUIRED_DEPS = False
else:
    _HAS_REQUIRED_DEPS = True
import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave.index.index_sql import IndexSQL
from weave.tests.pytest_resources import IndexForTest
from weave.tests.pytest_resources import PantryForTest
from weave.tests.pytest_resources import get_sample_basket_df

s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(
    name="test_pantry",
    params=[s3fs, local_fs],
    ids=["S3FileSystem", "LocalFileSystem"],
)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests"""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


@pytest.fixture(
    name="test_index",
    params=[IndexSQL],
    ids=["IndexSQL"],
)
def fixture_test_index(request):
    """Sets up test index for the tests"""
    index_constructor = request.param
    test_index = IndexForTest(index_constructor, local_fs)
    yield test_index
    test_index.cleanup_index()


# Skip tests if sqlalchemy is not installed.
@pytest.mark.skipif(
    not _HAS_REQUIRED_DEPS
    or not os.environ.get("MSSQL_PASSWORD", False),
    reason="Modules: 'pyodbc', 'sqlalchemy' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
# Mock the environment variables for the test.
@mock.patch.dict(os.environ, {}, clear=True)
def test_index_sql_no_env_vars():
    """Test that the SQL Index will raise an error if the required env vars
    are not set.
    """
    with pytest.raises(KeyError) as err:
        IndexSQL(LocalFileSystem(), "weave-test-pantry")

    assert (
        str(err.value) == "'The following environment variables must be set to "
        "use this class: MSSQL_HOST, MSSQL_USERNAME, MSSQL_PASSWORD.'"
    )


# Skip tests if sqlalchemy is not installed.
@pytest.mark.skipif(
    not _HAS_REQUIRED_DEPS
    or not os.environ.get("MSSQL_PASSWORD", False),
    reason="Modules: 'pyodbc', 'sqlalchemy' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
def test_index_sql_properties_are_read_only():
    """Test that the properties of the SQL Index (database_name, pantry_schema)
    are read only, and cannot be changed during execution.
    """
    pantry_path = (
        "pytest-temp-pantry"
        f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    ind = IndexSQL(LocalFileSystem(), pantry_path)

    original_db_name = "weave_db"
    original_schema_name = pantry_path.replace("-", "_")

    with pytest.raises(AttributeError):
        ind.database_name = "new_db_name"

    with pytest.raises(AttributeError):
        ind.pantry_schema = "new_schema_name"

    assert ind.database_name == original_db_name
    assert ind.pantry_schema == original_schema_name


# Skip tests if sqlalchemy is not installed.
@pytest.mark.skipif(
    not _HAS_REQUIRED_DEPS
    or not os.environ.get("MSSQL_PASSWORD", False),
    reason="Modules: 'pyodbc', 'sqlalchemy' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
def test_index_sql_tracks_different_pantries():
    """Test that the SQL Index will track different baskets using schemas."""
    sample_basket_df = get_sample_basket_df()
    uuid = str(sample_basket_df["uuid"].iloc[0])

    pantry_path = (
        "pytest-temp-pantry"
        f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
    )

    ind_1 = IndexSQL(LocalFileSystem(), pantry_path + "_1")
    ind_2 = IndexSQL(LocalFileSystem(), pantry_path + "_2")

    # Perform tracks and untracks on both indices, and ensure they are correct.
    ind_1.track_basket(sample_basket_df)
    assert len(ind_1) == 1
    assert len(ind_2) == 0

    ind_2.track_basket(sample_basket_df)
    assert len(ind_1) == 1
    assert len(ind_2) == 1

    ind_1.untrack_basket(uuid)
    assert len(ind_1) == 0
    assert len(ind_2) == 1

    ind_2.untrack_basket(uuid)
    assert len(ind_1) == 0
    assert len(ind_2) == 0

    # Need to manually clean up the database in this test.
    ind_1.execute_sql(f"DROP TABLE {ind_1.pantry_schema}.pantry_index;",
        commit=True)
    ind_1.execute_sql(f"DROP TABLE {ind_1.pantry_schema}.parent_uuids;",
        commit=True)
    ind_1.execute_sql(f"DROP SCHEMA {ind_1.pantry_schema};",
        commit=True)
    ind_2.execute_sql(f"DROP TABLE {ind_2.pantry_schema}.pantry_index;",
        commit=True)
    ind_2.execute_sql(f"DROP TABLE {ind_2.pantry_schema}.parent_uuids;",
        commit=True)
    ind_2.execute_sql(f"DROP SCHEMA {ind_2.pantry_schema};",
        commit=True)


# Skip tests if sqlalchemy is not installed.
@pytest.mark.skipif(
    not _HAS_REQUIRED_DEPS
    or not os.environ.get("MSSQL_PASSWORD", False),
    reason="Modules: 'pyodbc', 'sqlalchemy' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
def test_index_sql_track_basket_adds_to_parent_uuids(test_index):
    """Test that track_basket adds necessary rows to the parent_uuids table."""
    sample_basket_df = get_sample_basket_df()
    uuid = "1000"
    sample_basket_df["uuid"] = uuid

    # Add uuids to the parent_uuids of the df.
    sample_basket_df["parent_uuids"] = [["0001", "0002", "0003"]]

    # Track the basket.
    test_index.index.track_basket(sample_basket_df)

    ind = test_index.index
    # Manually check the parent_uuids table.
    rows, _ = ind.execute_sql(
        f"SELECT * FROM {test_index.index.pantry_schema}.parent_uuids"
    )

    print(rows)

    # Check we have the expected values.
    assert len(rows) == 3
    assert str(rows[0]) == str(("1000", "0001"))
    assert str(rows[1]) == str(("1000", "0002"))
    assert str(rows[2]) == str(("1000", "0003"))

    # Untrack the basket and ensure values are removed from parent_uuids table.
    test_index.index.untrack_basket(uuid)
    rows, _ = ind.execute_sql(
        f"SELECT * FROM {test_index.index.pantry_schema}.parent_uuids"
    )
    assert len(rows) == 0
