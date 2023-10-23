"""This script contains tests that test that pytest is deleting baskets
correctly."""

import os
import sys

from fsspec.implementations.local import LocalFileSystem
# Try-Except required to make pyodbc an optional dependency.
try:
    import pyodbc
except ImportError:
    _HAS_PYODBC = False
else:
    _HAS_PYODBC = True
import pytest
import s3fs

from weave.index.create_index import create_index_from_fs
from weave.index.index_sql import IndexSQL
from weave.tests.pytest_resources import PantryForTest
from weave.tests.pytest_resources import get_sample_basket_df

# s3fs = s3fs.S3FileSystem(
#     client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
# )
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(
    name="test_pantry",
    params=[local_fs],
    ids=["LocalFileSystem"],
    # params=[s3fs, local_fs],
    # ids=["S3FileSystem", "LocalFileSystem"],
)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests"""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


# Skip tests if pyodbc is not installed.
@pytest.mark.skipif(
    "pyodbc" not in sys.modules or not _HAS_PYODBC
    or not os.environ["MSSQL_PASSWORD"],
    reason="Module 'pyodbc' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
def test_index_sql_properties_are_read_only():
    """Test that the properties of the SQL Index (database_name, pantry_schema),
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


# Skip tests if pyodbc is not installed.
@pytest.mark.skipif(
    "pyodbc" not in sys.modules or not _HAS_PYODBC
    or not os.environ["MSSQL_PASSWORD"],
    reason="Module 'pyodbc' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
def test_index_sql_tracks_different_pantries():
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
    ind_1.cur.execute(f"DROP TABLE {ind_1.pantry_schema}.pantry_index;")
    ind_1.cur.execute(f"DROP TABLE {ind_1.pantry_schema}.parent_uuids;")
    ind_1.cur.execute(f"DROP SCHEMA {ind_1.pantry_schema};")
    ind_1.cur.commit()

    ind_2.cur.execute(f"DROP TABLE {ind_2.pantry_schema}.pantry_index;")
    ind_2.cur.execute(f"DROP TABLE {ind_2.pantry_schema}.parent_uuids;")
    ind_2.cur.execute(f"DROP SCHEMA {ind_2.pantry_schema};")
    ind_2.cur.commit()
