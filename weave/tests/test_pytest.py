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

from weave.tests.pytest_resources import PantryForTest


s3fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()


# Test with two different fsspec file systems (above).
@pytest.fixture(
    params=[s3fs, local_fs],
    ids=["S3FileSystem", "LocalFileSystem"],
)
def set_up_tb_no_cleanup(request, tmpdir):
    """Sets up test basket fixture."""
    file_system = request.param
    temp_basket = PantryForTest(tmpdir, file_system)
    # Purposefully don't clean up pantry, it will be cleaned up in the test.
    return temp_basket

# Ignore pylint's warning "redefined-outer-name" as this is simply
# how pytest works when it comes to pytest fixtures.
# pylint: disable=redefined-outer-name
def test_weave_pytest_suffix(set_up_tb_no_cleanup):
    """Test that env var suffix works, and pantrys are still deleted."""
    # Check pantry name includes suffix if applicable.
    suffix = os.environ.get('WEAVE_PYTEST_SUFFIX', '')
    assert set_up_tb_no_cleanup.pantry_path == f"pytest-temp-pantry{suffix}"

    # Check the pantry was made.
    assert set_up_tb_no_cleanup.file_system.exists(
        set_up_tb_no_cleanup.pantry_path
    )

    # Cleanup the pantry.
    set_up_tb_no_cleanup.cleanup_pantry()

    # Check the pantry is actually deleted.
    assert not set_up_tb_no_cleanup.file_system.exists(
        set_up_tb_no_cleanup.pantry_path
    )

# Skip tests if pyodbc is not installed.
@pytest.mark.skipif(
    "pyodbc" not in sys.modules or not _HAS_PYODBC,
    reason="Module 'pyodbc' required for this test",
)
def test_github_cicd_sql_server():
    """Test that the MS SQL Server is properly setup in CICD."""
    mssql_password = os.environ["MSSQL_PASSWORD"]
    assert mssql_password is not None

    # con = pyodbc.connect("Server=localhost,1433;Initial Catalog=MyTestDb;"
    #                      f"User Id=sa;Password={mssql_password};")

    server = 'localhost'
    database = 'test_db'
    username = 'sa'

    con = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};DATABASE={database};"
        f"UID={username};PWD={mssql_password}"
    )
    cur = con.cursor()

    print(cur.execute("SELECT @@version;").fetchone())

    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_table(
        uuid TEXT, num INT, PRIMARY KEY(uuid), UNIQUE(uuid));
    """)
    cur.execute("""INSERT INTO test_table VALUES ("0001", 1);""")
    cur.commit()
    print(cur.execute("SELECT * FROM test_table").fetchall())
