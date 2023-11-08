"""This script contains tests that test that pytest is deleting baskets
correctly."""

import os
import sys

from fsspec.implementations.local import LocalFileSystem
# Try-Except required to make pyodbc an optional dependency.
# Ignore pylint. This is used to explicitly show the optional dependency.
# pylint: disable=duplicate-code
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
    name="set_up_tb_no_cleanup",
    params=[s3fs, local_fs],
    ids=["S3FileSystem", "LocalFileSystem"],
)
def fixture_set_up_tb_no_cleanup(request, tmpdir):
    """Sets up test basket fixture."""
    file_system = request.param
    temp_basket = PantryForTest(tmpdir, file_system)
    # Purposefully don't clean up pantry, it will be cleaned up in the test.
    return temp_basket


def test_weave_pytest_suffix(set_up_tb_no_cleanup):
    """Test that env var suffix works, and pantrys are still deleted."""
    # Check pantry name includes suffix if applicable.
    suffix = os.environ.get("WEAVE_PYTEST_SUFFIX", "")
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
    "pyodbc" not in sys.modules or not _HAS_PYODBC
    or not os.environ["MSSQL_PASSWORD"] or not os.environ["MSSQL_HOST"],
    reason="Module 'pyodbc' required for this test "
    "AND env variables: 'MSSQL_HOST', 'MSSQL_PASSWORD'",
)
def test_github_cicd_sql_server():
    """Test that the MS SQL Server is properly setup in CICD."""
    # Documentation for mssql container:
    # https://hub.docker.com/_/microsoft-mssql-server
    # Default System Administrator username is "sa"
    username = "sa"
    mssql_password = os.environ["MSSQL_PASSWORD"]
    server = os.environ["MSSQL_HOST"]
    database = "tempdb"

    # Pylint has a problem recognizing 'connect' as a valid member function
    # so we ignore that here.
    # pylint: disable-next=c-extension-no-member
    con = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={mssql_password};"
        f"Encrypt=no;"
    )
    cur = con.cursor()

    # Create a temporary table for testing.
    cur.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
        JOIN sys.schemas s ON (t.schema_id = s.schema_id)
        WHERE s.name = 'dbo' AND t.name = 'test_table'
    )
    CREATE TABLE dbo.test_table (
        uuid varchar(64),
        num int
    );
    """)

    # Insert a test value, and then check we can retrieve the value.
    cur.execute("""
        INSERT INTO dbo.test_table (uuid, num) VALUES ('0001', 1);
    """)
    cur.commit()
    assert cur.execute("SELECT * FROM dbo.test_table").fetchall() != []

    # Delete the test value, and then check it was actually deleted.
    cur.execute("""DELETE FROM dbo.test_table WHERE uuid = '0001';""")
    cur.commit()
    assert cur.execute("SELECT * FROM dbo.test_table;").fetchall() == []

    cur.execute("""DROP TABLE dbo.test_table;""")
    cur.commit()
