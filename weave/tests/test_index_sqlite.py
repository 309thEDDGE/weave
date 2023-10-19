"""Pytest tests for the sqlite index."""
import os

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave.pantry import Pantry
from weave.index.index_sqlite import IndexSQLite
from weave.tests.pytest_resources import PantryForTest


###############################################################################
#                      Pytest Fixtures Documentation:                         #
#            https://docs.pytest.org/en/7.3.x/how-to/fixtures.html            #
#                                                                             #
#                  https://docs.pytest.org/en/7.3.x/how-to/                   #
#          fixtures.html#teardown-cleanup-aka-fixture-finalization            #
#                                                                             #
#  https://docs.pytest.org/en/7.3.x/how-to/fixtures.html#fixture-parametrize  #
###############################################################################

# Pylint doesn't like redefining the test fixture here from
# test_basket, but this is the right way to do it if at some
# point in the future the two need to be differentiated.
# pylint: disable=duplicate-code

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


def test_index_two_pantries_with_same_name(test_pantry):
    """Validate that 2 pantry objects with the same basename have their
    own index.
    """
    pantry_1_path = os.path.join(test_pantry.pantry_path, "test-pantry-1")
    pantry_2_path = os.path.join(test_pantry.pantry_path,
                                 "test",
                                 "test-pantry-1")

    test_pantry.file_system.mkdir(pantry_1_path)
    test_pantry.file_system.mkdir(pantry_2_path)

    tmp_txt_file = test_pantry.tmpdir.join("test.txt")
    tmp_txt_file.write("this is a test")

    upload_path_1 = os.path.join(pantry_1_path, "text.txt")
    upload_path_2 = os.path.join(pantry_2_path, "text.txt")

    # Must upload a file because Minio will remove empty directories
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_1)
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_2)

    # Make the Pantries.
    pantry_1 = Pantry(
        IndexSQLite,
        pantry_path=pantry_1_path,
        file_system=test_pantry.file_system,
    )

    pantry_2 = Pantry(
        IndexSQLite,
        pantry_path=pantry_2_path,
        file_system=test_pantry.file_system,
    )

    # Because the two Pantry Objects have the same name, or path basename,
    # the db files must be different.
    assert pantry_1.index.db_path != pantry_2.index.db_path

    # Remove the .db files that are not cleaned up with 'test_pantry'
    os.remove(pantry_1.index.db_path)
    os.remove(pantry_2.index.db_path)


def test_index_uploaded_basket_not_found_in_another_index(test_pantry):
    """Validate that a basket uploaded to one pantry does not show up in
    another pantry.
    """
    pantry_1_path = os.path.join(test_pantry.pantry_path, "test-pantry-1")
    pantry_2_path = os.path.join(test_pantry.pantry_path, "test-pantry-2")

    test_pantry.file_system.mkdir(pantry_1_path)
    test_pantry.file_system.mkdir(pantry_2_path)

    tmp_txt_file = test_pantry.tmpdir.join("test.txt")
    tmp_txt_file.write("this is a test")

    upload_path_1 = os.path.join(pantry_1_path, "text.txt")
    upload_path_2 = os.path.join(pantry_2_path, "text.txt")

    # Must upload a file because Minio will remove empty directories
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_1)
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_2)

    # Make the Pantries.
    pantry_1 = Pantry(
        IndexSQLite,
        pantry_path=pantry_1_path,
        file_system=test_pantry.file_system,
    )

    pantry_2 = Pantry(
        IndexSQLite,
        pantry_path=pantry_2_path,
        file_system=test_pantry.file_system,
    )

    # Upload a basket to each pantry.
    pantry_1.upload_basket(
        upload_items=[{"path":str(tmp_txt_file.realpath()), "stub":False}],
        basket_type="test-basket",
        unique_id="0000",
    )
    pantry_2.upload_basket(
        upload_items=[{"path":str(tmp_txt_file.realpath()), "stub":False}],
        basket_type="test-basket",
        unique_id="0001",
    )

    # Check that the indices are not the same
    pantry_1_index = pantry_1.index.to_pandas_df()
    pantry_2_index = pantry_2.index.to_pandas_df()

    # Validate that each basket was uploaded to the proper pantry, and
    # not the other.
    assert not pantry_1_index.equals(pantry_2_index)
    assert "0000" in pantry_1_index["uuid"].values
    assert "0000" not in pantry_2_index["uuid"].values
    assert "0001" in pantry_2_index["uuid"].values
    assert "0001" not in pantry_1_index["uuid"].values

    # Remove the .db files that are not cleaned up with 'test_pantry'
    os.remove(pantry_1.index.db_path)
    os.remove(pantry_2.index.db_path)
