"""Pytest tests for the sqlite index."""
import os

import pytest

from weave.pantry import Pantry
from weave.index.index_sqlite import IndexSQLite
from weave.tests.pytest_resources import get_sample_basket_df, get_file_systems
from weave.tests.pytest_resources import IndexForTest
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

# Create fsspec objects to be tested, and add to file_systems list.
file_systems, file_systems_ids = get_file_systems()


# Test with different fsspec file systems (above).
@pytest.fixture(
    name="test_pantry",
    params=file_systems,
    ids=file_systems_ids,
)
def fixture_test_pantry(request, tmpdir):
    """Sets up test pantry for the tests"""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


@pytest.fixture(
    name="test_index",
    params=[IndexSQLite],
    ids=["IndexSQLite"],
)
def fixture_test_index(request):
    """Sets up test index for the tests"""
    index_constructor = request.param
    test_index = IndexForTest(index_constructor, file_systems[0])
    yield test_index
    test_index.cleanup_index()


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
    pantry_1.index.cur.close()
    pantry_1.index.con.close()
    pantry_2.index.cur.close()
    pantry_2.index.con.close()
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
    pantry_1.index.cur.close()
    pantry_1.index.con.close()
    pantry_2.index.cur.close()
    pantry_2.index.con.close()
    os.remove(pantry_1.index.db_path)
    os.remove(pantry_2.index.db_path)


def test_index_sqlite_track_basket_adds_to_parent_uuids(test_index):
    """Test that track_basket adds necessary rows to the parent_uuids table."""
    sample_basket_df = get_sample_basket_df()
    uuid = "1000"
    sample_basket_df["uuid"] = uuid

    # Add uuids to the parent_uuids of the df.
    sample_basket_df["parent_uuids"] = [["0001", "0002", "0003"]]

    # Track the basket.
    test_index.index.track_basket(sample_basket_df)

    # Use the cursor to check the parent_uuids table.
    cursor = test_index.index.con.cursor()
    cursor.execute("SELECT * FROM parent_uuids")
    rows = cursor.fetchall()

    # Check we have the expected values.
    assert len(rows) == 3
    assert rows[0] == (uuid, "0001")
    assert rows[1] == (uuid, "0002")
    assert rows[2] == (uuid, "0003")

    # Untrack the basket and ensure values are removed from parent_uuids table.
    test_index.index.untrack_basket(uuid)
    cursor.execute("SELECT * FROM parent_uuids")
    rows = cursor.fetchall()
    assert len(rows) == 0
