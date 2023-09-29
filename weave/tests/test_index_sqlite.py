"""Pytest tests for the index directory."""
import os
from datetime import datetime

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


def test_index_is_different_between_pantries(test_pantry):
    """Validate that pantries will have their own index/db file instead of
    sharing one
    """
    pantry_1_path = os.path.join(test_pantry.pantry_path, "test-pantry-1")
    pantry_2_path = os.path.join(test_pantry.pantry_path, "test-pantry-2")

    test_pantry.file_system.mkdir(pantry_1_path)
    test_pantry.file_system.mkdir(pantry_2_path)

    tmp_txt_file = test_pantry.tmpdir.join("test.txt")
    tmp_txt_file.write("this is a test")

    upload_path_1 = os.path.join(pantry_1_path, "text.txt")
    upload_path_2 = os.path.join(pantry_2_path, "text.txt")

    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_1)
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_2)

    # Make the pantries.
    pantry_1 = Pantry(
        IndexSQLite,
        pantry_path=pantry_1_path,
        file_system=test_pantry.file_system,
        sync=True,
    )

    pantry_2 = Pantry(
        IndexSQLite,
        pantry_path=pantry_2_path,
        file_system=test_pantry.file_system,
        sync=True,
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

    pantry_index_1 = pantry_1.index.to_pandas_df()
    pantry_index_2 = pantry_2.index.to_pandas_df()

    assert len(pantry_index_1) == 1
    assert len(pantry_index_2) == 1

    pantry1_basket = pantry_1.get_basket("0000")
    pantry2_basket = pantry_2.get_basket("0001")

    assert pantry_index_1["uuid"][0] == pantry1_basket.uuid
    assert isinstance(pantry_index_1["upload_time"][0], datetime)
    assert pantry_index_1["parent_uuids"][0] == pantry1_basket.parent_uuids
    assert pantry_index_1["basket_type"][0] == pantry1_basket.basket_type
    assert pantry_index_1["weave_version"][0] == pantry1_basket.weave_version
    assert pantry_index_1["address"][0] == pantry1_basket.address
    assert pantry_index_1["storage_type"][0] == pantry1_basket.storage_type

    assert pantry_index_2["uuid"][0] == pantry2_basket.uuid
    assert isinstance(pantry_index_1["upload_time"][0], datetime)
    assert pantry_index_2["parent_uuids"][0] == pantry2_basket.parent_uuids
    assert pantry_index_2["basket_type"][0] == pantry2_basket.basket_type
    assert pantry_index_2["weave_version"][0] == pantry2_basket.weave_version
    assert pantry_index_2["address"][0] == pantry2_basket.address
    assert pantry_index_2["storage_type"][0] == pantry2_basket.storage_type

    # Remove the .db files.
    os.remove(pantry_1.index.db_path)
    os.remove(pantry_2.index.db_path)


def test_index_pantry_with_same_name(test_pantry):
    """Validate that 2 pantries with the same name will have unique indices
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

    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_1)
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_2)

    # Make the Pantries.
    pantry_1 = Pantry(
        IndexSQLite,
        pantry_path=pantry_1_path,
        file_system=test_pantry.file_system,
        sync=True,
    )

    pantry_2 = Pantry(
        IndexSQLite,
        pantry_path=pantry_2_path,
        file_system=test_pantry.file_system,
        sync=True,
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

    pantry_1.index.generate_index()
    pantry_2.index.generate_index()

    pantry_index_1 = pantry_1.index.to_pandas_df()
    pantry_index_2 = pantry_2.index.to_pandas_df()

    assert len(pantry_index_1) == 1
    assert len(pantry_index_2) == 1

    pantry1_basket = pantry_1.get_basket("0000")
    pantry2_basket = pantry_2.get_basket("0001")

    assert pantry_index_1["uuid"][0] == pantry1_basket.uuid
    assert isinstance(pantry_index_1["upload_time"][0], datetime)
    assert pantry_index_1["parent_uuids"][0] == pantry1_basket.parent_uuids
    assert pantry_index_1["basket_type"][0] == pantry1_basket.basket_type
    assert pantry_index_1["weave_version"][0] == pantry1_basket.weave_version
    assert pantry_index_1["address"][0] == pantry1_basket.address
    assert pantry_index_1["storage_type"][0] == pantry1_basket.storage_type

    assert pantry_index_2["uuid"][0] == pantry2_basket.uuid
    assert isinstance(pantry_index_1["upload_time"][0], datetime)
    assert pantry_index_2["parent_uuids"][0] == pantry2_basket.parent_uuids
    assert pantry_index_2["basket_type"][0] == pantry2_basket.basket_type
    assert pantry_index_2["weave_version"][0] == pantry2_basket.weave_version
    assert pantry_index_2["address"][0] == pantry2_basket.address
    assert pantry_index_2["storage_type"][0] == pantry2_basket.storage_type

    # Remove the .db files.
    os.remove(pantry_1.index.db_path)
    os.remove(pantry_2.index.db_path)
