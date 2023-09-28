"""Pytest tests for the index directory."""
import os
import re

import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave.pantry import Pantry
from weave.index.index_sqlite import IndexSQLite
from weave.index.index_pandas import IndexPandas
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
# def fixture_test_index(request, tmpdir):
#     """Sets up test pantry for the tests"""
#     file_system = request.param
#     text_index = IndexForTest(tmpdir, file_system)
#     yield text_index
#     text_index.cleanup_index()


def test_index_is_different_between_pantries(test_pantry):

    pantry_1_path = os.path.join(test_pantry.pantry_path, "test-pantry-1")
    pantry_2_path = os.path.join(test_pantry.pantry_path, "test-pantry-2")
    test_pantry.file_system.mkdir(pantry_1_path)
    test_pantry.file_system.mkdir(pantry_2_path)

    tmp_txt_file = test_pantry.tmpdir.join("test.txt")
    print('tmp_txt_file: ', tmp_txt_file)
    tmp_txt_file.write("this is a test")

    upload_path_1 = os.path.join(pantry_1_path, "text.txt")
    upload_path_2 = os.path.join(pantry_2_path, "text.txt")

    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_1)
    test_pantry.file_system.upload(str(tmp_txt_file.realpath()), upload_path_2)



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

    print('pantry1 dbpath: ', pantry_1.index.db_path)
    print('pantry2 dbpath: ', pantry_2.index.db_path)

    for i in range(2):
        pantry_1.upload_basket(
            upload_items=[{"path":str(tmp_txt_file.realpath()), "stub":False}],
            basket_type="test-basket",
            unique_id="000" + str(i),
        )

        pantry_2.upload_basket(
            upload_items=[{"path":str(tmp_txt_file.realpath()), "stub":False}],
            basket_type="test-basket",
            unique_id="000" + str(i),
        )

    pantry_1.index.generate_index()
    pantry_2.index.generate_index()

    os.remove(pantry_1.index.db_path)
    os.remove(pantry_2.index.db_path)

    pantry_index_1 = pantry_1.index.to_pandas_df()
    pantry_index_2 = pantry_2.index.to_pandas_df()
    
    print('\nindex1: \n', pantry_index_1)
    print('\nindex2: \n', pantry_index_2)

    assert len(pantry_index_1) == 2
    assert len(pantry_index_2) == 2
    
    pantry1_basket = pantry_1.get_basket("0000")
    pantry2_basket = pantry_2.get_basket("0000")
    
    print('pantry1_basket: \n', pantry1_basket)
    print('pantry2_basket: \n', pantry2_basket)
    
    assert pantry_index_1["uuid"][0] == pantry1_basket.uuid
    assert pantry_index_1["upload_time"][0] == pantry1_basket.upload_time
    assert pantry_index_1["parent_uuids"][0] == pantry1_basket.parent_uuids
    assert pantry_index_1["basket_type"][0] == pantry1_basket.basket_type
    assert pantry_index_1["weave_version"][0] == pantry1_basket.weave_version
    assert pantry_index_1["address"][0] == pantry1_basket.address
    assert pantry_index_1["storage_type"][0] == pantry1_basket.storage_type
    # assert pantry_index_1["weave_version"] == pantry1_basket.uuid
    # assert pantry_index_1["uuid"] == pantry1_basket.uuid



    # with open(tmp_txt_file, "w", encoding="utf-8") as outfile:
    #     json.dump("this is a test", outfile)


    # pantry_1 = Pantry(
    #     IndexSQLite,
    #     pantry_path=te
    # )
    
    # tmp_basket_txt_file = test_pantry.tmpdir.mkdir("test-pantry-1")
    
    
    # print('file system: ', test_pantry.file_system.ls(test_pantry.pantry_path))
    # print('file system: ', test_pantry.file_system.ls(test_pantry.pantry_path))
    

    # pantry_2_path = test_pantry.pantry_path + "-2"
    
    # test_pantry.file_system.mkdir(pantry_2_path)
    # print('pantry 2 path: ', pantry_2_path)
    # pantry_2 = Pantry(
    #     IndexSQLite,
    #     pantry_path=test_pantry.pantry_path + "-2",
    #     file_system=test_pantry.file_system,
    #     sync=True,
    # )
    
    # Upload baskets to the 2 pantries
    # tmp_basket_dir = test_pantry.set_up_basket("basket_one")
    # test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir, uid="0001")
    # # pantry_2.upload_basket(tmp_basket_dir=tmp_basket_dir, uid="0001")
    # pantry_1 = Pantry(
    #     IndexSQLite,
    #     pantry_path=test_pantry.pantry_path,
    #     file_system=test_pantry.file_system,
    #     sync=True,
    # )
    # print('\npantry1 index: \n', pantry_1.index.to_pandas_df())
    # print('\n\npantry2 index: \n', pantry_2.index.to_pandas_df())
    # pantry.index.to_pandas_df()
    
    
    # test_pantry.file_system.rm(pantry_2_path, recursive=True)