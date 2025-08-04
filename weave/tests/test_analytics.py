"""Pytest for the weave analytics related functionality."""
import os
import shutil

import pytest

from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.analytics.dummy_baskets import (generate_dummy_baskets,
                                           run_index_basket_upload_test)
from weave.tests.pytest_resources import get_file_systems, PantryForTest


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
    """Fixture to set up and tear down test_basket."""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


def test_dummy_baskets_basket_count(test_pantry):
    """Test the generate_dummy_baskets function to ensure it creates the
    expected number of baskets and files.
    Expected: 10 baskets with 5 files each"""
    file_path = "test_dummy_data"
    pantry_path = test_pantry.pantry_path
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=file_path, num_basket_types=3)

    files = [f for f in os.listdir(file_path)
            if os.path.isfile(os.path.join(file_path, f))]

    # Create a new pantry
    upload_pantry = Pantry(IndexPandas, pantry_path=pantry_path,
                        file_system=test_pantry.file_system)

    # Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        upload_pantry.upload_basket(**basket)

    assert len(baskets) == 10
    assert len(files) == 5
    assert len(upload_pantry.index.to_pandas_df()) == 10

    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)


def test_dummy_baskets_empty_pantry(test_pantry):
    """Test the generate_dummy_baskets function with no baskets to ensure it
    handles empty cases correctly"""
    file_path = "test_dummy_data"
    pantry_path = test_pantry.pantry_path
    baskets = generate_dummy_baskets(basket_count=0, file_count=5,
        file_size_mb=1, file_path=file_path, num_basket_types=3)

    files = [f for f in os.listdir(file_path)
            if os.path.isfile(os.path.join(file_path, f))]

    # Create a new pantry
    upload_pantry = Pantry(IndexPandas, pantry_path=pantry_path,
                        file_system=test_pantry.file_system)

    # Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        upload_pantry.upload_basket(**basket)

    assert len(baskets) == 0
    assert len(files) == 5
    assert len(upload_pantry.index.to_pandas_df()) == 0

    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)


def test_dummy_baskets_no_files():
    """Test the generate_dummy_baskets function
    with no files to ensure it handles empty cases correctly"""
    file_path = "test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=0,
        file_size_mb=1, file_path=file_path, num_basket_types=3)

    assert len(baskets) == 0

    if os.path.exists(file_path):
        # Ensure no baskets are created when file_count is 0
        assert False
    else:
        assert True


def test_dummy_baskets_empty_files(test_pantry):
    """Test the generate_dummy_baskets function with empty files to
    ensure the correct number of empty files are generated"""
    file_path = "test_dummy_data"
    pantry_path = test_pantry.pantry_path
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=0, file_path=file_path, num_basket_types=3)

    files = [f for f in os.listdir(file_path)
             if os.path.isfile(os.path.join(file_path, f))]

    #Create a new pantry
    upload_pantry = Pantry(IndexPandas, pantry_path=pantry_path,
        file_system=test_pantry.file_system)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        upload_pantry.upload_basket(**basket)

    # Ensure the 5 files are created
    assert len(baskets) == 10
    assert len(files) == 5
    assert len(upload_pantry.index.to_pandas_df()) == 10

    # Check if the files are empty
    for f in os.listdir(file_path):
        full_path = os.path.join(file_path, f)
        if os.path.isfile(full_path):
            assert os.path.getsize(full_path) == 0, f"{f} should be empty"

    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)


def test_dummy_baskets_no_basket_types(test_pantry):
    """Test the generate_dummy_baskets function with no basket types"""
    file_path = "test_dummy_data"
    pantry_path = test_pantry.pantry_path
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=file_path, num_basket_types=0)

    files = [f for f in os.listdir(file_path)
             if os.path.isfile(os.path.join(file_path, f))]

    #Create a new pantry
    upload_pantry = Pantry(IndexPandas,
        pantry_path=pantry_path, file_system=test_pantry.file_system)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        upload_pantry.upload_basket(**basket)

    assert len(baskets) == 0
    assert len(files) == 5
    assert len(upload_pantry.index.to_pandas_df()) == 0

    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)


def test_dummy_baskets_negative_values():
    """Test the generate_dummy_baskets function with negative values
    to ensure it handles them correctly"""
    file_path = "test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=-10, file_count=-5,
        file_size_mb=-1, file_path=file_path, num_basket_types=-3)

    assert len(baskets) == 0

    if os.path.exists(file_path):
        assert False
    else:
        # Directory does not exist, which is expected
        assert True


def test_dummy_baskets_non_string_filepath():
    """Test the generate_dummy_baskets function with a non-string file_path"""
    with pytest.raises(TypeError, match="expected str, bytes or os.PathLike " \
                                        "object, not int"):
        generate_dummy_baskets(basket_count=10, file_count=5,
            file_size_mb=1, file_path=123, num_basket_types=3)


def test_run_index_basket_upload_test_works(test_pantry):
    """Test that the run_index_basket_upload_test function works as expected"""
    file_path = "test_dummy_data"
    pantry_path = test_pantry.pantry_path
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=file_path, num_basket_types=1)

    # Create a new pantry
    upload_pantry = Pantry(IndexPandas, pantry_path=pantry_path,
                           file_system=test_pantry.file_system)
    
    # Upload the test baskets
    _ = run_index_basket_upload_test(basket_list=baskets,
                                     pantry=upload_pantry)
    
    # Check that 10 baskets are in the index
    assert len(upload_pantry.index.to_pandas_df()) == 10

    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)


def test_run_index_basket_upload_test_empty_list(test_pantry):
    """Test that the run_index_basket_upload_test function does not upload any
    baskets when an empty basket_list is passed in
    """
    pantry_path = test_pantry.pantry_path

    # Create a new pantry
    upload_pantry = Pantry(IndexPandas, pantry_path=pantry_path,
                           file_system=test_pantry.file_system)
    
    # Upload the test baskets
    _ = run_index_basket_upload_test(basket_list=[],
                                     pantry=upload_pantry)
    
    # Check that 0 baskets are in the index
    assert len(upload_pantry.index.to_pandas_df()) == 0


def test_run_index_basket_upload_test_invalid_pantry():
    """Test that the correct error is raised when run_index_basket_upload_test
    function is called when a bad pantry object is passed in
    """
    file_path = "test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=file_path, num_basket_types=1)

    # Create a fake pantry from a string
    upload_pantry = "fake_pantry"
    
    with pytest.raises(AttributeError, match="'str' object has no attribute "\
                                             "'upload_basket'"):
        _ = run_index_basket_upload_test(basket_list=baskets,
                                         pantry=upload_pantry)
    
    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)


def test_run_index_basket_upload_test_num_basket_uploads(test_pantry):
    """Test that the run_index_basket_upload_test function only uploads the
    specified number of basket uploads
    """
    file_path = "test_dummy_data"
    pantry_path = test_pantry.pantry_path
    baskets = generate_dummy_baskets(basket_count=10, file_count=1,
        file_size_mb=1, file_path=file_path, num_basket_types=5)

    # Create a new pantry
    upload_pantry = Pantry(IndexPandas, pantry_path=pantry_path,
                           file_system=test_pantry.file_system)
    
    # Upload the test baskets
    _ = run_index_basket_upload_test(basket_list=baskets,
                                     pantry=upload_pantry,
                                     num_basket_uploads=5)
    
    # Check that 10 baskets are in the index
    assert len(upload_pantry.index.to_pandas_df()) == 5

    #Clean up the test files after the test is asserted
    if os.path.exists(file_path):
        shutil.rmtree(file_path)
