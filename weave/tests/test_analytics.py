""" Pytest for the weave analytics related functionality. """
import os
import shutil
from fsspec.implementations.local import LocalFileSystem
from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.analytics.dummy_baskets import generate_dummy_baskets
from fsspec.implementations.local import LocalFileSystem
from weave.tests.pytest_resources import get_file_systems
from weave.tests.pytest_resources import PantryForTest
import pytest
from pathlib import Path


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
    file_path = os.path.join(test_pantry.pantry_path, "test_dummy_data")
    pantry_path = os.path.join(test_pantry.pantry_path, "dummy_pantry")
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=str(file_path), num_basket_types=3)

    assert len(baskets) == 10

    #Create a new pantry
    test_pantry = Pantry(IndexPandas, pantry_path=str(pantry_path),
                         file_system=test_pantry.file_system)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path) if
             os.path.isfile(os.path.join(file_path, f))]
    assert len(files) == 5

def test_dummy_baskets_empty_pantry():
    """Test the generate_dummy_baskets function with no baskets to ensure it 
    handles empty cases correctly"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=0, file_count=5, 
        file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 0

    #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite,
        pantry_path=str("weave/analytics/dummy_pantry"), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path) 
        if os.path.isfile(os.path.join(file_path, f))]
    assert len(files) == 5
    cleanup_test_directory()
    
    
def test_dummy_baskets_no_files():
    """Test the generate_dummy_baskets function 
    with no files to ensure it handles empty cases correctly"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=0, 
        file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 10
    
    if os.path.exists(file_path):
        # Ensure no baskets are created when file_count is 0
        assert False
    else:
        assert True
    
    cleanup_test_directory()

def test_dummy_baskets_empty_files():
    """Test the generate_dummy_baskets function with empty files to
    ensure the correct number of empty files are generated"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=0, file_path=str(file_path), num_basket_types=3)
    
      #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, 
        pantry_path=str("weave/analytics/dummy_pantry"), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path)
        if os.path.isfile(os.path.join(file_path, f))]
    assert len(files) == 5, "Ensure the 5 files are created"
    
    # Check if the files are empty
    for f in os.listdir(file_path):
        full_path = os.path.join(file_path, f)
        if os.path.isfile(full_path):
            assert os.path.getsize(full_path) == 0, f"{f} should be empty"
            
    cleanup_test_directory()
    
def test_dummy_baskets_no_basket_types():
    """Test the generate_dummy_baskets function with no basket types"""
    file_path = "weave/analytics/test_dummy_data"
    pantry_path = "weave/analytics/dummy_pantry"
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=str(file_path), num_basket_types=0)
    
    assert len(baskets) == 10

    #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, 
        pantry_path=str(pantry_path), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    # make sure there is only one directory created in dummy_pantry
    directories = [d for d in os.listdir(pantry_path)
        if os.path.isdir(os.path.join(pantry_path, d))]
    assert len(directories) == 1
    cleanup_test_directory()

    
def test_dummy_baskets_negative_values():
    """Test the generate_dummy_baskets function with negative values
        to ensure it handles them correctly"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=-10, file_count=-5, 
        file_size_mb=-1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 0
    
    if os.path.exists(file_path):
        assert False
    else:
        # Directory does not exist, which is expected
        assert True
    cleanup_test_directory()
    
def test_dummy_baskets_non_string_filepath():
    """Test the generate_dummy_baskets function with a non-string file_path"""
    file_path = 123
    baskets = generate_dummy_baskets(basket_count=10, file_count=5,
        file_size_mb=1, file_path=file_path, num_basket_types=3)
    
    assert len(baskets) == 10

    
    if (os.path.exists(file_path)):
        assert False
    else:
        assert True
        
    if os.path.exists(str(file_path)):
        shutil.rmtree(str(file_path))
        
    cleanup_test_directory()
    
       
    