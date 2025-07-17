""" Pytest for the weave analytics related functionality. """
import weave
from weave.__init__ import __version__ as weave_version
import shutil
import sys
import os
from datetime import datetime, timedelta
from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.index.index_sqlite import IndexSQLite
from weave.analytics.dummy_baskets import generate_dummy_baskets
from fsspec.implementations.local import LocalFileSystem
import pytest
from pathlib import Path

def cleanup_test_directory():
    """Cleanup the test directory by removing the dummy data and pantry directories.
    And remove the dummy_pantry.db file if it exists."""
    if os.path.exists("weave/analytics/test_dummy_data"):
        shutil.rmtree("weave/analytics/test_dummy_data")
    if os.path.exists("weave/analytics/dummy_pantry"):
        shutil.rmtree("weave/analytics/dummy_pantry")
    db_path = "weave/analytics/dummy_pantry.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except PermissionError:
            pass

def test_dummy_baskets_basket_count():
    """Test the generate_dummy_baskets function to ensure it creates the expected number of baskets and files.
    Expected: 10 baskets with 5 files each"""
    file_path = "weave/analytics/test_dummy_data"
    pantry_path = "weave/analytics/dummy_pantry"
    baskets = generate_dummy_baskets(basket_count=10, file_count=5, file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 10, "Ensure baskets returns a list of data to make 10 baskets"

    #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, pantry_path=str(pantry_path), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
    assert len(files) == 5, "Ensure 5 files are created in the dummy data directory"
    
    cleanup_test_directory()

def test_dummy_baskets_empty_pantry():
    """Test the generate_dummy_baskets function with no baskets to ensure it handles empty cases correctly"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=0, file_count=5, file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 0, "Ensure baskets returns a list of data to make 10 baskets"

    #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, pantry_path=str("weave/analytics/dummy_pantry"), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
    assert len(files) == 5, "Ensure 5 files are created in the dummy data directory"
    cleanup_test_directory()
    
    
def test_dummy_baskets_no_files():
    """Test the generate_dummy_baskets function with no files to ensure it handles empty cases correctly"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=0, file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 10
    
    if os.path.exists(file_path):
        # Ensure no baskets are created when file_count is 0
        assert False, "Expected no files to be created when file_count is 0"
    else:
        assert True, "Dummy data directory does not exist, as expected when file_count is 0"
    
    cleanup_test_directory()

def test_dummy_baskets_empty_files():
    """Test the generate_dummy_baskets function with empty files to ensure the correct number of empty files are generated"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=5, file_size_mb=0, file_path=str(file_path), num_basket_types=3)
    
      #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, pantry_path=str("weave/analytics/dummy_pantry"), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
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
    baskets = generate_dummy_baskets(basket_count=10, file_count=5, file_size_mb=1, file_path=str(file_path), num_basket_types=0)
    
    assert len(baskets) == 10, "Ensure baskets returns a list of data to make 10 baskets"

    #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, pantry_path=str(pantry_path), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    # make sure there is only one directory created in dummy_pantry
    directories = [d for d in os.listdir(pantry_path) if os.path.isdir(os.path.join(pantry_path, d))]
    assert len(directories) == 1, "Ensure only one directory for baskets is created in the pantry"
    cleanup_test_directory()

    
def test_dummy_baskets_negative_values():
    """Test the generate_dummy_baskets function with negative values to ensure it handles them correctly"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=-10, file_count=-5, file_size_mb=-1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 0, "Ensure baskets returns an empty list when negative values are provided"
    
    if os.path.exists(file_path):
        assert False
    else:
        # Directory does not exist, which is expected
        assert True
    cleanup_test_directory()
    
def test_dummy_baskets_non_string_filepath():
    """Test the generate_dummy_baskets function with negative values to ensure it handles them correctly"""
    file_path = 123
    baskets = generate_dummy_baskets(basket_count=10, file_count=5, file_size_mb=1, file_path=file_path, num_basket_types=3)
    
    if (os.path.exists(file_path)):
        assert False
    else:
        assert True
        
    if os.path.exists(str(file_path)):
        shutil.rmtree(str(file_path))
        
    cleanup_test_directory()
    
       
    