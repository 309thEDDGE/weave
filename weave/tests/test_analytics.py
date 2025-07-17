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

    if os.path.exists("weave/analytics/test_dummy_data"):
        shutil.rmtree("weave/analytics/test_dummy_data")
            
    if os.path.exists("weave/analytics/weave-demo-pantry"):
        shutil.rmtree("weave/analytics/weave-demo-pantry")

def test_basket_count():
    """Test the generate_dummy_baskets function to ensure it creates the expected number of baskets and files.
    Expected: 10 baskets with 5 files each"""
    file_path = "weave/analytics/test_dummy_data"
    pantry_path = "weave/analytics/weave-demo-pantry"
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

def test_empty_pantry():
    """Test the generate_dummy_baskets function with no baskets to ensure it handles empty cases correctly.s"""
    file_path = "weave/analytics/test_dummy_data"
    pantry_path = "weave/analytics/weave-demo-pantry"
    baskets = generate_dummy_baskets(basket_count=0, file_count=5, file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 0, "Ensure baskets returns a list of data to make 10 baskets"

    #Create a new pantry
    local_fs = LocalFileSystem()
    test_pantry = Pantry(IndexSQLite, pantry_path=str(pantry_path), file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        test_pantry.upload_basket(**basket)
    
    files = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
    assert len(files) == 5, "Ensure 5 files are created in the dummy data directory"
    cleanup_test_directory()
    
    
def test_no_files():
    """Test the generate_dummy_baskets function with no baskets to ensure it handles empty cases correctly.s"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=0, file_size_mb=1, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 10
    
    if os.path.exists(file_path):
        # Ensure no baskets are created when file_count is 0
        assert False, "Expected no files to be created when file_count is 0"
    else:
        assert True, "Dummy data directory does not exist, as expected when file_count is 0"

def test_empty_data():
    """Test the generate_dummy_baskets function with no baskets to ensure it handles empty cases correctly.s"""
    file_path = "weave/analytics/test_dummy_data"
    baskets = generate_dummy_baskets(basket_count=10, file_count=1, file_size_mb=0, file_path=str(file_path), num_basket_types=3)
    
    assert len(baskets) == 10
    
    if os.path.exists(file_path):
        # Ensure no baskets are created when file_count is 0
        assert False, "Expected no files to be created when file_count is 0"
    else:
        assert True, "Dummy data directory does not exist, as expected when file_count is 0"

    
       
    