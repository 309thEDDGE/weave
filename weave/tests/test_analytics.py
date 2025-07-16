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


def test_generate_dummy_baskets():
    """Test the generate_dummy_baskets function."""
    baskets = generate_dummy_baskets(basket_count=10, file_count=5, file_size_mb=1, file_path="weave/analytics/test_dummy_data", num_basket_types=3)
    
    #Create a new pantry
    local_fs = LocalFileSystem()
    pantry1 = Pantry(IndexSQLite, pantry_path="weave/analytics/weave-demo-pantry", file_system=local_fs)

    #Use ** to unpack the dictionary returned by generate_dummy_files
    for basket in baskets:
        pantry1.upload_basket(**basket)
        
    # Check if the pantry has the expected number of baskets
    data_dir = Path("weave/analytics/test_dummy_data")
    files = [f for f in data_dir.iterdir() if f.is_file()]
    assert len(files) == 5, f"Expected 5 files, but found {len(files)}"
    