#in here we test everything we want to try and break it with
#every test function can only have 1 assert statement
#

import os
from fsspec.implementations.local import LocalFileSystem
import s3fs
import pytest
from weave import validate


def test_manifest_uuid():
    
    print("local file system: ", LocalFileSystem())
    
    
    # tempPath = './TestingValidation/'
    # validate_bucket(tempPath)
    
    