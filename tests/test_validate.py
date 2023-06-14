#in here we test everything we want to try and break it with
#every test function can only have 1 assert statement
#

from fsspec.implementations.local import LocalFileSystem
import s3fs
import pytest
from weave import validate

