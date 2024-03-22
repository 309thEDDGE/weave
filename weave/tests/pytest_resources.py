"""Resources for use in pytest."""

import json
import os
import io
from pathlib import Path

import pandas as pd
import s3fs
from fsspec.implementations.local import LocalFileSystem

from weave.upload import UploadBasket


def get_file_systems():
    """Returns a list of file systems and their display names."""
    file_systems = [LocalFileSystem()]
    file_systems_ids = ["LocalFileSystem"]
    if "S3_ENDPOINT" in os.environ:
        s3 = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
        )
        file_systems.append(s3)
        file_systems_ids.append("S3FileSystem")
    return (file_systems, file_systems_ids)


def get_sample_basket_df():
    """Return a sample basket dataframe. THIS SHOULD ONLY BE USED AS REFERENCE
    FOR THE STRUCTURE OF THE DATAFRAME (ie column names, data types, etc.)"""
    df = pd.read_csv(io.StringIO(
        "uuid,upload_time,parent_uuids,basket_type,label,"
        "weave_version,address,storage_type\n"
        "1000,2023-10-23 16:52:43.992310+00:00,[],test_basket,test_label,"
        "1.2.0,pytest-temp-pantry/test_basket/1000,LocalFileSystem")
    )
    df["upload_time"] = pd.to_datetime(df["upload_time"])
    df["uuid"].astype(str)
    return df


def file_path_in_list(search_path, search_list):
    """Check if a file path is in a list (of file paths).

    Parameters
    ----------
    search_path: str
        The file path being searched for.
    search_list: [str]
        A list of strings (presumably file paths).

    Returns
    ----------
    bool: True if any of the file paths in the search list end with the
    file path being searched for, otherwise False.

    This allows us to determine if a file is in the list regardless of file
    system dependent prefixes such as /home/user/ which can usually be ignored.
    For example, given a list of file paths:
    ['/home/user/data/file.txt', '/home/user/data/this/is/test.txt']
    and the function is searching for 'data/file.txt',
    the function will return True as the file exists.
    """
    search_path = str(search_path)
    for file_path in search_list:
        if Path(file_path).match(search_path):
            return True

    return False


class PantryForTest:
    """Handles resources for much of weave testing."""
    def __init__(self, tmpdir, file_system, pantry_path=None):
        self.tmpdir = tmpdir
        self.file_system = file_system
        self.pantry_path = pantry_path

        if self.pantry_path is None:
            self.pantry_path = (
                "pytest-temp-pantry"
                f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
            )
        self.basket_list = []
        self._set_up_pantry()

    def _set_up_pantry(self):
        """Create a temporary pantry for testing purposes."""
        try:
            self.file_system.mkdir(self.pantry_path)
        except FileExistsError:
            self.cleanup_pantry()
            self._set_up_pantry()

    def set_up_basket(
        self, tmp_dir_name, file_name="test.txt", file_content="This is a test"
    ):
        """Create a temporary (local) basket, with a single text file."""
        tmp_basket_dir = self.tmpdir.mkdir(tmp_dir_name)
        tmp_basket_txt_file = tmp_basket_dir.join(file_name)

        if file_name[file_name.rfind(".") :] == ".json":
            with open(tmp_basket_txt_file, "w", encoding="utf-8") as outfile:
                json.dump(file_content, outfile)
        else:
            tmp_basket_txt_file.write(file_content)

        return tmp_basket_dir

    def add_lower_dir_to_temp_basket(self, tmp_basket_dir):
        """Add a nested directory inside the temporary basket."""
        nested_dir = tmp_basket_dir.mkdir("nested_dir")
        nested_dir.join("another_test.txt").write("more test text")
        return tmp_basket_dir

    def upload_basket(
        self, tmp_basket_dir, uid="0000", basket_type="test_basket", **kwargs
    ):
        """Upload a temporary (local) basket to the S3 test pantry."""
        upload_items = [
            {"path": str(tmp_basket_dir.realpath()), "stub": False}
        ]

        # Ignore pylint duplicate code. Code here is required to ensure proper
        # upload, and is similar to the pantry upload call--obviously.
        # pylint: disable-next=duplicate-code
        up_dir = UploadBasket(
            upload_items=upload_items,
            basket_type=basket_type,
            file_system=self.file_system,
            pantry_path=self.pantry_path,
            unique_id=uid,
            **kwargs,
        ).get_upload_path()
        return up_dir

    def cleanup_pantry(self):
        """Delete the temporary test pantry, including any uploaded baskets."""
        self.file_system.rm(self.pantry_path, recursive=True)

# This class is to facilitate creating and deleting indices for tests.
# No other functionality required, but it is needed for pytest
# pylint: disable-next=too-few-public-methods
class IndexForTest:
    """Creates an index for testing."""
    def __init__(self, index_constructor, file_system,
                 pantry_path=None, **kwargs):
        self.file_system = file_system
        self.pantry_path = pantry_path

        if self.pantry_path is None:
            self.pantry_path = (
                "pytest-temp-pantry"
                f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
            )

        # This is only used in the sqlite implementation and will not affect
        # other implmentations.
        self.db_path = f"weave-{self.pantry_path}.db"

        self.index = index_constructor(
            file_system=self.file_system,
            pantry_path=self.pantry_path,
            db_path=self.db_path,
            **kwargs,
        )

    def cleanup_index(self):
        """Clean up any artifacts created by index implementations."""
        if self.index.__class__.__name__ == "IndexSQLite":
            self.index.cur.close()
            self.index.con.close()
            # Remove SQLite db file.
            if os.path.exists(self.db_path):
                os.remove(self.db_path)

        cleanup_sql_index(self.index)

def cleanup_sql_index(index):
    """Clean up any IndexSQL"""
    if index.__class__.__name__ == "IndexSQL":
        # Drop the pantry_index (User Table) if it exists.
        index.execute_sql(f"""
            DROP TABLE IF EXISTS {index.pantry_schema}.pantry_index;
        """, commit=True)

        # Drop the parent_uuids (User Table) if it exists.
        index.execute_sql(f"""
            DROP TABLE IF EXISTS {index.pantry_schema}.parent_uuids;
        """, commit=True)

        # Drop the pantry_schema (Schema) if it exists.
        index.execute_sql(f"""
            DROP SCHEMA IF EXISTS {index.pantry_schema};
        """, commit=True)
