"""
Resources for use in pytest.
"""
import json
import os

from weave.upload import UploadBasket


def file_path_in_list(search_path, search_list):
    """Check if a file path is in a list (of file paths).

    Parameters
    ----------
    search_path: string
        The file path we want to search for.
    search_list: [string]
        A list of strings (presumably file paths)

    Returns True if any of the file paths in the search list end with the
    file path we're searching for, otherwise False.

    This allows us to determine if a file is in the list regardless of file
    system dependent prefixes such as /home/user/ which can usually be ignored.
    For example, if we have a list of file paths:
    ['/home/user/data/file.txt', '/home/user/data/this/is/test.txt']
    and we search for 'data/file.txt', we return True as the file exists.
    """
    search_path = str(search_path)
    for file_path in search_list:
        if str(file_path).endswith(search_path):
            return True

    return False


class BucketForTest:
    """Handles resources for much of weave testing."""

    def __init__(self, tmpdir, file_system, pantry_path=None):
        self.tmpdir = tmpdir
        self.file_system = file_system
        self.pantry_path = pantry_path

        if self.pantry_path is None:
            self.pantry_path = (
                "pytest-temp-bucket"
                f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
            )
        self.basket_list = []
        self._set_up_bucket()

    def _set_up_bucket(self):
        """Create a temporary Bucket for testing purposes."""
        try:
            self.file_system.mkdir(self.pantry_path)
        except FileExistsError:
            self.cleanup_bucket()
            self._set_up_bucket()

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
        """Upload a temporary (local) basket to the S3 test bucket."""
        upload_items = [
            {"path": str(tmp_basket_dir.realpath()), "stub": False}
        ]

        up_dir = UploadBasket(
            upload_items=upload_items,
            basket_type=basket_type,
            file_system=self.file_system,
            pantry_path=self.pantry_path,
            unique_id=uid,
            **kwargs,
        ).get_upload_path()
        return up_dir

    def cleanup_bucket(self):
        """Delete the temporary test bucket, including any uploaded baskets."""
        self.file_system.rm(self.pantry_path, recursive=True)


class IndexForTest:
        """Creates an index for testing."""
    def __init__(self, index_constructor, file_system, pantry_path=None):
        self.file_system = file_system
        self.pantry_path = pantry_path

        if self.pantry_path is None:
            self.pantry_path = (
                "pytest-temp-bucket"
                f"{os.environ.get('WEAVE_PYTEST_SUFFIX', '')}"
            )

        # This is only used in the sqlite implementation and will not affect
        # other implmentations.
        self.db_path = f"{self.pantry_path}.db"

        self.index = index_constructor(
            file_system=self.file_system,
            pantry_path=self.pantry_path,
            db_path=self.db_path,
        )

    def cleanup_index(self):
        """Clean up any artifacts created by index implementations."""
        # Remove SQLite db file.
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
