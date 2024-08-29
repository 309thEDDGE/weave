"""Pytest tests for the index directory."""
import os
import re
import tempfile
import warnings

import pytest

from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.tests.pytest_resources import PantryForTest, get_file_systems


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
    """Sets up test pantry for the tests"""
    file_system = request.param
    test_pantry = PantryForTest(tmpdir, file_system)
    yield test_pantry
    test_pantry.cleanup_pantry()


def test_sync_index_gets_latest_index(test_pantry):
    """Tests IndexPandas.sync_index by generating two distinct objects and
    making sure that they are both syncing to the index pandas DF (represented
    by JSON) on the file_system."""

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    pantry2 = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry2.index.generate_index()

    # assert length of index includes both baskets and excludes the index
    assert len(pantry.index.to_pandas_df()) == 2

    #assert all baskets in index are not index baskets
    for i in range(len(pantry.index.to_pandas_df())):
        basket_type = pantry.index.to_pandas_df()["basket_type"][i]
        assert basket_type != "index"


def test_sync_index_calls_generate_index_if_no_index(test_pantry):
    """Test to make sure that if there isn't a index available then
    generate_index will still be called."""

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    assert len(pantry.index.to_pandas_df()) == 1

def test_sync_index_autocleanup_if_false(test_pantry):
    """Tests that IndexPandas.sync raises the correct warning with
    auto-cleanup turned off."""
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
        auto_cleanup=False,
    )

    for i in range (25):
        tmp_basket_dir_one = test_pantry.set_up_basket(f"basket_{i}")
        test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one,
                             uid=f"000{i}")

        pantry.index.generate_index()

    with warnings.catch_warnings(record=True) as warn:
        pantry.index.sync_index()
    warning_list = [warn[i].message for i in range(len(warn))]
    warning_1 = warning_list[0]
    assert warning_1.args[0] == (
        "The index basket count is 25. "
        "Consider running weave.IndexPandas.clean_up_indices"
    )

def test_get_index_time_from_path(test_pantry):
    """Tests Index._get_index_time_from_path to ensure it returns the correct
    string."""

    path = "C:/asdf/gsdjls/1234567890-index.json"
    # Testing a protected access var here.
    # pylint: disable-next=protected-access
    time = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    ).index._get_index_time_from_path(path=path)
    assert time == 1234567890


def test_clean_up_indices_n_not_int(test_pantry):
    """Tests that IndexPandas.clean_up_indices errors on a str (should be int).
    """

    test_str = "the test"
    with pytest.raises(
        ValueError,
        match=re.escape("invalid literal for int() with base 10: 'the test'"),
    ):
        pantry = Pantry(
            IndexPandas,
            pantry_path=test_pantry.pantry_path,
            file_system=test_pantry.file_system,
            sync=True,
        )
        pantry.index.clean_up_indices(n_keep=test_str)


def test_clean_up_indices_leaves_n_indices(test_pantry):
    """Tests that IndexPandas.clean_up_indices leaves behind the correct number
    of indices."""

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    pantry.index.generate_index()

    # Now there should be two index baskets. clean up all but one of them:
    pantry.index.clean_up_indices(n_keep=1)
    index_path = os.path.join(test_pantry.pantry_path, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 1


def test_clean_up_indices_with_n_greater_than_num_of_indices(test_pantry):
    """Tests that IndexPandas.clean_up_indices behaves well when given a number
    greater than the total number of indices."""

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")
    pantry.index.generate_index()

    # Now there should be two index baskets. clean up all but three of them:
    pantry.index.clean_up_indices(n_keep=3)
    index_path = os.path.join(test_pantry.pantry_path, "index")
    assert len(test_pantry.file_system.ls(index_path)) == 2


def test_is_index_current(test_pantry):
    """Creates two IndexPandas objects and pits them against eachother in order
    to ensure that IndexPandas.is_index_current is working as expected."""

    # Put basket in the temporary pantry
    tmp_basket_dir_one = test_pantry.set_up_basket("basket_one")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_one, uid="0001")

    # Create index
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry.index.to_pandas_df()

    # Add another basket
    tmp_basket_dir_two = test_pantry.set_up_basket("basket_two")
    test_pantry.upload_basket(tmp_basket_dir=tmp_basket_dir_two, uid="0002")

    # Regenerate index outside of current index object
    pantry2 = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    pantry2.index.generate_index()
    assert pantry2.index.is_index_current() is True
    assert pantry.index.is_index_current() is False

def test_index_updated_after_new_pantry_basket_upload(test_pantry):
    """Tests basket_upload updates the index after creating IndexPandas pantry
    object when another IndexPandas index exists."""

    # Create first pantry
    pantry = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    # Create temp directory for uploading basket
    with tempfile.TemporaryDirectory(dir=".") as tmpdir:
        tmp_file_path = os.path.join(tmpdir, "temp_basket.txt")
        with open(tmp_file_path, "w", encoding="utf-8") as tmp_file:
            pantry.upload_basket(
                upload_items=[{"path":tmp_file.name, "stub":False}],
                basket_type="test-1",
            )
    # Create second pantry
    pantry2 = Pantry(
        IndexPandas,
        pantry_path=test_pantry.pantry_path,
        file_system=test_pantry.file_system,
        sync=True,
    )
    # Upload basket with new index
    with tempfile.TemporaryDirectory(dir=".") as tmpdir:
        tmp_file_path = os.path.join(tmpdir, "temp_basket.txt")
        with open(tmp_file_path, "w", encoding="utf-8") as tmp_file:
            pantry2.upload_basket(
                upload_items=[{"path":tmp_file.name, "stub":False}],
                basket_type="test-1",
            )
    assert len(pantry2.index.to_pandas_df()) == 2
