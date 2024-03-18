"""Contains scripts concerning MongoDB functionality."""

# Try-Except required to make pymongo an optional dependency.

# Ignore pylint duplicate code. Code here is used to explicitly show pymongo is
# an optional dependency. Duplicate code is found in config.py (where pymongo
# is actually imported)
# pylint: disable-next=duplicate-code
try:
    # For the sake of explicitly showing that pymongo is optional, import
    # pymongo here, even though it is not currently used in this file.
    # Pylint ignore the next unused-import pylint warning.
    # Also inline ruff ignore unused import (F401)
    # pylint: disable-next=unused-import
    import pymongo # noqa: F401
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True

from .mongo_db import MongoDB


def load_mongo(index_table, collection="metadata", **kwargs):
    """Load files from baskets into the mongo database.

    A metadata.json is created in baskets when the metadata
    field is provided upon upload. This metadata, along with the manifest and
    supplement are added to the Mongo database when invoking load_mongo. UUID,
    and basket_type from the index_table are also added to Mongo for referrence
    back to the datasource.

    Parameters
    ----------
    index_table: dataframe
        Weave index dataframe fetched using the Index class.
        The dataframe must include the following columns:
            uuid
            basket_type
            address
    collection: str (default="metadata")
        Metadata will be added to the Mongo collection specified.
    **file_system: fsspec object (optional)
        The file system to retrieve the baskets' metadata from.
    """

    file_system = kwargs.get("file_system", None)
    mongo_db = MongoDB(index_table=index_table,
                       database="mongo_metadata",
                       file_system=file_system)

    mongo_db.load_mongo_metadata(collection=collection)
    mongo_db.load_mongo_manifest(collection="manifest")
    mongo_db.load_mongo_supplement(collection="supplement")
