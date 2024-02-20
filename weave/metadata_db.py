"""Contains scripts concerning MongoDB functionality."""

import pandas as pd
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

from .basket import Basket
from .config import get_file_system, get_mongo_db


def load_mongo(index_table, collection="metadata", **kwargs):
    """Load metadata from baskets into the mongo database.

       A metadata.json is created in baskets when the metadata
       field is provided upon upload. This metadata is added to the
       Mongo database when invoking load_mongo. UUID, and basket_type
       from the index_table are also added to Mongo for referrence
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
        **file_system: fsspec object (required)
            The file system to retrieve the baskets' metadata from.
        """
    if not _HAS_PYMONGO:
        raise ImportError("Missing Dependency. The package 'pymongo' "
                          "is required to use this function")

    if not isinstance(index_table, pd.DataFrame):
        raise TypeError("Invalid datatype for index_table: "
                        "must be Pandas DataFrame")

    if not isinstance(collection, str):
        raise TypeError("Invalid datatype for collection: "
                        "must be a string")

    required_columns = ["uuid", "basket_type", "address"]

    for required_column in required_columns:
        if required_column not in index_table.columns.values.tolist():
            raise ValueError("Invalid index_table: missing "
                             f"{required_column} column")

    file_system = kwargs.get("file_system", None)
    if file_system is None:
        file_system = get_file_system()
    database = get_mongo_db().mongo_metadata

    for _, row in index_table.iterrows():
        basket = Basket(row["address"], file_system=file_system)
        metadata = basket.get_metadata()
        if metadata is None:
            continue
        manifest = basket.get_manifest()
        mongo_metadata = {}
        mongo_metadata["uuid"] = manifest["uuid"]
        mongo_metadata["basket_type"] = manifest["basket_type"]
        mongo_metadata.update(metadata)

        # If the UUID already has metadata loaded in MongoDB,
        # the metadata should not be loaded to MongoDB again.
        if 0 == database[
            collection
        ].count_documents({"uuid": manifest["uuid"]}):
            database[collection].insert_one(mongo_metadata)
