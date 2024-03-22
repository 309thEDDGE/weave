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
    import pymongo  # noqa: F401
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True

from .basket import Basket
from .config import get_file_system, get_mongo_db

class MongoDB():
    """Initializes mongo class. Creates a Mongo DB and facilitates the
    uploading of files to mongo collections.
    """

    def __init__(self, index_table, database="mongo_db", **kwargs):
        """Creates the mongo database and checks for any errors that could
        occur when creating the database.

        Parameters
        ----------
        index_table: dataframe
            Weave index dataframe fetched using the Index class.
            The dataframe must include the following columns:
                uuid
                basket_type
                address
        database: str (default=mongo_db)
            The name of the database you want to upload to. The default name
            of the database is mongo_db
        **file_system: fsspec object (optional)
            The file system to retrieve the baskets' metadata from.
        """
        self.index_table = index_table
        self.database = get_mongo_db()[database]
        if not _HAS_PYMONGO:
            raise ImportError("Missing Dependency. The package 'pymongo' "
                              "is required to use this function")

        if not isinstance(self.index_table, pd.DataFrame):
            raise TypeError("Invalid datatype for index_table: "
                            "must be Pandas DataFrame")

        required_columns = ["uuid", "basket_type", "address"]

        for required_column in required_columns:
            if required_column not in self.index_table.columns.values.tolist():
                raise ValueError("Invalid index_table: missing "
                                 f"{required_column} column")

        self.file_system = kwargs.get("file_system", None)
        if self.file_system is None:
            self.file_system = get_file_system()


    def load_mongo_metadata(self, collection="metadata"):
        """Load metadata from baskets into the mongo database.

        A metadata.json is created in baskets when the metadata
        field is provided upon upload. This metadata is added to the
        Mongo database when invoking load_mongo_metadata. UUID and basket_type
        from the index_table are also added to Mongo for referrence
        back to the datasource.

        Parameters
        ----------
        collection: str (default="metadata")
            Metadata will be added to the Mongo collection specified.
        """
        if not isinstance(collection, str):
            raise TypeError("Invalid datatype for metadata collection: "
                            "must be a string")

        for _, row in self.index_table.iterrows():
            basket = Basket(row["address"], file_system=self.file_system)
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
            if 0 == self.database[
                collection
            ].count_documents({"uuid": manifest["uuid"]}):
                self.database[collection].insert_one(mongo_metadata)


    def load_mongo_manifest(self, collection="manifest"):
        """Load manifest from baskets into the mongo database.

        A manifest.json is created in baskets upon upload. This manifest is
        added to the Mongo database when invoking load_mongo_manifest. UUID,
        and basket_type from the index_table are also added to Mongo for
        referrence back to the datasource.

        Parameters
        ----------
        collection: str (default="manifest")
            Manifest will be added to the Mongo collection specified.
        """
        if not isinstance(collection, str):
            raise TypeError("Invalid datatype for manifest collection: "
                            "must be a string")

        for _, row in self.index_table.iterrows():
            basket = Basket(row["address"], file_system=self.file_system)
            mongo_manifest = basket.get_manifest()
            if mongo_manifest is None:
                continue

            # If the UUID already has the manifest loaded in MongoDB,
            # the manifest should not be loaded to MongoDB again.
            if 0 == self.database[
                collection
            ].count_documents({"uuid": mongo_manifest["uuid"]}):
                self.database[collection].insert_one(mongo_manifest)


    def load_mongo_supplement(self, collection="supplement"):
        """Load supplement from baskets into the mongo database.

        A supplement.json is created in baskets upon upload. This supplement
        is added to the Mongo database when invoking load_mongo_supplement.
        UUID, and basket_type from the index_table are also added to Mongo for
        referrence back to the datasource.

        Parameters
        ----------
        collection: str (default="supplement")
            Supplement will be added to the Mongo collection specified.
        """
        if not isinstance(collection, str):
            raise TypeError("Invalid datatype for supplement collection: "
                            "must be a string")

        for _, row in self.index_table.iterrows():
            basket = Basket(row["address"], file_system=self.file_system)
            supplement = basket.get_supplement()
            if supplement is None:
                continue
            manifest = basket.get_manifest()
            mongo_supplement = {}
            mongo_supplement["uuid"] = manifest["uuid"]
            mongo_supplement["basket_type"] = manifest["basket_type"]
            mongo_supplement.update(supplement)

            # If the UUID already has metadata loaded in MongoDB,
            # the metadata should not be loaded to MongoDB again.
            if 0 == self.database[
                collection
            ].count_documents({"uuid": manifest["uuid"]}):
                self.database[collection].insert_one(mongo_supplement)


    def load_mongo(self, **kwargs):
        """Load metadata, manifest, and supplement from baskets into the
        mongo database.

        Parameters
        ----------
        **metadata_collection: str (default="metadata")
            Metadata will be added to the Mongo collection specified.
        **manifest_collection: str (default="manifest")
            Manifest will be added to the Mongo collection specified.
        **supplement_collection: str (default="supplement")
            Supplement will be added to the Mongo collection specified.
        """
        metadata_collection = kwargs.get("metadata_collection", "metadata")
        manifest_collection = kwargs.get("manifest_collection", "manifest")
        supplement_collection = kwargs.get("supplement_collection",
                                           "supplement")

        self.load_mongo_metadata(collection=metadata_collection)
        self.load_mongo_manifest(collection=manifest_collection)
        self.load_mongo_supplement(collection=supplement_collection)
