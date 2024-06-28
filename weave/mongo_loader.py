"""Contains scripts concerning Mongo Loader functionality."""

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
from .config import get_mongo_db


class MongoLoader():
    """Initializes a mongo loader class. Retrieves a connection to the mongo
    server and facilitates the uploading of records to each pantry's individual
    mongo db based on the record type (ie supplement, manifest, metadata).
    """

    def __init__(self, pantry):
        """Creates the mongo loader and makes a reference to the pantry's DB.

        Parameters
        ----------
        pantry: weave.Pantry
            A pantry object
        """
        if not _HAS_PYMONGO:
            raise ImportError("Missing Dependency. The package 'pymongo' "
                              "is required to use this class.")

        self.pantry = pantry
        self.database = get_mongo_db()[self.pantry.pantry_path]


    def load_mongo_metadata(self, uuids, collection="metadata"):
        """Load metadata from baskets into the mongo database.

        A metadata.json is created in baskets when the metadata
        field is provided upon upload. This metadata is added to the
        Mongo database when invoking load_mongo_metadata. UUID and basket_type
        from the index_table are also added to Mongo for referrence
        back to the datasource.

        Parameters
        ----------
        uuids: [str]
            A list of uuids to add their metadata to the mongo db.
        collection: str (default="metadata")
            Metadata will be added to the Mongo collection specified.
        """
        if not isinstance(uuids, list):
            uuids = [uuids]
        if not isinstance(uuids[0], str):
            raise TypeError("Invalid datatype for uuids: "
                            "must be a list of strings [str]")
        if not isinstance(collection, str):
            raise TypeError("Invalid datatype for metadata collection: "
                            "must be a string")

        for uuid in uuids:
            basket = Basket(uuid, pantry=self.pantry)
            metadata = basket.get_metadata()
            if not metadata:
                continue
            mongo_metadata = {}
            mongo_metadata["uuid"] = basket.uuid
            mongo_metadata["basket_type"] = basket.basket_type
            mongo_metadata.update(metadata)

            # If the UUID already has metadata loaded in MongoDB,
            # the metadata should not be loaded to MongoDB again.
            if 0 == self.database[
                collection
            ].count_documents({"uuid": basket.uuid}):
                self.database[collection].insert_one(mongo_metadata)

    def load_mongo_manifest(self, uuids, collection="manifest"):
        """Load manifest from baskets into the mongo database.

        A manifest.json is created in baskets upon upload. This manifest is
        added to the Mongo database when invoking load_mongo_manifest. UUID,
        and basket_type from the index_table are also added to Mongo for
        referrence back to the datasource.

        Parameters
        ----------
        uuids: [str]
            A list of uuids to add their manifests to the mongo db.
        collection: str (default="manifest")
            Manifest will be added to the Mongo collection specified.
        """
        if not isinstance(uuids, list):
            uuids = [uuids]
        if not isinstance(collection, str):
            raise TypeError("Invalid datatype for manifest collection: "
                            "must be a string")

        for uuid in uuids:
            basket = Basket(uuid, pantry=self.pantry)
            mongo_manifest = basket.get_manifest()
            if not mongo_manifest:
                continue

            # If the UUID already has the manifest loaded in MongoDB,
            # the manifest should not be loaded to MongoDB again.
            if 0 == self.database[
                collection
            ].count_documents({"uuid": basket.uuid}):
                self.database[collection].insert_one(mongo_manifest)

    def load_mongo_supplement(self, uuids, collection="supplement"):
        """Load supplement from baskets into the mongo database.

        A supplement.json is created in baskets upon upload. This supplement
        is added to the Mongo database when invoking load_mongo_supplement.
        UUID, and basket_type from the index_table are also added to Mongo for
        referrence back to the datasource.

        Parameters
        ----------
        uuids: [str]
            A list of uuids to add their supplement to the mongo db.
        collection: str (default="supplement")
            Supplement will be added to the Mongo collection specified.
        """
        if not isinstance(uuids, list):
            uuids = [uuids]
        if not isinstance(collection, str):
            raise TypeError("Invalid datatype for supplement collection: "
                            "must be a string")

        for uuid in uuids:
            basket = Basket(uuid, pantry=self.pantry)
            supplement = basket.get_supplement()
            if not supplement:
                continue
            mongo_supplement = {}
            mongo_supplement["uuid"] = basket.uuid
            mongo_supplement["basket_type"] = basket.basket_type
            mongo_supplement.update(supplement)

            # If the UUID already has metadata loaded in MongoDB,
            # the metadata should not be loaded to MongoDB again.
            if 0 == self.database[
                collection
            ].count_documents({"uuid": basket.uuid}):
                self.database[collection].insert_one(mongo_supplement)

    def load_mongo(self, uuids, **kwargs):
        """Load metadata, manifest, and supplement from baskets into the
        mongo database.

        Parameters
        ----------
        uuids: [str]
            A list of uuids to add their data to the mongo db.
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

        self.load_mongo_metadata(uuids, collection=metadata_collection)
        self.load_mongo_manifest(uuids, collection=manifest_collection)
        self.load_mongo_supplement(uuids, collection=supplement_collection)


    def remove_document (self, uuid : str, **kwargs):
        """Delete a document using the uuid in the collections.

        Parameters
        ----------
        uuid: str
            "uuid" will be used to locate and remove the document from MongoDB
            in the supplement, manifest, and metadata collections.
        pantry: Pantry
            The Pantry of interest.
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

        collection_names = (metadata_collection,
                            manifest_collection,
                            supplement_collection)

        for e in collection_names:
            self.database[e].delete_one({'uuid':uuid})
