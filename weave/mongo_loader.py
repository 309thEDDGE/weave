"""Contains scripts concerning Mongo Loader functionality."""

# Ignore pylint duplicate code. Code here is used to explicitly show pymongo is
# an optional dependency. Duplicate code is found in config.py (where pymongo
# is actually imported)
# pylint: disable-next=duplicate-code
try:
    import pymongo
except ImportError:
    _HAS_PYMONGO = False
else:
    _HAS_PYMONGO = True

from .basket import Basket
from .config import get_mongo_db

# pylint: disable-next=too-many-instance-attributes
class MongoLoader():
    """Initializes a mongo loader class. Retrieves a connection to the mongo
    server and facilitates the uploading of records to each pantry's individual
    mongo db based on the record type (ie supplement, manifest, metadata).
    """

    def __init__(self, pantry, mongo_client=None, **kwargs):
        """Creates the mongo loader and makes a reference to the pantry's DB.

        Parameters
        ----------
        pantry: weave.Pantry
            A pantry object
        mongo_client: pymongo.MongoClient or None (optional; defaults to None)
            A pre-constructed MongoClient to be used. Ignored if the Pantry arg
            has a non-None pantry.mongo_client. Otherwise, this arg will be
            used for the client. If none, is provided, a MongoClient will be
            constructed using the optional mongo_config dictionary,
            or weave.config.get_mongo_db() as a last resort.
        **mongo_config: dict (optional)
            Dictionary containing the configuration settings of this loader.
            Supported Keys:
        """
        if not _HAS_PYMONGO:
            raise ImportError("Missing Dependency. The package 'pymongo' "
                              "is required to use this class.")

        self.pantry = pantry
        self.mongo_config = kwargs.get("mongo_config", {})

        # Priorize using the pantry client, otherwise use the given client.
        self.mongo_client = self.pantry.mongo_client or mongo_client
        # If both were None, try to make a client using the mongo_config.
        if self.mongo_client is None and self.mongo_config:
            self.mongo_client = pymongo.MongoClient(
                host=self.mongo_config["mongodb_host"],
                username=self.mongo_config["mongodb_username"],
                password=self.mongo_config["mongodb_password"],
                port=self.mongo_config.get("mongodb_port", 27017),
                timeoutMS=self.mongo_config.get("mongodb_timeout", None)
            )
            self.mongo_client.server_info()
        # If we still don't have a valid mongo_client, use the weave config as
        # a last resort.
        if self.mongo_client is None:
            timeout = self.mongo_config.get("mongodb_timeout", None)
            self.mongo_client = get_mongo_db(timeout=timeout)

        # Get the database. (Use MONGODB_DATABASE, defaulting to
        # pantry_path if it is not present.)
        self.database_name = self.mongo_config.get(
            "mongodb_database", self.pantry.pantry_path)
        self.database = self.mongo_client[self.database_name]

        self.metadata_collection = self.mongo_config.get(
            "metadata_collection", "metadata")
        self.manifest_collection = self.mongo_config.get(
            "manifest_collection", "manifest")
        self.supplement_collection = self.mongo_config.get(
            "supplement_collection", "supplement")


    def load_mongo_metadata(self, uuids, **kwargs):
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
        **collection: str (optional; defaults to self.metadata_collection)
            Metadata will be added to the Mongo collection specified. If not
            provided, populate using self.metadata_collection (which defaults
            to "metadata" if otherwise unspecified).
        """
        collection = kwargs.get("collection", self.metadata_collection)
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
                self.metadata_collection
            ].count_documents({"uuid": basket.uuid}):
                self.database[collection].insert_one(mongo_metadata)

    def load_mongo_manifest(self, uuids, **kwargs):
        """Load manifest from baskets into the mongo database.

        A manifest.json is created in baskets upon upload. This manifest is
        added to the Mongo database when invoking load_mongo_manifest. UUID,
        and basket_type from the index_table are also added to Mongo for
        referrence back to the datasource.

        Parameters
        ----------
        uuids: [str]
            A list of uuids to add their manifests to the mongo db.
        **collection: str (optional; defaults to self.manifest_collection)
            Manifests will be added to the Mongo collection specified. If not
            provided, populate using self.manifest_collection (which defaults
            to "manifest" if otherwise unspecified).
        """
        collection = kwargs.get("collection", self.manifest_collection)
        if not isinstance(uuids, list):
            uuids = [uuids]
        if not isinstance(uuids[0], str):
            raise TypeError("Invalid datatype for uuids: "
                            "must be a list of strings [str]")
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

    def load_mongo_supplement(self, uuids, **kwargs):
        """Load supplement from baskets into the mongo database.

        A supplement.json is created in baskets upon upload. This supplement
        is added to the Mongo database when invoking load_mongo_supplement.
        UUID, and basket_type from the index_table are also added to Mongo for
        referrence back to the datasource.

        Parameters
        ----------
        uuids: [str]
            A list of uuids to add their supplement to the mongo db.
        **collection: str (optional; defaults to self.supplement_collection)
            Supplements will be added to the Mongo collection specified. If not
            provided, populate using self.supplement_collection (which defaults
            to "supplement" if otherwise unspecified).
        """
        collection = kwargs.get("collection", self.supplement_collection)
        if not isinstance(uuids, list):
            uuids = [uuids]
        if not isinstance(uuids[0], str):
            raise TypeError("Invalid datatype for uuids: "
                            "must be a list of strings [str]")
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
        **metadata_collection: str (default=self.metadata_collection)
            Metadata will be added to the Mongo collection specified.
        **manifest_collection: str (default=self.manifest_collection)
            Manifest will be added to the Mongo collection specified.
        **supplement_collection: str (default=self.supplement_collection)
            Supplement will be added to the Mongo collection specified.
        """
        metadata_collection = kwargs.get("metadata_collection",
                                         self.metadata_collection)
        manifest_collection = kwargs.get("manifest_collection",
                                         self.manifest_collection)
        supplement_collection = kwargs.get("supplement_collection",
                                           self.supplement_collection)

        self.load_mongo_metadata(uuids, collection=metadata_collection)
        self.load_mongo_manifest(uuids, collection=manifest_collection)
        self.load_mongo_supplement(uuids, collection=supplement_collection)

    def clear_mongo(self, refresh=False):
        """Clear the metadata, manifest, and supplement collections optionally,
        refreshing them from the pantry.

        Parameters
        ----------
        refresh: bool (default=False)
            If True, reload the collections with data retreived from the pantry
        """
        # Drop the referenced database.
        self.database.client.drop_database(self.database_name)

        # Optionally refresh the mongo collections with the current index.
        if refresh:
            self.load_mongo(
                self.pantry.index.to_pandas_df(max_rows=None)['uuid'].to_list()
            )

    def remove_document(self, uuid, **kwargs):
        """Delete all documents containing the given uuid from all collections.

        Parameters
        ----------
        uuid: str
            "uuid" will be used to locate and remove the documents from MongoDB
            in the supplement, manifest, and metadata collections.
        **metadata_collection: str (default=self.metadata_collection)
            Metadata will be added to the Mongo collection specified.
        **manifest_collection: str (default=self.manifest_collection)
            Manifest will be added to the Mongo collection specified.
        **supplement_collection: str (default=self.supplement_collection)
            Supplement will be added to the Mongo collection specified.
        """
        metadata_collection = kwargs.get("metadata_collection",
                                         self.metadata_collection)
        manifest_collection = kwargs.get("manifest_collection",
                                         self.manifest_collection)
        supplement_collection = kwargs.get("supplement_collection",
                                           self.supplement_collection)

        collection_names = (metadata_collection,
                            manifest_collection,
                            supplement_collection)

        for collection in collection_names:
            if not isinstance(collection, str):
                raise TypeError("Invalid datatype for collection: "
                                "must be a string")

        for collection in collection_names:
            self.database[collection].delete_many({'uuid':uuid})
