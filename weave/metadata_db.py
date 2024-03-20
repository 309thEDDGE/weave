"""Contains scripts concerning MongoDB functionality."""

from .config import get_file_system
from .mongo_db import MongoDB


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
    **database: str (default="mongo_metadata")
    """
    file_system = kwargs.get("file_system", None)
    if file_system is None:
        file_system = get_file_system()

    database = kwargs.get("database", "mongo_metadata")

    mongo = MongoDB(
        index_table=index_table,
        database=database,
        file_system=file_system
    )

    mongo.load_mongo_metadata(collection=collection)
