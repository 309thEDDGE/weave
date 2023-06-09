import pandas as pd
from weave import config, Basket

def load_mongo(index_table):
    """Load metadata from baskets into the mongo database.

       A metadata.json is created in Baskets when the metadata
       field is provided upon upload. This metadata is added to the
       mongo database when invoking load_mongo. UUID, and basket_type
       from the index_table are also added to mongo for referrence
       back to the datasource.

        Parameters
        ----------
        index_table: [Pandas Dataframe]
            Weave index object. This object can be fetched using index.py.
            The dataframe must have the following fields.
               uuid
               basket_type
               address
        """
    
    if not isinstance(index_table, pd.DataFrame):
        raise TypeError("Invalid datatype for index_table: "
                        "must be Pandas DataFrame")
    
    required_columns = ['uuid', 'basket_type', 'address']
    
    for required_column in required_columns:
        if required_column not in index_table.columns.values.tolist():
            raise ValueError("Invalid index_table: missing "
                             f"{required_column} column")
    
    db = config.get_mongo_db().mongo_metadata
    
    for index, row in index_table.iterrows():
        basket = Basket(row['address'])
        metadata = basket.get_metadata()
        if metadata is None:
            continue
        manifest = basket.get_manifest()
        mongo_metadata = {}
        mongo_metadata['uuid'] = manifest['uuid']
        mongo_metadata['basket_type'] = manifest['basket_type']
        mongo_metadata.update(metadata)
        db['metadata'].insert_one(mongo_metadata)
