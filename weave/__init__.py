"""
Weave
=====

Weave can be used to facilitate the creation, maintenance, and access of
coordinated, complex data storage.

Weave is used to create baskets. A basket is used to store an atomic unit (i.e.
the smallest sensible unit) of data, as well as it's associated metadata,
including lineage.

--> Note that there is a Basket class available in Weave. Use it to access
individual baskets.

A collection of baskets is known as a pantry. A pantry can be accessed using
its Index.

--> Note that you can use the Index class to access a pantry's Index. You can
also upload new baskets to the pantry using Index.upload_basket().
"""

from .basket import Basket
from .index.index import Index
from .metadata_db import load_mongo

__version__ = "0.10.11"

__all__ = [
    "Basket",
    "Index",
    "load_mongo",
]
