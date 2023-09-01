"""
Weave
=====

Weave can be used to facilitate the creation, maintenance, and access of
coordinated, complex data storage.

Weave is used to create Baskets. A Basket is used to store an atomic unit (i.e.
the smallest sensible unit) of data, as well as it's associated metadata,
including lineage.

--> Note that there is a Basket class available in Weave. Use it to access
individual Baskets.

A collection of Baskets is known as a Pantry. A Pantry can be accessed using
it's Index.

--> Note that you can use the Index class to access a Pantry's Index. You can
also upload new Baskets to the Pantry using Index.upload_basket().
"""

from .basket import Basket
from .index.index import Index
from .metadata_db import load_mongo

__version__ = "0.12.10"

__all__ = [
    "Basket",
    "Index",
    "load_mongo",
]
