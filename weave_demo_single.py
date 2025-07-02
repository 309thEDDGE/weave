# Weave Demo: Single Python File Example
# This script demonstrates how to use the Weave library for data management

# This block of code imports necessary libraries and modules for the Weave demo
# It includes imports for data manipulation with pandas, file system operations
# with fsspec, and various Weave components for indexing and validation.
# It also sets up the local file system for file operations.
import os

import pandas as pd
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 79)          # Narrower output for line length
pd.set_option('display.colheader_justify', 'center')  # Center column headers

from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
from weave.index.index_sqlite import IndexSQLite
from weave import validate

from fsspec.implementations.local import LocalFileSystem


# ----- Pantry Setup -----#
# A pantry is a storage location that holds baskets or collections of baskets.

# This sets up the location for the pantry, which in this example is the local
# file system.
local_fs = LocalFileSystem()
text_file = open("WeaveDemoText.txt", "w")
text_file.write("This is some text for the weave notebook demo.")
text_file.close()

# Below we create a pantry using the local file system within our working
# directory.
pantry_name = "weave-demo-pantry"
local_fs.mkdir(pantry_name)

# This creates an index for the pantry using the IndexPandas class, which is
# designed to work with pandas DataFrames.
pantry1 = Pantry(IndexPandas, pantry_path=pantry_name, file_system=local_fs)
index_df = pantry1.index.to_pandas_df()
print("Initial Pantry Index DataFrame:")
print("=" * 40)
print("Initial Pantry Index DataFrame:")
print(index_df.to_string(index=False))
print("=" * 40 + "\n")
print("\n")


# ---- Create and Upload Baskets------#
# A basket is a represenation of an atomic data product within a pantry.

# Here we create a basket with a single item, which is a text file.
# The basket is uploaded to the pantry with a specific type and metadata.
pantry1.upload_basket(
    upload_items=[{'path': 'WeaveDemoText.txt', 'stub': False}],
    basket_type="test-1",
    metadata={'Data Type': 'text'}
)

# Exporting the index to a pandas DataFrame for easier viewing and manipulation
# This DataFrame will contain information about the baskets in the pantry.
pantry1_df = pantry1.index.to_pandas_df()
print("Pantry Index DataFrame:")
print(pantry1_df.reset_index(drop=True))

# Having our pantry index catalog in a dataframe allows us to use pandas
# functionality to access information in the dataframe, like the UUID below.
print("\nUUID of the first basket in the pantry:")
print(pantry1_df['uuid'][0])

#-----Accessing Basket Data-----#
# Weave handles much of its data provenance tracking through the creation of
# basket. Each basket can be accessed using its UUID, which is a unique
# identifier for the basket.

# This retrieves a specific basket from the pantry using its UUID.
basket = pantry1.get_basket(pantry1_df['uuid'][0])
print("\nRetrieved Basket:", basket)

# The basket manifest contains a concise description of the basket in dict form
print("\nBasket Manifest:", basket.get_manifest())

# The basket supplement contains additional details about the basket, including
# integrity data. This is useful for verifying the integrity of the data within
# the basket.
print("\nBasket Supplement:", basket.get_supplement())

# The basket metadata is data the user may add when uploading a basket to a
# pantry.
print("\nBasket Metadata:", basket.get_metadata())

# Much like the Linux 'ls' command, Weave's ls lists files and directories
# within the file system.
basket_contents = basket.ls()
print("\nBasket Contents:")
print(basket_contents)
print("\n")

# ----- Data Provenance ----- #
# Data provenance in Weave is tracked through the relationships between baskets
# Each basket can have parent and child relationships, allowing for a clear
# lineage of data.

# This retrieves the parents of the basket using its UUID.
# Parents are baskets that were used to create the current basket.
pantry1.index.get_parents(pantry1_df['uuid'][0])

# Currently, the basket has no parents since it was created independently.
# We can upload another basket to create a parent-child relationship.

# This uploads a new basket with a text file and establishes a parent-child
# relationship. The new basket is a child of the first basket, as indicated by
# the parent_ids parameter.
pantry1.upload_basket(
    upload_items=[{'path': 'WeaveDemoText.txt', 'stub': False}],
    basket_type="test-2",
    parent_ids=[pantry1_df['uuid'][0]]
)

# Now the pantry index is updated to reflect the new basket and its
# relationship.
pantry1_df = pantry1.index.to_pandas_df()
print("Updated Pantry Index DataFrame with Parent-Child Relationship:")
print(pantry1_df.reset_index(drop=True))

# Using the .get_children() method, we can quickly retrieve the children of a
# specific basket.
child = pantry1.index.get_children(pantry1_df['uuid'][0])
print("\nChildren of the first basket:")
print(child)
print("\n")
# Same with the .get_parents() method, we can quickly retrieve the parents of a
# specific basket.
parent = pantry1.index.get_parents(pantry1_df['uuid'][1])
print("\nParents of the second basket:")
print(parent)
print("\n")

#-----Generating an index using the SQLite backend-----#
# When creating a pantry, you can choose different index backends.
# In this example, we create a new pantry using the SQLite index backend.
# This allows for more complex queries and better performance for larger
# datasets. The Weave Library is also compatible with Pandas and SQL.

# This creates a new pantry with the SQLite index backend.
pantry2 = Pantry(IndexSQLite, pantry_path=pantry_name, file_system=local_fs)
pantry2.index.generate_index()

# With the index created, we can take a look at the dataframe containing our
# baskets.
pantry2_df = pantry2.index.to_pandas_df()
print("\nPantry Index DataFrame with SQLite Backend:")
print(pantry2_df.reset_index(drop=True))

# -----Validating a Pantry------#
# Validation is an important step in ensuring the integrity and correctness of
# the data in a pantry. Weave provides a validation function that checks the
# integrity of the pantry and its baskets.

# This validates the pantry using the validate_pantry function.
warnings = validate.validate_pantry(pantry1)
print("\nValidation Warnings:")
print(warnings)
# Or validate using the pantry object.
print(pantry1.validate())

# Since the basket data is present we return an empty list.
# If the basket data was missing or corrupted, the warnings list would contain
# details about the issues found.

# Deleting the basket manifest file to simulate a validation error.
local_fs.rm(
    os.path.join(
        'weave-demo-pantry', 'test-1',
        str(pantry1_df['uuid'][0]), 'basket_manifest.json'
    )
)
print(pantry1.validate())

#-------Clean up and Remove-------#
# After completing the demo, it's important to clean up and remove the pantry
# and its contents. This ensures that no unnecessary files or data are left
# behind.
local_fs.rm("weave-demo-pantry", recursive=True)
pantry2.index.drop_index()
os.remove("WeaveDemoText.txt")


