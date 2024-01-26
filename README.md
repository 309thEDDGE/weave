<h1 align="center">
<img src="./weave_transparent.png" width="300">
</h1>

# Weave

Weave is a custom package used to facilitate the creation and maintenance of
complex data warehouses.

Weave was created from a need to track the lineage of data products derived
from multiple sources. Weave can be used to upload arbitrary data products to a
datastore with options to store metadata and information about how data were
derived. When Weave is used to upload the data, Weave can then be used to
access the data using Pythonic API calls, as well as giving the user easy
access to data provenance.

## Weave Vocabulary

**Bucket**: A location in MINIO where files may be stored.

**Pantry**: A storage location that holds baskets or collections of baskets.

**Basket**: A representation of an atomic data product within a pantry.

**Index**: An object or file that tracks the baskets in a pantry.

**Manifest**: A concise definition of a basket for representation in the index.

**Supplement**: Extended details of basket contents, including integrity data.

**Metadata**: Additional data the user may add when uploading a basket to a
pantry.

## Weave Artifacts

When Weave uploads a basket, three additional files are created.

### Manifest

The manifest contains a concise description of the basket following the schema
found in weave/config.py. It contains the following:

- UUID: Unique identifier.
- Upload Time: ISO 8601 timestamp for when the basket was uploaded.
- Parent UUIDS: Basket(s) that created the current basket.
- Basket Type: The type of basket.
- Label: An additional optional label for the basket.

### Supplement

This file follows the schema found in weave/config.py as follows:

- Upload Items: The items uploaded within the basket.
- Integrity data: A data to verify data was successfully uploaded for each
                    file.
    - File Size: Total size of the file in bytes.
    - Hash: SHA-256 hash checksum for the file.
    - Access Date: Date the basket was uploaded.
    - Source Path: Input path of the uploaded file.
    - Byte Count: A threshold value to accelerate checksum computation for
                    large files.
    - Upload Path: Location in the file system where the file was uploaded.

### Metadata

Users may supply an optional metadata argument to provide custom metadata for
the uploaded files. The metadata must be in the form of a dictionary and is
saved in the basket as a .json file.

## Usage

Weave can be installed by running `pip install .` from the root directory.
Optional dependencies can be included by running `pip install .[extras]` instead.
Optional dependencies currently include: pymongo, psycopg2, sqlalchemy.
Useful functions are available after running `import weave`.
Weave was built with the intention of connecting to an S3 pantry with an
`s3fs.S3FileSystem` object and also supports a LocalFileSystem. Any filesystem
that uses an `fsspec.implementations` API should be possible to implement. For
now, Weave has only been tested using S3 and local filesystems.

The following environment variables are required to establish an S3 connection:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- S3_ENDPOINT

If pymongo is intended to be used, the following environment variables are
required to establish a MongoClient connection:
- MONGODB_HOST
- MONGODB_USERNAME
- MONGODB_PASSWORD

If the IndexSQL backend is intended to be used, the following environment
variables are required to establish a Postgres SQL Connection:
- WEAVE_SQL_HOST
- WEAVE_SQL_USERNAME
- WEAVE_SQL_PASSWORD
- WEAVE_SQL_DB_NAME (postgres, defaults to weave_db)
- WEAVE_SQL_PORT (optional, defaults to 5432)

### Initializing FileSystem

The default file system for weave is s3fs. However, a custom s3fs connection or
a local file system may readily be used. These file systems can then be passed
to the Basket, Index, or UploadBasket classes.

```python
from fsspec.implementations.local import LocalFileSystem
import s3fs

s3_fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
local_fs = LocalFileSystem()

Index(args, file_system=s3_fs)
Basket(args, file_system=local_fs)
UploadBasket(args) # Default s3fs used
```

The default pantry name for Weave classes is "weave-test". A pantry can be 
named any valid fsspec directory name. This can be done as follows:

```python
pantry_name = "weave-test"
s3_fs.mkdir(path_to_pantry + os.path.sep + pantry_name)
local_fs.mkdir(path_to_pantry + os.path.sep + pantry_name)
```

### Baskets

Weave handles much of its data provenance tracking through the creation of
baskets. A basket is meant to represent an atomic data product. It can contain
whatever a user wishes to put in the basket, but it's intended purpose is to
hold a single instance of one type of data, be it an image, video, text file,
or curated training set. A basket in its entirety contains the actual data
files specified by the user along with the supplemental files that Weave
creates. These supplemental files contain data integrity information, arbitrary
metadata specified by the user, and lineage artifacts. Baskets are created at
their time of upload and uploaded in an organized state to the data store.

#### Creating and Uploading Baskets

Weave automatically creates baskets during the upload process. However, the
user must specify what information they want contained in the basket.

Required basket information:
- upload_items: List of dictionaries of items to upload.
    - path: Path of the file on the local system.
    - stub: Boolean to indicate whether the basket includes a copy or reference
            to the file. True indicates a reference is uploaded.
- basket_type: A category for the basket.
- pantry_name/upload_directory: Where to upload the files.

Optional basket information:
- source_file_system: file system where weave will get the file to upload.
- parent_ids: Baskets from which the current basket was derived.
- metadata: User customizable metadata.
- label: Additional user label.

The preferred method to upload baskets is using the Index. However, baskets
can be uploaded directly:

```python
from weave.upload import UploadBasket
upload_items = [{"path":"Path_to_file_or_dir", "stub": False}]
upload_path = UploadBasket(upload_items,
                           basket_type="item",
                           upload_directory="weave-test")
```

A Basket can also be uploaded as a `metadata-only` basket. This is used to add
more metadata to a previously existing basket. There are three requirements to
upload a metadata-only basket: No `upload_items`, include `metadata`, and
include `parent_ids`.
```python
upload_path = UploadBasket(upload_items=[],
                           basket_type="item",
                           upload_directory="weave-test",
                           metadata={"test":"metadata"},
                           parent_ids=["existing_parent_UUID"])
```

Running `help(weave.upload)` will print the docstring that provides more
information on each of these upload parameters.

#### Basket Information

The basket information can readily be accessed by creating a Basket object:

```python
basket = Basket(basket_address, pantry=Optional)
basket.get_manifest()
basket.get_supplement()
basket.get_metadata()
basket_contents = basket.ls()
```

#### Using Basket to access files

Basket can readily list and access files within the file system using
basket.ls(). Once the the ls() retrieves the avaialable files, they are used
like any file path. The following example loads a csv using Basket:

```python
s3 = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
)
basket_contents = [pantry_name/basket_type/uuid/data.csv]
df = pd.read_csv(s3.open(basket_contents[0], mode="rb"))
```

### Using a Pantry

The Pantry class facilitates interaction with the file system including upload,
access, and delete baskets. The pantry can also track pantry-level metadata.
To enhance these functions, the pantry has an abstract base class of an index
that tracks whenever baskets are added and removed from the file system. This
index provides information about each basket, including its uuid, upload time,
parent uuids, basket type, label, address and storage type. An index is created
by passing an Index object as the first argument to the Pantry constructor.

Weave supports a Pandas, SQLite, and Postgres SQL implementation for the index backend.
Example code to create this index:
```python
from weave.pantry import Pantry
from weave.index.index_pandas import IndexPandas
pantry = Pantry(
    IndexPandas,
    pantry_path,
    file_system=file_system,
)
index_df = pantry.index.to_pandas_df()
```
`Index.to_pandas_df()` returns a pandas dataframe with each row corresponding
to a basket in the datastore. The columns in the dataframe follow the manifest
schema for the basket. An example basket entry is shown below:

| uuid | upload_time | parent_uuids | basket_type | label | address | storage_type |
| ---- | ----------- | ------------ | ----------- | ----- | ------- | ------------ |
| fe42575a41c711eeb2210242ac1a000a | 2023-08-23T15:16:11.546136 | [] | item |  | example_address/item/fe4257... | S3FileSystem |


The Pantry class also provides convenient functions for uploading, accessing,
and deleting baskets.

```python
# Get pantry metadata
pantry_metadata = pantry.metadata

# Upload a basket using the Index.
upload_items = [{"path":"Path_to_file_or_dir", "stub": False}]
uploaded_info = pantry.upload_basket(upload_items,
                                     basket_type="item",
                                     parent_ids=Optional,
                                     metadata=Optional,
                                     label=Optional)

# Access the uploaded_basket (likely called well after uploading the basket).
basket = pantry.get_basket(uploaded_info.uuid[0])
basket_path = uploaded_info.upload_path[0]

# Access the parents and children
basket_parents = pantry.index.get_parents(uploaded_info.uuid[0])
basket_children = pantry.index.get_children(uploaded_info.uuid[0])

# Delete the basket
pantry.delete_basket(uploaded_info.uuid[0])
```

### Validating a Pantry

Weave can validate an existing directory is a valid pantry following the Weave
schema:

```python
from weave import validate
warnings = validate.validate_pantry(pantry)
# Or validate using the pantry object.
pantry.validate()
```

## Contribution

Anyone who desires to contribute to Weave is encouraged to create a branch,
make the changes as they see fit, and submit them for review to a member of
309th EDDGE. Make sure contributions follow proper test driven development
practices and PEP-8 style guidelines. The contribution guide can be found
<a href="https://github.com/309thEDDGE/weave/blob/main/CONTRIBUTING.md">here</a>.
