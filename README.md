# Weave

Weave is a custom package used to facilitate the creation and maintenance of
complex data warehouse.

Weave was created from a need to track the lineage of data products derived
from multiple sources. Weave can be used to upload arbitrary data products to a
datastore with options to store metadata and information about how data were
derived. When Weave is used to upload the data, Weave can then be used to
access the data using Pythonic API calls, as well as giving the user easy
access to data provenance.

## Usage

Weave can be installed by running `pip install .` from the root directory.
Useful functions are imported from `weave.access` and `weave.create_index`.
Weave was built with the intention of connecting to an S3 bucket with an
`s3fs.S3FileSystem` object. Any filesystem that uses an
`fsspec.implementations` API should be possible to implement. For now, Weave
has only been tested using an S3 bucket filesystem.

### Baskets 

Weave handles much of its data provenance tracking through the creation of
Baskets. A Basket is meant to represent an atomic data product. It can contain
whatever a user wishes to put in the basket, but it's intended purpose is to
hold a single instance of one type of data, be it an image, video, text file,
or curated training set. A Basket in its entirety contains the actual data
files specified by the user along with the supplemental files that Weave
creates. These supplemental files contain data integrity information, arbitrary
metadata specified by the user, and lineage artifacts. Baskets are created at
their time of upload and uploaded in an organized state to the data store.

### Creating and Uploading Baskets

Weave automatically creates baskets during the upload process. However, the
user must specify what information they want contained in the basket.

Required basket information:
- upload items
- basket type
- bucket name

Optional basket information:
- parent ids
- metadata
- label

Example code to upload a basket:

```python
from weave.access import upload
upload_items = [{'path':'Path_to_file_or_dir', 'stub': False}]
upload(upload_items, basket_type = 'item', bucket_name = 'basket-data')
```

Running `help(weave.access.upload)` will print the docstring that provides more
information on each of these upload parameters.

### Creating an Index

Weave can scrape a datastore of baskets and create an index of all the baskets
in that datastore. This index provides information about each basket, including
its uuid, upload time, parent uuids, basket type, label, address and storage
type. Example code to create this index:
```python
from weave.create_index import create_index_from_s3
index = create_index_from_s3(name_of_s3_bucket)
```
`create_index_from_s3` returns a pandas dataframe with each row corresponding
to a basketin the datastore.

## Contribution

Anyone who desires to contribute to Weave is encouraged to create a branch,
make the changes as they see fit, and submit them for review to a member of
309th EDDGE. Make sure contributions follow proper test driven development
practices and PEP 8 style guidelines.