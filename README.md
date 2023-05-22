# Weave

Weave is a custom package used to facilitate the creation and maintenance of complex data storehouses.

Weave was created from a need to track the lineage of data products derived from multiple sources. 
Therefore, Weave can be used to upload arbitrary data products to a datastore with options to store metadata and 
information about how the data was derived. When Weave is used to upload the data, Weave can then be used to access
the data using pythonic API calls, as well as giving the user easy access to data provenance information.

## Usage

Weave can be installed by running `pip install .` from the root directory. Useful functions are imported from
`weave.access`, `weave.create_index`, and `weave.uploader`. Weave is built off an `fsspec` filesystem structure, so it
should be compatible with fsspec compatible filesystem objects. It was built with the intention of connecting to
an S3 bucket with an `s3fs.S3FileSystem` object. Currently, most functions require passing in a filesystem object
the user has created. This is in process of being handled by Weave so the user only needs to create the filesystem
object once.


#### Baskets 

Weave handles much of its data provenance tracking through the creation of Baskets. A Basket is meant to
represent an atomic data product. It can contain whatever a user wishes to put in the basket, but it's
intended purpose is to hold a single instance of one type of data, be it an image, video, text file, or curated
training set. A Basket in its entirety contains the actual data files specified by the user along with the supplemental
files that Weave creates. Baskets are created at their time of upload and uploaded in an organized state to 
the data store.

#### Creating and Uploading Baskets

#### Creating an Index

## Contribution

Anyone who desires to contribute to Weave is encouraged to create a branch or fork the repo, make the changes as they see fit,
and submit them for review to a member of 309th EDDGE.
