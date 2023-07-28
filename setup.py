from setuptools import setup

setup(
    name="weave",
    version="0.4.1",
    packages=["weave", "weave/tests"],
    install_requires=["pandas", "s3fs", "fsspec", "pymongo", "jsonschema"]
)
