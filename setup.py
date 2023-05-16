from setuptools import setup, find_namespace_packages

setup(
    name="weave",
    version="0.0.1",
    packages=["weave"],
    install_requires=["pandas", 
                      "s3fs",
                      "fsspec"],
)
