from setuptools import setup

setup(
    name="weave",
    version="0.4.0-1086",
    packages=["weave", "weave/tests"],
    install_requires=["pandas", "s3fs", "fsspec", "pymongo"]
)
