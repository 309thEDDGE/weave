from setuptools import setup

setup(
    name="weave",
    version="0.1.0",
    packages=["weave", "weave/tests"],
    install_requires=["pandas", "s3fs", "fsspec", "pymongo"]
)
