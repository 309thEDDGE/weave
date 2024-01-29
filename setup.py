import os
from setuptools import setup

def read(rel_path: str) -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    # intentionally *not* adding an encoding option to open.
    # See: https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with open(os.path.join(here, rel_path)) as file_path:
        return file_path.read()


def get_version(rel_path: str) -> str:
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="weave-db",
    version=get_version("weave/__init__.py"),
    description="Library to facilitate the creation and maintenance of " +
                "complex data warehouses.",
    packages=["weave", "weave/tests", "weave/index"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/309thEDDGE/weave",
    author="309thEDDGE",
    license="GNU General Public",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    install_requires=["pandas", "s3fs", "fsspec", "jsonschema"],
    extras_require={
        "extras": ["pymongo", "psycopg2-binary", "sqlalchemy"],
    },
    python_requires=">=3.10",
)
