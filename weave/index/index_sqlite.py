"""Wherein is contained the concrete SQLite implementation of the Index."""
import json
import os
import warnings

import datetime
import sqlite3
import pandas as pd

from weave import Basket
from ..config import index_schema
from .index_abc import IndexABC
from .list_baskets import _get_list_of_basket_jsons
from .validate_basket import validate_basket_dict


class IndexSQLite(IndexABC):
    """Concrete implementation of Index, using SQLite."""
    def __init__(self, file_system, pantry_path, **kwargs):
        """Initializes the Index class.

        Parameters
        ----------
        file_system: fsspec object
            The fsspec object which hosts the pantry we desire to index.
        pantry_path: str
            Path to the pantry root which we want to index.
        **db_path: str
            Path to the sqlite db file to be used. If none is set, defaults to
            'basket-data.db'
        """
        self._file_system = file_system
        self._pantry_path = pantry_path

        self.db_path = kwargs.get("db_path", "basket-data.db")
        self.con = sqlite3.connect(self.db_path)
        self.cur = self.con.cursor()
        self.connect_db()

    def connect_db(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS pantry_index(
                uuid TEXT, upload_time TEXT, parent_uuids TEXT,
                basket_type TEXT, label TEXT, address TEXT, storage_type TEXT,
                PRIMARY KEY(uuid), UNIQUE(uuid));
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS parent_uuids(
                uuid TEXT, parent_uuid TEXT,
                PRIMARY KEY(uuid, parent_uuid), UNIQUE(uuid, parent_uuid));
        """)
        self.con.commit()

    @property
    def file_system(self):
        """The file system of the pantry referenced by this Index."""
        return self._file_system

    @property
    def pantry_path(self):
        """The pantry path referenced by this Index."""
        return self._pantry_path

    def generate_metadata(self, **kwargs):
        """Populates the metadata for the index."""
        return {"db_path": self.db_path}

    def generate_index(self, **kwargs):
        """Populates the index from the file system.

        Generate the index by scraping the pantry and adding the manifest data
        of found baskets to the index.
        """
        if not isinstance(self.pantry_path, str):
            raise TypeError("'pantry_path' must be a string: "
                            f"'{self.pantry_path}'")

        if not self.file_system.exists(self.pantry_path):
            raise FileNotFoundError("'pantry_path' does not exist: "
                                    f"'{self.pantry_path}'")

        basket_jsons = _get_list_of_basket_jsons(self.pantry_path,
                                                 self.file_system)
        schema = index_schema()
        index_dict = {}

        storage_type = self.file_system.__class__.__name__

        bad_baskets = []
        for basket_json_address in basket_jsons:
            with self.file_system.open(basket_json_address, "rb") as file:
                basket_dict = json.load(file)

                # If the basket is invalid, add it to a list, then skip it.
                if not validate_basket_dict(basket_dict):
                    bad_baskets.append(os.path.dirname(basket_json_address))
                    continue
                # Skip baskets that are indexes.
                if basket_dict["basket_type"] == "index":
                    continue

                absolute_path = os.path.dirname(basket_json_address)
                relative_path = (
                    absolute_path[absolute_path.find(self.pantry_path):]
                )
                basket_dict["address"] = relative_path
                basket_dict["storage_type"] = storage_type

                # parent_uuids = basket_dict.pop("parent_uuids")
                parent_uuids = basket_dict["parent_uuids"]
                basket_dict["parent_uuids"] = str(basket_dict["parent_uuids"])

                sql = """INSERT OR IGNORE INTO pantry_index(
                uuid, upload_time, parent_uuids, basket_type,
                label, address, storage_type) VALUES(?,?,?,?,?,?,?) """

                val = tuple(basket_dict.values())

                self.cur.execute(sql, val)

                sql = """INSERT OR IGNORE INTO parent_uuids(
                uuid, parent_uuid) VALUES(?,?)"""
                for parent_uuid in parent_uuids:
                    self.cur.execute(sql, (basket_dict['uuid'], parent_uuid))

        if len(bad_baskets) != 0:
            warnings.warn('baskets found in the following locations '
                          'do not follow specified weave schema:\n'
                          f'{bad_baskets}')

        self.con.commit()

    def to_pandas_df(self, max_rows=1000, **kwargs):
        """Returns the pandas dataframe representation of the index.

        Parameters
        ----------
        max_rows: int
            Max rows returned in the pandas dataframe.

        Returns
        ----------
        pandas.DataFrame
            Returns a dataframe of the manifest data of the baskets in the
            pantry.
        """
        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        ind_df = pd.DataFrame(
            self.cur.execute("SELECT * FROM pantry_index LIMIT ?", (max_rows,))
            .fetchall(),
            columns=columns
        )
        return ind_df

    def track_basket(self, entry_df, **kwargs):
        """Track a basket (or many baskets) from the pantry with the Index.

        Parameters
        ----------
        entry_df : pd.DataFrame
            Uploaded baskets' manifest data to append to the index.
        """
        entry_df["parent_uuids"] = entry_df["parent_uuids"].astype(str)
        entry_df.to_sql("pantry_index", self.con,
                        if_exists="append", method="multi", index=False)


    def untrack_basket(self, basket_address, **kwargs):
        """Remove a basket from being tracked of given UUID or path.

        Parameters
        ----------
        basket_address: str or [str]
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. These may also be passed in
            as a list.
        """
        if not isinstance(basket_address, list):
            basket_address = [basket_address]

        if self.file_system.exists(os.fspath(basket_address[0])):
            id_column = "address"
        else:
            id_column = "uuid"

        query = (
            f"DELETE FROM pantry_index WHERE {id_column} in "
            f"({','.join(['?']*len(basket_address))})"
        )
        self.cur.execute(query, basket_address)
        if self.cur.rowcount != len(basket_address):
            warnings.warn(
                UserWarning(
                    "Incomplete Request. Index could not untrack baskets, "
                    "as some were not being tracked to begin with.",
                    len(basket_address) - self.cur.rowcount
                )
            )

        self.con.commit()


    def get_basket(self, basket_address, **kwargs):
        """Returns a Basket of given UUID or path.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.

        Returns
        ----------
        Basket
            Returns the Basket object.
        """
        # Create a Basket from the given address, and the index's file_system
        # and bucket name. Basket will catch invalid inputs and raise
        # appropriate errors.

        # Get the path of the basket, before we pass it into Basket.
        if not self.file_system.exists(os.fspath(basket_address)):
            # Get the path from the assumed uuid.
            query_result = self.cur.execute(
                "SELECT address FROM pantry_index WHERE uuid = ?",
                (basket_address,)
            ).fetchone()

            # If no result is returned, the uuid didn't exist: raise an error.
            if query_result is None:
                raise ValueError(f"Basket does not exist: {basket_address}")
            basket_path = query_result[0]

        # Otherwise the basket_address is a path that exists, use it as the path.
        else:
            basket_path = basket_address

        return Basket(
            basket_address=basket_path,
            file_system=self.file_system
        )

    def get_rows(self, basket_address, **kwargs):
        """Returns a pd.DataFrame row information of given UUID or path.

        Parameters
        ----------
        basket_address: str or [str]
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. These may also be passed in
            as a list.

        Returns
        ----------
        pandas.DataFrame
            Manifest information for the requested basket(s).
        """
        if not isinstance(basket_address, list):
            basket_address = [basket_address]

        if self.file_system.exists(os.fspath(basket_address[0])):
            id_column = "address"
        else:
            id_column = "uuid"

        query = (
            f"SELECT * FROM pantry_index WHERE {id_column} in "
            f"({','.join(['?']*len(basket_address))})"
        )
        results = self.cur.execute(query, basket_address).fetchall()

        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        ind_df = pd.DataFrame(
            results,
            columns=columns
        )
        return ind_df


    def get_parents(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all parents of a basket.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.

        Returns
        ----------
        pandas.DataFrame containing all the manifest data AND generation level
        of parents (and recursively their parents) of the given basket.
        """
        if self.file_system.exists(os.fspath(basket_address)):
            basket_uuid = self.cur.execute(
                "SELECT uuid FROM pantry_index WHERE address = ?",
                (basket_address,)
            ).fetchone()
        else:
            basket_uuid = basket_address

        parent_df = pd.DataFrame(self.cur.execute(
            """WITH RECURSIVE
                child_record(level, id) AS (
                    VALUES(0, ?)
                    UNION
                    SELECT child_record.level+1, parent_uuids.parent_uuid
                    FROM parent_uuids, child_record
                    WHERE parent_uuids.uuid = child_record.id
                )
            SELECT pantry_index.*, child_record.level
            FROM pantry_index, parent_uuids, child_record
            WHERE pantry_index.uuid = parent_uuids.uuid
                AND parent_uuids.uuid = child_record.id
            ORDER BY child_record.level ASC""", (basket_uuid,)
        ).fetchall())
        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        columns.append("generation_level")
        parent_df.columns = columns
        return parent_df

    def get_children(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all children of a basket.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.

        Returns
        ----------
        pandas.DataFrame containing all the manifest data AND generation level
        of children (and recursively their children) of the given basket.
        """
        if self.file_system.exists(os.fspath(basket_address)):
            basket_uuid = self.cur.execute(
                "SELECT uuid FROM pantry_index WHERE address = ?",
                (basket_address,)
            ).fetchone()
        else:
            basket_uuid = basket_address

        child_df = pd.DataFrame(self.cur.execute(
            """WITH RECURSIVE
                child_record(level, id) AS (
                    VALUES(0, ?)
                    UNION
                    SELECT child_record.level-1, parent_uuids.uuid
                    FROM parent_uuids, child_record
                    WHERE parent_uuids.parent_uuid = child_record.id
                )
            SELECT pantry_index.*, child_record.level
            FROM pantry_index, parent_uuids, child_record
            WHERE pantry_index.uuid = parent_uuids.uuid
            AND parent_uuids.uuid = child_record.id
            ORDER BY child_record.level ASC""", (basket_uuid,)
        ).fetchall())
        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        columns.append("generation_level")
        child_df.columns = columns
        return child_df

    def get_baskets_of_type(self, basket_type, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets of basket_type.

        Parameters
        ----------
        basket_type: str
            The basket type to filter for.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets of the type.
        """
        ind_df = pd.DataFrame(
            self.cur.execute(
                "SELECT * FROM pantry_index"
                "WHERE basket_type = ? LIMIT ?", (basket_type, max_rows)
            ).fetchall()
        )
        ind_df.columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        return ind_df

    def get_baskets_of_label(self, basket_label, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets with label.

        Parameters
        ----------
        basket_label: str
            The label to filter for.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets with the label
        """
        ind_df = pd.DataFrame(
            self.cur.execute(
                "SELECT * FROM pantry_index"
                "WHERE label = ? LIMIT ?", (basket_label, max_rows)
            ).fetchall()
        )
        ind_df.columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        return ind_df

    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   max_rows=1000, **kwargs):
        """Returns a pandas dataframe of baskets uploaded between two times.

        Parameters
        ----------
        start_time: datetime.datetime
            The start datetime object to filter between. If None, will filter
            from the beginning of time.
        end_time: datetime.datetime
            The end datetime object to filter between. If None, will filter
            to the current datetime.
        max_rows: int
            Max rows returned in the pandas dataframe.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets uploaded
        between the start and end times.
        """
        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )

        if start_time and end_time:
            results = self.cur.execute(
                """SELECT * FROM pantry_index
                WHERE upload_time BETWEEN date(?) AND date(?) LIMIT ?
                """, (start_time, end_time, max_rows)).fetchall()
        elif start_time:
            results = self.cur.execute(
                """SELECT * FROM pantry_index
                WHERE upload_time < date(?) LIMIT ?
                """, (start_time, max_rows)).fetchall()
        elif end_time:
            results = self.cur.execute(
                """SELECT * FROM pantry_index
                WHERE upload_time > date(?) LIMIT ?
                """, (end_time, max_rows)).fetchall()
        elif start_time is None and end_time is None:
            return self.to_pandas_df(max_rows=max_rows)

        ind_df = pd.DataFrame(
            results,
            columns=columns
        )

        return ind_df

    def query(self, expr, **kwargs):
        """Returns a pandas dataframe of the results of the query.

        Parameters
        ----------
        expr: str
            An expression passed to the backend. An example could be a SQL or
            pandas query. Largely dependent on concrete implementations.
        **query_args: tuple
            Arguments to pass to the Sqlite query. Should be in the form:
            (arg1, arg2) or (arg1,) if only one argument.

        Returns
        ----------
        pandas.DataFrame of the resulting query.
        """
        query_args = kwargs.get(query_args, ())
        return pd.DataFrame(self.cur.execute(expr, query_args).fetchall())

    def __len__(self):
        """Returns the number of baskets in the index."""
        return (
            self.cur.execute("SELECT COUNT () FROM pantry_index").fetchone()[0]
        )

    def __str__(self):
        """Returns the str instantiation type of this Index (ie 'SQLIndex')."""
        return self.__class__.__name__
