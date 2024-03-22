"""Wherein is contained the concrete SQLite implementation of the Index."""
import os
import sqlite3
import warnings
from datetime import datetime

import ast
import pandas as pd

from .index_abc import IndexABC
from .list_baskets import _get_list_of_basket_jsons
from .create_index import create_index_from_fs


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
        **db_path: str (optional)
            Path to the sqlite db file to be used. If none is set, defaults to
            '{pantry_path}.db'
        """
        self._file_system = file_system
        self._pantry_path = pantry_path

        db_file_name = self._pantry_path.replace(os.sep, "-")

        self.db_path = kwargs.get("db_path", f"weave-{db_file_name}.db")
        self.con = sqlite3.connect(self.db_path)
        self.cur = self.con.cursor()
        self._create_tables()

    def __del__(self):
        """Close the database connection before closing."""
        self.con.close()

    def _create_tables(self):
        """Create the required DB tables if they do not already exist."""
        # THIS NEEDS TO BE UPDATED MANUALLY IF NEW COLUMNS ARE ADDED TO INDEX.
        # THE INSERT IN OTHER FUNCTIONS USE config.index_schema(), BUT THAT
        # CAN'T BE USED HERE AS TYPE NEEDS TO BE SPECIFIED.
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS pantry_index(
                uuid TEXT, upload_time INT, parent_uuids TEXT,
                basket_type TEXT, label TEXT, weave_version TEXT, address TEXT,
                storage_type TEXT, PRIMARY KEY(uuid), UNIQUE(uuid));
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
        """Populates the metadata for the index.

        Parameters
        ----------
        **kwargs unused for this function.
        """
        return {"db_path": self.db_path}

    def generate_index(self, **kwargs):
        """Populates the index from the file system.

        Generate the index by scraping the pantry and adding the manifest data
        of found baskets to the index.

        Parameters
        ----------
        **kwargs unused for this function.
        """
        if not isinstance(self.pantry_path, str):
            raise TypeError("'pantry_path' must be a string: "
                            f"'{self.pantry_path}'")

        if not self.file_system.exists(self.pantry_path):
            raise FileNotFoundError("'pantry_path' does not exist: "
                                    f"'{self.pantry_path}'")

        basket_jsons = _get_list_of_basket_jsons(self.pantry_path,
                                                 self.file_system)

        for basket_json_address in basket_jsons:
            entry = create_index_from_fs(basket_json_address,
                                         file_system=self.file_system)
            if len(self.get_rows(entry['uuid'].iloc[0])) == 0:
                self.track_basket(entry, _commit_db=False)

        self.con.commit()

    def to_pandas_df(self, max_rows=1000, offset=0, **kwargs):
        """Returns the pandas dataframe representation of the index.

        Parameters
        ----------
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        **kwargs unused for this function.

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
        query = "SELECT * FROM pantry_index ORDER BY UUID LIMIT ? OFFSET ?"
        ind_df = pd.DataFrame(
            self.cur.execute(query, (max_rows,offset,))
            .fetchall(),
            columns=columns
        )
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )
        return ind_df

    def track_basket(self, entry_df, **kwargs):
        """Track a basket (or many baskets) from the pantry with the Index.

        Parameters
        ----------
        entry_df: pd.DataFrame
            Uploaded baskets' manifest data to append to the index.

        **_commit_db: bool (default=True)
            Commit the SQL database. Argument is to facilitate generate_index()
        """
        _commit_db = kwargs.get("_commit_db", True)
        uuids = entry_df["uuid"]
        parent_uuids = entry_df["parent_uuids"]

        entry_df["parent_uuids"] = entry_df["parent_uuids"].astype(str)
        entry_df["upload_time"] = (
            entry_df["upload_time"].astype('int64') // 1e9
        ).astype('int64')
        # Bulk insert into pantry_index.
        entry_df.to_sql("pantry_index", self.con,
                        if_exists="append", method="multi", index=False)

        # Insert into the parent_uuids table.
        sql = """INSERT OR IGNORE INTO parent_uuids(
                uuid, parent_uuid) VALUES(?,?)"""

        # Loop all uuids and parent uuids (list of lists).
        for uuid, parent_uuids in zip(uuids, parent_uuids):
            # Loop all parent uuids (now a list of strings)
            for parent_uuid in parent_uuids:
                # Add the uuid, parent_uuid combo to the parent_uuids table.
                self.cur.execute(sql, (uuid, parent_uuid))
        if _commit_db:
            self.con.commit()

    def untrack_basket(self, basket_address, **kwargs):
        """Remove a basket from being tracked of given UUID or path.

        Parameters
        ----------
        basket_address: str or [str]
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. These may also be passed in
            as a list.

        **kwargs unused for this function.
        """
        if not isinstance(basket_address, list):
            basket_address = [basket_address]

        if self.file_system.exists(os.fspath(basket_address[0])):
            uuids = self.cur.execute(
                "SELECT uuid FROM pantry_index WHERE address in"
                f"({','.join(['?']*len(basket_address))})",
                basket_address,
            ).fetchall()
            uuids = [uuid[0] for uuid in uuids]
        else:
            uuids = basket_address

        # Delete from pantry_index.
        self.cur.execute(
            f"DELETE FROM pantry_index WHERE uuid in "
            f"({','.join(['?']*len(uuids))})",
            uuids,
        )
        if self.cur.rowcount != len(uuids):
            warnings.warn(
                UserWarning(
                    "Incomplete Request. Index could not untrack baskets, "
                    "as some were not being tracked to begin with.",
                    len(uuids) - self.cur.rowcount
                )
            )

        # Delete from parent_uuids.
        self.cur.execute(
            "DELETE FROM parent_uuids WHERE uuid in "
            f"({','.join(['?']*len(uuids))})",
            uuids,
        )
        self.con.commit()

    def get_rows(self, basket_address, **kwargs):
        """Returns a pd.DataFrame row information of given UUID or path.

        Parameters
        ----------
        basket_address: str or [str]
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket. These may also be passed in
            as a list.
        **kwargs unused for this function.

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
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )
        return ind_df

    def get_parents(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all parents of a basket.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        **max_gen_level: int (optional)
            This indicates the maximum generation level that will be reported.

        Returns
        ----------
        pandas.DataFrame containing all the manifest data AND generation level
        of parents (and recursively their parents) of the given basket.
        """

        max_gen_level = kwargs.get("max_gen_level", 999)

        if self.file_system.exists(os.fspath(basket_address)):
            id_column = "address"
        else:
            id_column = "uuid"
        basket_uuid = self.cur.execute(
            f"SELECT uuid FROM pantry_index WHERE {id_column} = ?",
            (basket_address,)
        ).fetchone()

        if basket_uuid is None:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0]

        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        columns.append("generation_level")
        columns.append("path")

        parent_df = pd.DataFrame(self.cur.execute(
            """WITH RECURSIVE
                child_record(level, id, path) AS (
                    VALUES(0, ?, ?)
                    UNION
                    SELECT child_record.level + 1, parent_uuids.parent_uuid,
                        path || '/' || parent_uuids.parent_uuid
                    FROM parent_uuids
                    JOIN child_record ON parent_uuids.uuid = child_record.id
                    WHERE path NOT LIKE parent_uuids.parent_uuid || '/%'
                        AND path NOT LIKE '%' || parent_uuids.parent_uuid
                        AND path
                            NOT LIKE '%' || parent_uuids.parent_uuid || '/%'
                        AND child_record.level < ?
                )
            SELECT pantry_index.*, child_record.level, child_record.path
            FROM pantry_index
            JOIN child_record ON pantry_index.uuid = child_record.id
            ORDER BY child_record.level ASC;""",
            (basket_uuid, basket_uuid, max_gen_level)
        ).fetchall(),
            columns=columns,
        )

        parent_df = parent_df.drop_duplicates()
        parent_df = parent_df[parent_df["uuid"] != basket_uuid]

        if parent_df.empty:
            return parent_df
        parent_df["parent_uuids"] = parent_df["parent_uuids"].apply(
            ast.literal_eval
        )
        parent_df["upload_time"] = pd.to_datetime(
            parent_df["upload_time"],
            unit="s",
            origin="unix",
        )

        for _, row in parent_df.iterrows():
            for prev in row['path'].split('/'):
                if prev in row["parent_uuids"]:
                    raise ValueError(
                        f"Parent-Child loop found at uuid: {basket_uuid}"
                    )

        parent_df.drop(columns="path", inplace=True)

        return parent_df

    def get_children(self, basket_address, **kwargs):
        """Returns a pandas dataframe of all children of a basket.

        Parameters
        ----------
        basket_address: str
            Argument can take one of two forms: either a path to the basket
            directory, or the UUID of the basket.
        **min_gen_level: int (optional)
            This indicates the minimum generation level that will be reported.

        Returns
        ----------
        pandas.DataFrame containing all the manifest data AND generation level
        of children (and recursively their children) of the given basket.
        """

        min_gen_level = kwargs.get("min_gen_level", -999)

        if self.file_system.exists(os.fspath(basket_address)):
            id_column = "address"
        else:
            id_column = "uuid"
        basket_uuid = self.cur.execute(
            f"SELECT uuid FROM pantry_index WHERE {id_column} = ?",
            (basket_address,)
        ).fetchone()

        if basket_uuid is None:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0]

        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        columns.append("generation_level")
        columns.append("path")

        child_df = pd.DataFrame(self.cur.execute(
                """WITH RECURSIVE
                    child_record(level, id, path) AS (
                        VALUES(0, ?, ?)
                        UNION
                        SELECT child_record.level - 1, parent_uuids.uuid,
                            path || '/' || parent_uuids.uuid
                        FROM parent_uuids
                        JOIN child_record
                            ON parent_uuids.parent_uuid = child_record.id
                        WHERE path NOT LIKE parent_uuids.uuid || '/%'
                            AND path NOT LIKE '%' || parent_uuids.uuid
                            AND path NOT LIKE '%' || parent_uuids.uuid || '/%'
                        AND child_record.level > ?
                    )
                SELECT pantry_index.*, child_record.level, child_record.path
                FROM pantry_index
                JOIN child_record ON pantry_index.uuid = child_record.id
                ORDER BY child_record.level DESC""",
                (basket_uuid, basket_uuid, min_gen_level)
            ).fetchall(),
            columns=columns,
        )
        child_df = child_df.drop_duplicates()
        child_df["parent_uuids"] = child_df["parent_uuids"].apply(
            ast.literal_eval
        )
        child_df["upload_time"] = pd.to_datetime(
            child_df["upload_time"],
            unit="s",
            origin="unix",
        )

        parents = {}
        for _, row in child_df.iterrows():
            parents[row['uuid']] = row["parent_uuids"]
            for prev in row['path'].split('/'):
                if row['uuid'] in parents[prev]:
                    raise ValueError(
                        f"Parent-Child loop found at uuid: {basket_uuid}"
                    )

        child_df = child_df[child_df['uuid'] != basket_uuid]
        child_df.drop(columns="path", inplace=True)

        return child_df

    def get_baskets_of_type(self, basket_type, max_rows=1000,
                            offset=0, **kwargs):
        """Returns a pandas dataframe containing baskets of basket_type.

        Parameters
        ----------
        basket_type: str
            The basket type to filter for.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets of the type.
        """
        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        query="""SELECT * FROM pantry_index WHERE basket_type = ?
                 ORDER BY UUID LIMIT ? OFFSET ?"""
        ind_df = pd.DataFrame(
            self.cur.execute(
                query, (basket_type, max_rows, offset)
            ).fetchall(),
            columns=columns,
        )
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )
        return ind_df

    def get_baskets_of_label(self, basket_label, max_rows=1000,
                             offset=0, **kwargs):
        """Returns a pandas dataframe containing baskets with label.

        Parameters
        ----------
        basket_label: str
            The label to filter for.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets with the label
        """
        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )
        query = """SELECT * FROM pantry_index WHERE label = ?
                   ORDER BY UUID LIMIT ? OFFSET ?"""
        ind_df = pd.DataFrame(
            self.cur.execute(
                query, (basket_label, max_rows, offset)
            ).fetchall(),
            columns=columns,
        )
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )
        return ind_df

    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   max_rows=1000, offset=0, **kwargs):
        """Returns a pandas dataframe of baskets uploaded between two times.

        Parameters
        ----------
        start_time: datetime.datetime (optional)
            The start datetime object to filter between. If None, will filter
            from the beginning of time.
        end_time: datetime.datetime (optional)
            The end datetime object to filter between. If None, will filter
            to the current datetime.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        offset: int (default=0)
            Offset from the beginning of the index to begin the query

        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets uploaded
        between the start and end times.
        """
        super().get_baskets_by_upload_time(start_time, end_time)
        if start_time is None and end_time is None:
            return self.to_pandas_df(max_rows=max_rows, offset=offset)

        columns = (
            [info[1] for info in
             self.cur.execute("PRAGMA table_info(pantry_index)").fetchall()]
        )

        if start_time and end_time:
            start_time = int(datetime.timestamp(start_time))
            end_time = int(datetime.timestamp(end_time))
            results = self.cur.execute(
                """SELECT * FROM pantry_index
                WHERE upload_time >= ? AND upload_time <= ?
                ORDER BY UUID LIMIT ? OFFSET ?
                """, (start_time, end_time, max_rows, offset)).fetchall()
        elif start_time:
            start_time = int(datetime.timestamp(start_time))
            results = self.cur.execute(
                """SELECT * FROM pantry_index
                WHERE upload_time >= ? ORDER BY UUID LIMIT ? OFFSET ?
                """, (start_time, max_rows, offset)).fetchall()
        elif end_time:
            end_time = int(datetime.timestamp(end_time))
            results = self.cur.execute(
                """SELECT * FROM pantry_index
                WHERE upload_time <= ? ORDER BY UUID LIMIT ? OFFSET ?
                """, (end_time, max_rows, offset)).fetchall()

        ind_df = pd.DataFrame(
            results,
            columns=columns
        )
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )

        return ind_df

    def query(self, expr, **kwargs):
        """Returns a pandas dataframe of the results of the expression.

        Parameters
        ----------
        expr: str
            An expression passed to the backend. An example could be a SQL or
            pandas query. Largely dependent on concrete implementations.
        **expr_args: tuple (optional)
            Arguments to pass to the SQLite expression. Should be in the form:
            (arg1, arg2) or (arg1,) if only one argument.

        Returns
        ----------
        pandas.DataFrame of the resulting query.
        """
        expr_args = kwargs.get("expr_args", ())
        return pd.DataFrame(self.cur.execute(expr, expr_args).fetchall())

    def __len__(self):
        """Returns the number of baskets in the index."""
        return (
            self.cur.execute("SELECT COUNT () FROM pantry_index").fetchone()[0]
        )

    def __str__(self):
        """Returns the str instantiation type of this Index (ie 'SQLIndex')."""
        return self.__class__.__name__
