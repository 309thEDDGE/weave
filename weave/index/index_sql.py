"""Wherein is contained the concrete SQLite implementation of the Index."""
# Pylint doesn't like the similarity between this file and the SQLite file, but
# it doesn't make sense to write shared functions for them. So ignore pylint.
# pylint: disable=duplicate-code
import json
import os
import warnings
from datetime import datetime

import ast
import dateutil
import pandas as pd
# Try-Except required to make pyodbc an optional dependency.
try:
    import pyodbc
except ImportError:
    _HAS_PYODBC = False
else:
    _HAS_PYODBC = True

from ..config import get_index_column_names
from .index_abc import IndexABC
from .list_baskets import _get_list_of_basket_jsons
from .validate_basket import validate_basket_dict

class IndexSQL(IndexABC):
    """Concrete implementation of Index, using SQL."""
    def __init__(self, file_system, pantry_path, **kwargs):
        """Initializes the Index class.

        Parameters
        ----------
        file_system: fsspec object
            The fsspec object which hosts the pantry we desire to index.
        pantry_path: str
            Path to the pantry root which we want to index.
        **database_name: str (default='weave_db')
            DB to be used. If none is set, defaults to 'weave_db'.
        **encrypt: bool (default=False)
            Whether or not to encrypt the database (disabled by default, due
            to the fact that it requires additional configuration).
        **odbc_driver: str (default='{ODBC Driver 18 for SQL Server}')
            Driver to be used for the connection. Usually similar to:
            '{ODBC Driver <18, 17, 13...> for SQL Server}'.
        **pantry_schema: str (default=<pantry_path>)
            The schema to use for the pantry. If none is set, defaults to the
            pantry path (with _ replacements when necessary).
        """
        if not _HAS_PYODBC:
            raise ImportError("Missing Dependency. The package 'pyodbc' "
                              "is required to use this class")

        # Check that the required environment variables are set.
        try:
            # We want to fail early if these are not set, so we check them here
            # even though they are not used until we connect.
            # Pylint thinks this is pointless, so pylint is getting ignored.
            # pylint: disable=pointless-statement
            os.environ["MSSQL_HOST"]
            os.environ["MSSQL_USERNAME"]
            os.environ["MSSQL_PASSWORD"]
            # pylint: enable=pointless-statement
        except KeyError as key_error:
            raise KeyError("The following environment variables must be set "
                           "to use this class: MSSQL_HOST, MSSQL_USERNAME, "
                           "MSSQL_PASSWORD.") from key_error

        self._file_system = file_system
        self._pantry_path = pantry_path

        self._database_name = kwargs.get("database_name", "weave_db")\
            .replace("-", "_")
        d_schema_name = self._pantry_path.replace(os.sep, "_")\
            .replace("-", "_")
        self._pantry_schema = kwargs.get("pantry_schema", d_schema_name)

        # Make a connection to tempdb, then create the desired database if it
        # does not exist. Then connect to the desired database, and create the
        # tables if they do not exist.
        self._connect(database_name="tempdb", **kwargs)
        self._select_db(database_name=self._database_name)
        self._create_schema()
        self._create_tables()

    # Create read-only properties for the database and schema names.
    @property
    def database_name(self):
        """The database name of the index."""
        return self._database_name

    @property
    def pantry_schema(self):
        """The schema name of the index."""
        return self._pantry_schema

    def __del__(self):
        """Close the connection to the database."""
        try:
            self.cur.close()
            self.con.close()
        except AttributeError:
            pass

    def _connect(self, database_name="weave_db", **kwargs):
        """Connect to the server, and select the specified DB.

        Parameters
        ----------
        database_name: str (default='weave_db')
            The database to connect to.
        **odbc_driver: str (default='{ODBC Driver 18 for SQL Server}')
            Driver to be used for the connection. Usually similar to:
            '{ODBC Driver <18, 17, 13...> for SQL Server}'.
        **encrypt: bool (default=False)
            Whether or not to encrypt the database (disabled by default, due
            to the fact that it requires additional configuration).
        """
        driver = kwargs.get("odbc_driver", "{ODBC Driver 18 for SQL Server}")
        encrypt = kwargs.get("encrypt", False)

        # Pylint has a problem recognizing 'connect' as a valid member function
        # so we ignore that here.
        # pylint: disable-next=c-extension-no-member
        self.con = pyodbc.connect(
            f"DRIVER={driver};"
            f"SERVER={os.environ['MSSQL_HOST']};"
            f"DATABASE={database_name};"
            f"UID={os.environ['MSSQL_USERNAME']};"
            f"PWD={os.environ['MSSQL_PASSWORD']};"
            f"Encrypt={'yes' if encrypt else 'no'};"
        )
        self.cur = self.con.cursor()

    def _select_db(self, database_name="weave_db"):
        """Select the database to use, or create it if it does not exist.

        Parameters
        ----------
        database_name: str (default='weave_db')
            The database to connect to.
        """
        # Query to see if the database exists.
        self.cur.execute(
            "SELECT * FROM sys.databases WHERE name = ?", (database_name,)
        )
        # If the database does not exist, create it.
        if not self.cur.fetchone():
            self.con.autocommit = True
            self.cur.execute(f"CREATE DATABASE {database_name};")
            self.cur.execute(f"USE {database_name};")
            self.con.autocommit = False
        else:
            self.cur.execute(f"USE {database_name};")

    def _create_schema(self):
        """Create the schema if it does not already exist."""
        # Check if self.pantry_schema exists.
        self.cur.execute(
            "SELECT * FROM sys.schemas WHERE name = ?", (self.pantry_schema,)
        )
        # If it does not exist, create it.
        if not self.cur.fetchone():
            self.cur.execute(f"CREATE SCHEMA {self.pantry_schema};")
            self.con.commit()

    def _create_tables(self):
        """Create the required tables if they do not already exist."""
        # THIS NEEDS TO BE UPDATED MANUALLY IF NEW COLUMNS ARE ADDED TO INDEX.
        # THE INSERT IN OTHER FUNCTIONS USE config.index_schema(), BUT THAT
        # CAN'T BE USED HERE AS TYPE NEEDS TO BE SPECIFIED.
        self.cur.execute(
            f"""
            IF NOT EXISTS (
                SELECT * FROM sys.tables t
                JOIN sys.schemas s ON (t.schema_id = s.schema_id)
                WHERE s.name = '{self.pantry_schema}'
                AND t.name = 'pantry_index'
            )
            CREATE TABLE {self.pantry_schema}.pantry_index (
                uuid varchar(64),
                upload_time INT,
                parent_uuids TEXT,
                basket_type TEXT,
                label TEXT,
                weave_version TEXT,
                address TEXT,
                storage_type TEXT,
                PRIMARY KEY(uuid),
                UNIQUE(uuid)
            );
            """
        )

        self.cur.execute(
            f"""
            IF NOT EXISTS (
                SELECT * FROM sys.tables t
                JOIN sys.schemas s ON (t.schema_id = s.schema_id)
                WHERE s.name = '{self.pantry_schema}'
                AND t.name = 'parent_uuids'
            )
            CREATE TABLE {self.pantry_schema}.parent_uuids (
                uuid varchar(64),
                parent_uuid varchar(64),
                PRIMARY KEY(uuid, parent_uuid),
                UNIQUE(uuid, parent_uuid)
            );
            """
        )
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

        Returns
        ----------
        dict
            Returns a dictionary of the metadata.
        """
        return {
            "database_host": os.environ["MSSQL_HOST"],
            "database_name": self.database_name,
            "database_schema": self.pantry_schema,
        }

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

        basket_jsons = _get_list_of_basket_jsons(
            self.pantry_path,
            self.file_system,
        )
        index_columns = get_index_column_names()
        num_index_columns = len(index_columns)
        index_columns = ", ".join(index_columns)

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
                basket_dict["storage_type"] = (
                    self.file_system.__class__.__name__
                )
                basket_dict["upload_time"] = int(
                    datetime.timestamp(
                        dateutil.parser.parse(basket_dict["upload_time"])
                    )
                )

                parent_uuids = basket_dict["parent_uuids"]
                basket_dict["parent_uuids"] = str(basket_dict["parent_uuids"])

                if "weave_version" not in basket_dict.keys():
                    basket_dict["weave_version"] = "<0.13.0"

                sql = (
                    f"""
                    INSERT INTO {self.pantry_schema}.pantry_index(
                        {index_columns}
                    )
                    SELECT {','.join(['?']*num_index_columns)}
                    WHERE NOT EXISTS
                        (SELECT 1 FROM {self.pantry_schema}.pantry_index
                        WHERE uuid = ?);
                    """
                )

                # Get the args as a single tuple value.
                args = tuple(basket_dict.values()) + (basket_dict['uuid'],)
                self.cur.execute(sql, args)

                sql = (
                    f"""
                    INSERT INTO {self.pantry_schema}.parent_uuids(
                        uuid, parent_uuid
                    )
                    SELECT ?, ?
                    WHERE NOT EXISTS 
                        (SELECT 1 FROM {self.pantry_schema}.parent_uuids
                        WHERE uuid = ? AND parent_uuid = ?);
                    """
                )
                for parent_uuid in parent_uuids:
                    self.cur.execute(
                        sql,
                        (basket_dict['uuid'], parent_uuid,
                         basket_dict['uuid'], parent_uuid),
                    )

        if len(bad_baskets) != 0:
            warnings.warn("baskets found in the following locations "
                          "do not follow specified weave schema:\n"
                          f"{bad_baskets}")

        self.con.commit()

    def to_pandas_df(self, max_rows=1000, **kwargs):
        """Returns the pandas dataframe representation of the index.

        Parameters
        ----------
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.
        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame
            Returns a dataframe of the manifest data of the baskets in the
            pantry.
        """
        # Get the rows from the index as a list of lists, then get the columns.
        result = self.cur.execute(
            f"SELECT TOP (?) * FROM {self.pantry_schema}.pantry_index",
            (max_rows,)
        ).fetchall()
        result = [list(row) for row in result]
        columns=[column[0] for column in self.cur.description]

        ind_df = pd.DataFrame(result, columns=columns)
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

        **kwargs unused for this function.
        """
        # Save off the original uuid and parent_uuids columns.
        uuids = entry_df["uuid"]
        parent_uuids = entry_df["parent_uuids"]

        # Convert the parent_uuids to a string, and the upload_time to an int.
        entry_df["parent_uuids"] = entry_df["parent_uuids"].astype(str)
        entry_df["upload_time"] = (
            entry_df["upload_time"].astype(int) // 1e9
        ).astype(int)

        index_columns = get_index_column_names()
        index_columns_str = ", ".join(index_columns)

        for basket_dict in entry_df.to_dict(orient="records"):
            # Insert into pantry_index.
            sql = (
                f"""
                INSERT INTO {self.pantry_schema}.pantry_index(
                    {index_columns_str}
                )
                SELECT {', '.join(['?']*len(index_columns))}
                WHERE NOT EXISTS
                    (SELECT 1 FROM {self.pantry_schema}.pantry_index
                    WHERE uuid = ?);
                """
            )
            # Get the args as a single tuple value.
            args = tuple(basket_dict.values()) + (basket_dict['uuid'],)
            self.cur.execute(sql, args)

            # Insert into parent_uuids.
            sql = (
                f"""
                INSERT INTO {self.pantry_schema}.parent_uuids(
                    uuid, parent_uuid
                )
                SELECT ?, ?
                WHERE NOT EXISTS
                    (SELECT 1 FROM {self.pantry_schema}.parent_uuids
                    WHERE uuid = ? AND parent_uuid = ?);
                """
            )
            # Loop all uuids and parent uuids (list of lists).
            for uuid, parent_uuids in zip(uuids, parent_uuids):
                # Loop all parent uuids (now a list of strings)
                for parent_uuid in parent_uuids:
                    self.cur.execute(
                        sql,
                        (uuid, parent_uuid,
                        uuid, parent_uuid),
                    )

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
                f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
                "WHERE CONVERT(nvarchar(MAX), address) IN "
                "(SELECT CONVERT(nvarchar(MAX), value) "
                "FROM STRING_SPLIT(?, ','))",
                ','.join(basket_address)
            ).fetchall()
            uuids = [uuid[0] for uuid in uuids]
        else:
            uuids = basket_address

        # Delete from pantry_index.
        query = (
            f"DELETE FROM {self.pantry_schema}.pantry_index WHERE "
            "CONVERT(nvarchar(MAX), uuid) IN "
            "(SELECT CONVERT(nvarchar(MAX), value) FROM STRING_SPLIT(?, ','))"
        )
        self.cur.execute(query, ','.join(uuids))
        if self.cur.rowcount != len(uuids):
            warnings.warn(
                UserWarning(
                    "Incomplete Request. Index could not untrack baskets, "
                    "as some were not being tracked to begin with.",
                    len(uuids) - self.cur.rowcount
                )
            )

        # Delete from parent_uuids.
        query = (
            f"DELETE FROM {self.pantry_schema}.parent_uuids WHERE "
            "CONVERT(nvarchar(MAX), uuid) IN "
            "(SELECT CONVERT(nvarchar(MAX), value) FROM STRING_SPLIT(?, ','))"
        )
        self.cur.execute(query, ','.join(uuids))
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
            f"SELECT * FROM {self.pantry_schema}.pantry_index "
            f"WHERE CONVERT(nvarchar(MAX), {id_column}) IN "
            "(SELECT CONVERT(nvarchar(MAX), value) FROM STRING_SPLIT(?, ','))"
        )
        results = self.cur.execute(query, ','.join(basket_address)).fetchall()
        results = [list(row) for row in results]
        columns = [column[0] for column in self.cur.description]

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
            f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
            f"WHERE CONVERT(nvarchar(MAX), {id_column}) = ?",
            (basket_address,)
        ).fetchone()

        if basket_uuid is None:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0]

        results = self.cur.execute(
            f"""
            WITH child_record AS (
                SELECT
                    0 AS level,
                    CAST(? AS nvarchar(max)) AS id,
                    CAST(? AS nvarchar(max)) AS path
                UNION ALL
                SELECT
                    child_record.level + 1,
                    CAST(parent_uuids.parent_uuid AS nvarchar(max)) AS id,
                    CAST(child_record.path + '/' + parent_uuids.parent_uuid
                        AS nvarchar(max)) AS path
                FROM {self.pantry_schema}.parent_uuids
                JOIN child_record ON parent_uuids.uuid = child_record.id
                WHERE 
                    path NOT LIKE CONCAT(parent_uuids.parent_uuid, '/%')
                    AND path NOT LIKE CONCAT('%', parent_uuids.parent_uuid)
                    AND path NOT LIKE
                        CONCAT('%', parent_uuids.parent_uuid, '/%')
                AND child_record.level < ?
            )
            SELECT pantry_index.*, child_record.level, child_record.path
            FROM {self.pantry_schema}.pantry_index as pantry_index
            JOIN child_record ON pantry_index.uuid = child_record.id
            ORDER BY child_record.level ASC OPTION (MAXRECURSION 0);
            """,
            (basket_uuid, basket_uuid, max_gen_level)
        ).fetchall()

        results = [list(row) for row in results]
        columns = [column[0] for column in self.cur.description]
        columns[columns.index("level")] = "generation_level"

        parent_df = pd.DataFrame(results, columns=columns)
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
            f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
            f"WHERE CONVERT(nvarchar(MAX), {id_column}) = ?",
            (basket_address,)
        ).fetchone()

        if basket_uuid is None:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0]

        results = self.cur.execute(
            f"""
            WITH child_record AS (
                SELECT
                    0 AS level,
                    CAST(? AS nvarchar(max)) AS id,
                    CAST(? AS nvarchar(max)) AS path
                UNION ALL
                SELECT 
                    child_record.level - 1,
                    CAST(parent_uuids.uuid AS nvarchar(max)) AS id,
                    CAST(child_record.path + '/' + parent_uuids.uuid
                        AS nvarchar(max)) AS path
                FROM {self.pantry_schema}.parent_uuids
                JOIN child_record ON parent_uuids.parent_uuid = child_record.id
                WHERE 
                    path NOT LIKE CONCAT(parent_uuids.uuid, '/%')
                    AND path NOT LIKE CONCAT('%', parent_uuids.uuid)
                    AND path NOT LIKE CONCAT('%', parent_uuids.uuid, '/%')
                AND child_record.level > ?
            )
            SELECT pantry_index.*, child_record.level, child_record.path
            FROM {self.pantry_schema}.pantry_index as pantry_index
            JOIN child_record ON pantry_index.uuid = child_record.id
            ORDER BY child_record.level DESC OPTION (MAXRECURSION 0);
            """,
            (basket_uuid, basket_uuid, min_gen_level)
        ).fetchall()

        results = [list(row) for row in results]
        columns = [column[0] for column in self.cur.description]
        columns[columns.index("level")] = "generation_level"

        child_df = pd.DataFrame(results, columns=columns)
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

    def get_baskets_of_type(self, basket_type, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets of basket_type.

        Parameters
        ----------
        basket_type: str
            The basket type to filter for.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.

        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets of the type.
        """
        result = self.cur.execute(
            f"SELECT TOP (?) * FROM {self.pantry_schema}.pantry_index "
            "WHERE CONVERT(nvarchar(MAX), basket_type) = ?",
            (max_rows, basket_type),
        ).fetchall()
        result = [list(row) for row in result]
        columns = [column[0] for column in self.cur.description]

        ind_df = pd.DataFrame(result, columns=columns)
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )
        return ind_df

    def get_baskets_of_label(self, basket_label, max_rows=1000, **kwargs):
        """Returns a pandas dataframe containing baskets with label.

        Parameters
        ----------
        basket_label: str
            The label to filter for.
        max_rows: int (default=1000)
            Max rows returned in the pandas dataframe.

        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets with the label
        """
        result = self.cur.execute(
            f"SELECT TOP (?) * FROM {self.pantry_schema}.pantry_index "
            "WHERE CONVERT(nvarchar(MAX), label) = ?",
            (max_rows, basket_label),
        ).fetchall()
        result = [list(row) for row in result]
        columns = [column[0] for column in self.cur.description]

        ind_df = pd.DataFrame(result, columns=columns)
        ind_df["parent_uuids"] = ind_df["parent_uuids"].apply(ast.literal_eval)
        ind_df["upload_time"] = pd.to_datetime(
            ind_df["upload_time"],
            unit="s",
            origin="unix",
        )
        return ind_df

    def get_baskets_by_upload_time(self, start_time=None, end_time=None,
                                   max_rows=1000, **kwargs):
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

        **kwargs unused for this function.

        Returns
        ----------
        pandas.DataFrame containing the manifest data of baskets uploaded
        between the start and end times.
        """
        super().get_baskets_by_upload_time(start_time, end_time)
        if start_time is None and end_time is None:
            return self.to_pandas_df(max_rows=max_rows)

        if start_time and end_time:
            start_time = int(datetime.timestamp(start_time))
            end_time = int(datetime.timestamp(end_time))
            results = self.cur.execute(
                f"""SELECT TOP (?) * FROM {self.pantry_schema}.pantry_index
                WHERE upload_time >= ? AND upload_time <= ?
                """, (max_rows, start_time, end_time)).fetchall()
        elif start_time:
            start_time = int(datetime.timestamp(start_time))
            results = self.cur.execute(
                f"""SELECT TOP (?) * FROM {self.pantry_schema}.pantry_index
                WHERE upload_time >= ?
                """, (max_rows, start_time)).fetchall()
        elif end_time:
            end_time = int(datetime.timestamp(end_time))
            results = self.cur.execute(
                f"""SELECT TOP (?) * FROM {self.pantry_schema}.pantry_index
                WHERE upload_time <= ?
                """, (max_rows, end_time)).fetchall()

        results = [list(row) for row in results]
        columns = [column[0] for column in self.cur.description]

        ind_df = pd.DataFrame(results, columns=columns)
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
        results = self.cur.execute(expr, expr_args).fetchall()
        results = [list(row) for row in results]
        columns = [column[0] for column in self.cur.description]
        return pd.DataFrame(results, columns=columns)

    def __len__(self):
        """Returns the number of baskets in the index."""
        return (
            self.cur.execute(
                f"SELECT COUNT (*) FROM {self.pantry_schema}.pantry_index"
            ).fetchone()[0]
        )

    def __str__(self):
        """Returns the str instantiation type of this Index (ie 'IndexSQL')."""
        return self.__class__.__name__
