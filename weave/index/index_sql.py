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
# Try-Except required to make pyodbc/sqlalchemy an optional dependency.
try:
    # pylint: disable=unused-import
    # pyodbc is imported here because sqlalchemy requires it.
    import pyodbc # noqa: F401
    import sqlalchemy as sqla # noqa: F401
    # pylint: enable=unused-import
except ImportError:
    _HAS_REQUIRED_DEPS = False
else:
    _HAS_REQUIRED_DEPS = True

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
        **odbc_driver: str (default='ODBC Driver 18 for SQL Server')
            Driver to be used for the connection. Usually similar to:
            'ODBC Driver <18, 17, 13...> for SQL Server'.
        **pantry_schema: str (default=<pantry_path>)
            The schema to use for the pantry. If none is set, defaults to the
            pantry path (with _ replacements when necessary).
        """
        if not _HAS_REQUIRED_DEPS:
            raise ImportError("Missing Dependencies. The packages: "
                              "'sqlalchemy' are required to use this class")

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

        # Set the database name (defaults to weave_db). DATABASE MUST ALREADY
        # EXIST. If it does not, the user must create it manually.
        self._database_name = kwargs.get("database_name", "weave_db")\
            .replace("-", "_")

        # Set the schema name (defaults to pantry_path). If the schema does not
        # exist, it will be created.
        d_schema_name = self._pantry_path.replace(os.sep, "_")\
            .replace("-", "_")
        self._pantry_schema = kwargs.get("pantry_schema", d_schema_name)

        self._engine = sqla.create_engine(sqla.engine.url.URL(
            drivername="mssql+pyodbc",
            username=os.environ["MSSQL_USERNAME"],
            password=os.environ["MSSQL_PASSWORD"],
            host=os.environ["MSSQL_HOST"],
            database=self.database_name,
            query={
                "driver": kwargs.get("odbc_driver",
                                     "ODBC Driver 18 for SQL Server"),
                "Encrypt": "yes" if kwargs.get("encrypt", False) else "no",
            },
            port=os.environ.get("MSSQL_PORT", "1433"),
        ))

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

    def execute_sql(self, sql_query, params=None, commit=False):
        """Executes the given SQL query. Returns the results.

        Parameters
        ----------
        sql_query: str or sqlalchemy.sql.text
            The SQL query to be executed.
        params: dict (optional)
            The parameters to be used in the query.
        commit: bool (default=False)
            Whether or not to commit the query.

        Returns
        ----------
        (list, list) or int or None
            Returns the results of the query and the column names as a tuple.
            If statement affects rows, returns the number of rows affected.
            If the query does not return any results, returns None.
        """
        try:
            with self._engine.connect() as connection:
                if isinstance(sql_query, sqla.sql.elements.TextClause):
                    query = sql_query
                elif isinstance(sql_query, str):
                    query = sqla.sql.text(sql_query)
                else:
                    raise ValueError(
                        "sql_query should be a str or a "
                        "sqlalchemy TextClause object"
                    )

                if params is not None:
                    if not isinstance(params, dict):
                        raise TypeError("params should be a dict")

                    # Execute the SQL query with parameters
                    result = connection.execute(query, params)
                else:
                    # Execute the SQL query without parameters
                    result = connection.execute(query)

                if commit:
                    connection.commit()

                # Fetch and return the results
                if result.returns_rows:
                    return result.fetchall(), list(result.keys())

                # Return rows affected (used for INSERT, DELETE, etc.)
                if result.rowcount != -1:
                    return result.rowcount

                return None

        except sqla.exc.SQLAlchemyError as err:
            raise err

    def _create_schema(self):
        """Create the schema if it does not already exist."""
        # Check if self.pantry_schema exists.
        results, _ = self.execute_sql(
            "SELECT * FROM sys.schemas WHERE name = :pantry_schema;",
            {"pantry_schema": self.pantry_schema}
        )
        # If it does not exist, create it.
        if not results:
            self.execute_sql(f"CREATE SCHEMA {self.pantry_schema};",
                             commit=True)

    def _create_tables(self):
        """Create the required tables if they do not already exist."""
        # THIS NEEDS TO BE UPDATED MANUALLY IF NEW COLUMNS ARE ADDED TO INDEX.
        # THE INSERT IN OTHER FUNCTIONS USE config.index_schema(), BUT THAT
        # CAN'T BE USED HERE AS TYPE NEEDS TO BE SPECIFIED.
        self.execute_sql(
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
            """, commit=True
        )

        self.execute_sql(
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
            """, commit=True
        )

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
            "database_port": os.environ.get("MSSQL_PORT", "1433"),
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

                # Insert into pantry_index.
                index_columns = list(basket_dict.keys())
                sql = sqla.text(
                    f"INSERT INTO {self.pantry_schema}.pantry_index ("
                    f"{', '.join([f'{column}' for column in index_columns])}) "
                    "SELECT "
                    f"{', '.join([f':{column}' for column in index_columns])} "
                    "WHERE NOT EXISTS "
                    f"(SELECT 1 FROM {self.pantry_schema}.pantry_index "
                    "WHERE uuid = :uuid);"
                )
                self.execute_sql(sql, basket_dict, commit=True)

                # Insert into parent_uuids.
                sql = sqla.text(
                    f"INSERT INTO {self.pantry_schema}.parent_uuids "
                    "(uuid, parent_uuid) "
                    "SELECT :uuid, :parent_uuid "
                    "WHERE NOT EXISTS "
                    f"(SELECT 1 FROM {self.pantry_schema}.parent_uuids "
                    "WHERE uuid = :uuid AND parent_uuid = :parent_uuid);"
                )
                # Loop all uuids and parent uuids.
                for parent_uuid in parent_uuids:
                    self.execute_sql(
                        sql,
                        {
                            "uuid": basket_dict["uuid"],
                            "parent_uuid": parent_uuid,
                        },
                        commit=True,
                    )

        if len(bad_baskets) != 0:
            warnings.warn("baskets found in the following locations "
                          "do not follow specified weave schema:\n"
                          f"{bad_baskets}")

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
        result, columns = self.execute_sql(
            f"SELECT TOP (:max_rows) * FROM {self.pantry_schema}.pantry_index",
            {"max_rows": max_rows},
        )
        result = [list(row) for row in result]

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

        for basket_dict in entry_df.to_dict(orient="records"):
            index_columns = list(basket_dict.keys())
            # Insert into pantry_index.
            sql = sqla.text(
                f"INSERT INTO {self.pantry_schema}.pantry_index ("
                f"{', '.join([f'{column}' for column in index_columns])}) "
                "SELECT "
                f"{', '.join([f':{column}' for column in index_columns])} "
                "WHERE NOT EXISTS "
                f"(SELECT 1 FROM {self.pantry_schema}.pantry_index "
                "WHERE uuid = :uuid);"
            )
            self.execute_sql(sql, basket_dict, commit=True)

            # Insert into parent_uuids.
            sql = sqla.text(
                f"INSERT INTO {self.pantry_schema}.parent_uuids "
                "(uuid, parent_uuid) "
                "SELECT :uuid, :parent_uuid "
                "WHERE NOT EXISTS "
                f"(SELECT 1 FROM {self.pantry_schema}.parent_uuids "
                "WHERE uuid = :uuid AND parent_uuid = :parent_uuid);"
            )
            # Loop all uuids and parent uuids (list of lists).
            for uuid, parent_uuids in zip(uuids, parent_uuids):
                # Loop all parent uuids (now a list of strings)
                for parent_uuid in parent_uuids:
                    self.execute_sql(
                        sql,
                        {
                            "uuid": uuid,
                            "parent_uuid": parent_uuid,
                        },
                        commit=True,
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
            uuids, _ = self.execute_sql(sqla.text(
                f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
                "WHERE CONVERT(nvarchar(MAX), address) IN "
                "(SELECT CONVERT(nvarchar(MAX), value) "
                "FROM STRING_SPLIT(:basket_address, ','))"),
                {"basket_address": ','.join(basket_address)}
            )
            uuids = [uuid[0] for uuid in uuids]
        else:
            uuids = basket_address

        # Delete from pantry_index.
        query = (
            f"DELETE FROM {self.pantry_schema}.pantry_index WHERE "
            "CONVERT(nvarchar(MAX), uuid) IN "
            "(SELECT CONVERT(nvarchar(MAX), value) "
            "FROM STRING_SPLIT(:uuids, ','))"
        )
        rowcount = self.execute_sql(
            query,
            {"uuids": ','.join(uuids)},
            commit=True,
        )
        if rowcount != len(uuids):
            warnings.warn(
                UserWarning(
                    "Incomplete Request. Index could not untrack baskets, "
                    "as some were not being tracked to begin with.",
                    len(uuids) - rowcount
                )
            )

        # Delete from parent_uuids.
        query = (
            f"DELETE FROM {self.pantry_schema}.parent_uuids WHERE "
            "CONVERT(nvarchar(MAX), uuid) IN "
            "(SELECT CONVERT(nvarchar(MAX), value) "
            "FROM STRING_SPLIT(:uuids, ','))"
        )
        self.execute_sql(query, {"uuids": ','.join(uuids)}, commit=True)

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
            "(SELECT CONVERT(nvarchar(MAX), value) "
            "FROM STRING_SPLIT(:basket_address, ','))"
        )
        results, columns = self.execute_sql(
            query,
            {"basket_address": ','.join(basket_address)},
        )
        results = [list(row) for row in results]

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

        basket_uuid, _ = self.execute_sql(
            f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
            f"WHERE CONVERT(nvarchar(MAX), {id_column}) = :basket_address",
            {"basket_address": basket_address}
        )

        if basket_uuid is None or len(basket_uuid) == 0:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0][0]

        results, columns = self.execute_sql(
            f"""
            WITH child_record AS (
                SELECT
                    0 AS level,
                    CAST(:basket_uuid AS nvarchar(max)) AS id,
                    CAST(:basket_uuid AS nvarchar(max)) AS path
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
                AND child_record.level < :max_gen_level
            )
            SELECT pantry_index.*, child_record.level, child_record.path
            FROM {self.pantry_schema}.pantry_index as pantry_index
            JOIN child_record ON pantry_index.uuid = child_record.id
            ORDER BY child_record.level ASC OPTION (MAXRECURSION 0);
            """,
            {"basket_uuid": basket_uuid, "max_gen_level": max_gen_level}
        )

        results = [list(row) for row in results]
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

        basket_uuid, _ = self.execute_sql(
            f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
            f"WHERE CONVERT(nvarchar(MAX), {id_column}) = :basket_address",
            {"basket_address": basket_address}
        )

        if basket_uuid is None or len(basket_uuid) == 0:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0][0]

        results, columns = self.execute_sql(
            f"""
            WITH child_record AS (
                SELECT
                    0 AS level,
                    CAST(:basket_uuid AS nvarchar(max)) AS id,
                    CAST(:basket_uuid AS nvarchar(max)) AS path
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
                AND child_record.level > :min_gen_level
            )
            SELECT pantry_index.*, child_record.level, child_record.path
            FROM {self.pantry_schema}.pantry_index as pantry_index
            JOIN child_record ON pantry_index.uuid = child_record.id
            ORDER BY child_record.level DESC OPTION (MAXRECURSION 0);
            """,
            {"basket_uuid": basket_uuid, "min_gen_level": min_gen_level}
        )

        results = [list(row) for row in results]
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
        result, columns = self.execute_sql(
            f"SELECT TOP (:max_rows) * FROM {self.pantry_schema}.pantry_index "
            "WHERE CONVERT(nvarchar(MAX), basket_type) = :basket_type",
            {"max_rows": max_rows, "basket_type": basket_type},
        )
        result = [list(row) for row in result]

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
        result, columns = self.execute_sql(
            f"SELECT TOP (:max_rows) * FROM {self.pantry_schema}.pantry_index "
            "WHERE CONVERT(nvarchar(MAX), label) = :basket_label",
            {"max_rows": max_rows, "basket_label": basket_label},
        )
        result = [list(row) for row in result]

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
            results, columns = self.execute_sql(
                f"""SELECT TOP (:max_rows) *
                FROM {self.pantry_schema}.pantry_index
                WHERE upload_time >= :start_time AND upload_time <= :end_time
                """,
                {"max_rows": max_rows,
                 "start_time": start_time,
                 "end_time": end_time
                })
        elif start_time:
            start_time = int(datetime.timestamp(start_time))
            results, columns = self.execute_sql(
                f"""SELECT TOP (:max_rows) *
                FROM {self.pantry_schema}.pantry_index
                WHERE upload_time >= :start_time
                """, {"max_rows": max_rows, "start_time": start_time})
        elif end_time:
            end_time = int(datetime.timestamp(end_time))
            results, columns = self.execute_sql(
                f"""SELECT TOP (:max_rows) *
                FROM {self.pantry_schema}.pantry_index
                WHERE upload_time <= :end_time
                """, {"max_rows": max_rows, "end_time": end_time})

        results = [list(row) for row in results]

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
        **expr_args: dict (optional)
            Arguments to pass to the SQL expression. Should be in the form:
            {"arg_name": arg_value, ...}

        Returns
        ----------
        pandas.DataFrame of the resulting query.
        """
        expr_args = kwargs.get("expr_args", ())
        results, columns = self.execute_sql(expr, expr_args)
        results = [list(row) for row in results]
        return pd.DataFrame(results, columns=columns)

    def __len__(self):
        """Returns the number of baskets in the index."""
        results, _ = self.execute_sql(
                f"SELECT COUNT (*) FROM {self.pantry_schema}.pantry_index"
            )
        return results[0][0]

    def __str__(self):
        """Returns the str instantiation type of this Index (ie 'IndexSQL')."""
        return self.__class__.__name__
