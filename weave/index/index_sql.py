"""Wherein is contained the concrete SQLite implementation of the Index."""
# Pylint doesn't like the similarity between this file and the SQLite file, but
# it doesn't make sense to write shared functions for them. So ignore pylint.
# pylint: disable=duplicate-code
import os
import warnings
from datetime import datetime

import ast
import pandas as pd
# Try-Except required to make psycopg2/sqlalchemy an optional dependency.
try:
    # pylint: disable=unused-import
    import importlib
    assert importlib.util.find_spec('psycopg2')
    import sqlalchemy as sqla # noqa: F401
    # pylint: enable=unused-import
except ImportError:
    _HAS_REQUIRED_DEPS = False
except AssertionError:
    _HAS_REQUIRED_DEPS = False
else:
    _HAS_REQUIRED_DEPS = True

from .index_abc import IndexABC
from .create_index import create_index_from_fs

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
            DB to be used. If none is set, the environment variable
            WEAVE_SQL_DB_NAME is check. If not set, default to weave_db.
        **pantry_schema: str (default=<pantry_path>)
            The schema to use for the pantry. If none is set, defaults to the
            pantry path (with _ replacements when necessary).
        """
        if not _HAS_REQUIRED_DEPS:
            raise ImportError("Missing Dependencies. The packages: 'psycopg2'"
                              "'sqlalchemy' are required to use this class")

        # Check that the required environment variables are set.
        try:
            # We want to fail early if these are not set, so we check them here
            # even though they are not used until we connect.
            # Pylint thinks this is pointless, so pylint is getting ignored.
            # pylint: disable=pointless-statement
            self._sql_connection = {
                'host': os.environ["WEAVE_SQL_HOST"],
                'username': os.environ["WEAVE_SQL_USERNAME"],
                'password': os.environ["WEAVE_SQL_PASSWORD"],
                'port': os.environ.get("WEAVE_SQL_PORT", 5432),
            }
            # pylint: enable=pointless-statement
        except KeyError as key_error:
            raise KeyError("The following environment variables must be set "
                           "to use this class: WEAVE_SQL_HOST, "
                           "WEAVE_SQL_USERNAME, WEAVE_SQL_PASSWORD."
                          ) from key_error

        self._file_system = file_system
        self._pantry_path = pantry_path

        # Set the database name (defaults to weave_db). DATABASE MUST ALREADY
        # EXIST. If it does not, the user must create it manually.
        self._database_name = kwargs.get(
            "database_name",
            os.environ.get("WEAVE_SQL_DB_NAME", "weave_db"),
        )
        self._database_name = self._database_name.replace("-", "_")

        # Set the schema name (defaults to pantry_path). If the schema does not
        # exist, it will be created.
        d_schema_name = self._pantry_path.replace(os.sep, "_")\
            .replace("-", "_")
        if d_schema_name == "":
            d_schema_name = "weave"
        self._pantry_schema = kwargs.get("pantry_schema", d_schema_name)

        self._engine = sqla.create_engine(sqla.engine.url.URL(
            drivername="postgresql",
            username=self._sql_connection['username'],
            password=self._sql_connection['password'],
            host=self._sql_connection['host'],
            database=self.database_name,
            query={},
            port=self._sql_connection['port'],
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
        with self._engine.connect() as connection:
            if not connection.dialect.has_schema(connection,
                                                 self.pantry_schema):
                self.execute_sql(f"CREATE SCHEMA {self.pantry_schema};",
                                commit=True)

    def _create_tables(self):
        """Create the required tables if they do not already exist."""
        # THIS NEEDS TO BE UPDATED MANUALLY IF NEW COLUMNS ARE ADDED TO INDEX.
        # THE INSERT IN OTHER FUNCTIONS USE config.index_schema(), BUT THAT
        # CAN'T BE USED HERE AS TYPE NEEDS TO BE SPECIFIED.
        if not sqla.inspect(self._engine).has_table("pantry_index",
                                                    self.pantry_schema):
            self.execute_sql(
                f"""
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
        if not sqla.inspect(self._engine).has_table("parent_uuids",
                                                    self.pantry_schema):
            self.execute_sql(
                f"""
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
            "database_host": self._sql_connection['host'],
            "database_port": self._sql_connection['port'],
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

        basket_jsons = [x for x in self.file_system.find(self.pantry_path)
            if x.endswith("basket_manifest.json")
        ]

        for basket_json_address in basket_jsons:
            entry = create_index_from_fs(basket_json_address,
                                         file_system=self.file_system)
            if len(self.get_rows(entry['uuid'].iloc[0])) == 0:
                self.track_basket(entry)

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
        # Get the rows from the index as a list of lists, then get the columns.
        result, columns = self.execute_sql(
             f"""SELECT *
                 FROM {self.pantry_schema}.pantry_index
                 ORDER BY UUID
                 OFFSET (:offset) ROWS
                 FETCH FIRST (:max_rows) ROWS ONLY""",
            {"offset": offset, "max_rows": max_rows},
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
        # Insert the parent_uuids.
        sql = sqla.text(
            f"INSERT INTO {self.pantry_schema}.parent_uuids "
            "(uuid, parent_uuid) "
            "SELECT :uuid, :parent_uuid "
            "WHERE NOT EXISTS "
            f"(SELECT 1 FROM {self.pantry_schema}.parent_uuids "
            "WHERE uuid = CAST(:uuid AS text) "
            "AND parent_uuid = CAST(:parent_uuid AS text));"
        )

        # Loop all uuids and parent uuids (list of lists).
        for _, entry in entry_df[["uuid", "parent_uuids"]].iterrows():
            # Loop all parent uuids (now a list of strings)
            for parent_uuid in entry.parent_uuids:
                self.execute_sql(
                    sql,
                    {
                        "uuid": entry.uuid,
                        "parent_uuid": parent_uuid,
                    },
                    commit=True,
                )

        # Convert the parent_uuids to a string, and the upload_time to an int.
        entry_df.loc[:,"parent_uuids"] = entry_df["parent_uuids"].astype(str)
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
                "WHERE uuid = CAST(:uuid AS text));"
            )
            self.execute_sql(sql, basket_dict, commit=True)

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
            uuids, _ = self.execute_sql(
                sqla.text(
                    f"SELECT uuid FROM {self.pantry_schema}.pantry_index "
                    f"WHERE address IN (:basket_address)"
                ),
                {"basket_address": ','.join(basket_address)}
            )
            uuids = [uuid[0] for uuid in uuids]
        else:
            uuids = basket_address

        # Delete from pantry_index.
        query = sqla.text(
            f"DELETE FROM {self.pantry_schema}.pantry_index "
            " WHERE uuid IN ( "
                " SELECT unnest(CAST(:uuids AS text[]))"
            ");"
        )
        rowcount = self.execute_sql(
            query,
            {"uuids": uuids},
            commit=True
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
            f"DELETE FROM {self.pantry_schema}.parent_uuids "
            " WHERE uuid IN ( "
                " SELECT unnest(CAST(:uuids AS text[]))"
            ");"
        )
        self.execute_sql(query, {"uuids": uuids}, commit=True)

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

        baskets = [f"'{e}'" for e in basket_address]
        query = (
            f"SELECT * FROM {self.pantry_schema}.pantry_index "
            f"WHERE {id_column} IN ({','.join(baskets)});"
        )
        results, columns = self.execute_sql(query,)
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
            f"WHERE {id_column} = :basket_address",
            {"basket_address": basket_address, "id_column": id_column}
        )

        if basket_uuid is None or len(basket_uuid) == 0:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0][0]

        results, columns = self.execute_sql(
            f"""
            WITH RECURSIVE child_record AS (
                SELECT
                    0 AS level,
                    CAST(:basket_uuid AS varchar) AS id,
                    CAST(:basket_uuid AS varchar) AS path
                UNION ALL
                SELECT
                    child_record.level + 1,
                    CAST(parent_uuids.parent_uuid AS varchar) AS id,
                    CAST(child_record.path || '/' || parent_uuids.parent_uuid
                        AS varchar) AS path
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
            ORDER BY child_record.level ASC;
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
            f"WHERE {id_column} = :basket_address",
            {"basket_address": basket_address, "id_column": id_column}
        )

        if basket_uuid is None or len(basket_uuid) == 0:
            raise FileNotFoundError(
                f"basket path or uuid does not exist '{basket_address}'"
            )
        basket_uuid = basket_uuid[0][0]

        results, columns = self.execute_sql(
            f"""
            WITH RECURSIVE child_record AS (
                SELECT
                    0 AS level,
                    CAST(:basket_uuid AS varchar) AS id,
                    CAST(:basket_uuid AS varchar) AS path
                UNION ALL
                SELECT 
                    child_record.level - 1,
                    CAST(parent_uuids.uuid AS varchar) AS id,
                    CAST(child_record.path || '/' || parent_uuids.uuid
                        AS varchar) AS path
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
            ORDER BY child_record.level DESC;
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
        result, columns = self.execute_sql(
            f"""SELECT *
                FROM {self.pantry_schema}.pantry_index
                WHERE basket_type = :basket_type
                ORDER BY UUID
                OFFSET (:offset) ROWS
                FETCH FIRST (:max_rows) ROWS ONLY""",
            {"offset": offset, "max_rows": max_rows,
             "basket_type": basket_type},
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
        result, columns = self.execute_sql(
            f"""SELECT *
                FROM {self.pantry_schema}.pantry_index
                WHERE label = :basket_label
                ORDER BY UUID
                OFFSET (:offset) ROWS
                FETCH FIRST (:max_rows) ROWS ONLY""",
            {"offset": offset, "max_rows": max_rows,
             "basket_label": basket_label},
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

        pre_query = f"SELECT * FROM {self.pantry_schema}.pantry_index "
        post_query = """ORDER BY UUID
                        OFFSET (:offset) ROWS
                        FETCH FIRST (:max_rows) ROWS ONLY"""

        if start_time and end_time:
            start_time = int(datetime.timestamp(start_time))
            end_time = int(datetime.timestamp(end_time))
            query = "WHERE upload_time >= :start_time " \
                    "AND upload_time <= :end_time "
            results, columns = self.execute_sql(
                pre_query + query + post_query,
                {"offset": offset,
                 "max_rows": max_rows,
                 "start_time": start_time,
                 "end_time": end_time
                })
        elif start_time:
            start_time = int(datetime.timestamp(start_time))
            query = "WHERE upload_time >= :start_time "
            results, columns = self.execute_sql(
                pre_query + query + post_query,
                {"offset": offset,
                 "max_rows": max_rows,
                 "start_time": start_time,
                })
        elif end_time:
            end_time = int(datetime.timestamp(end_time))
            query = "WHERE upload_time <= :end_time "
            results, columns = self.execute_sql(
                pre_query + query + post_query,
                {"offset": offset,
                 "max_rows": max_rows,
                 "end_time": end_time,
                })

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
