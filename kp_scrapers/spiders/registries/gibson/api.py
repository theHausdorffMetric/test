import itertools
import logging
import os
import sqlite3


logger = logging.getLogger(__name__)


class VesselsDB:
    """Pretty API around SQLite DB interaction with CSV extracts.

    Exposes two public methods for appending/retrieving data using a reconstructed SQLite DB:
        - set_rows (INSERT)
        - get_rows (SELECT)

    """

    def __init__(self, path):
        self.db_conn = sqlite3.connect(path)
        # see https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory
        # allow key-based access to columns
        self.db_conn.row_factory = sqlite3.Row

        # initialise empty tables
        list(self._execute(self._sql_script('create-tables.sql')))

    def _execute(self, query, *params):
        try:
            with self.db_conn:
                cursor = self.db_conn.cursor()
                # detect presence of multiple sql statements in a single query
                # workaround `execute()` being unable to handle multiple queries at once
                if len([q for q in query.split(';') if q.strip()]) > 1:
                    _exe = cursor.executescript
                # if parameters are given, assume intent of substitution
                elif params:
                    _exe = cursor.executemany
                else:
                    _exe = cursor.execute

                for row in _exe(query, *params):
                    yield dict(row)

        except sqlite3.Error as err:
            logger.error(f'Unable to run query: {err}')
            return

    @staticmethod
    def _sql_script(script_name):
        # from the module path, build the sql script's path
        sql_path = os.path.join(os.path.dirname(__file__), 'query', script_name)
        with open(sql_path, 'rt') as script:
            return script.read().replace('\n', ' ')

    @staticmethod
    def _scrub(raw):
        # table names cannot be parametrized by sqlite, so we need to protect against injection
        return ''.join(char for char in raw if char.isalnum() or char in ('.', '_'))

    def set_rows(self, table_name, rows):
        """Insert rows into a specified table.

        Args:
            table_name (str): name of table
            rows (List[List(str)]): a list of rows to be inserted into the table

        """
        # assume `rows` is truthy
        values = ','.join(itertools.repeat('?', len(rows[0])))
        # exhaust generator to commit query
        list(self._execute(f'INSERT INTO {self._scrub(table_name)} VALUES ({values});', rows))

    def get_rows(self, query=None):
        """Get rows from a specified query.

        If custom query is not specified, will default to obtaining rows as specified in
        the sql script in this function (i.e. "retrieve-records.sql").

        Args:
            query (Optional[str]): string containing sql select statement

        Yields:
            Dict[str, str]: single row from sql select statement

        """
        query = query if query else self._sql_script('retrieve-records.sql')
        yield from self._execute(query)
