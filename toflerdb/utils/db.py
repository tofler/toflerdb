'''MySQL easy-access module

This module makes access to a MySQL database very simple.
It manages the connection pools, etc, converts the returned rows into
dictionaries

For update/delete queries, it returns information about
the number of rows affected.

For insert queries, it returns the lastrowid in case the
affected table contains an auto increment column
'''

import mysql.connector

IntegrityError = mysql.connector.errors.IntegrityError


class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    '''This is a utility class that converts the rows returned from a MySQL
    query into a list of dictionaries where each column becomes a key'''

    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None


class Database(object):

    def __init__(self, config, pool_name='toflerdb'):
        self._config = config
        self._pool_name = pool_name

    def execute_query(self, query, params=None, **kwargs):
        """Execute a specified query on the MySQL database.

        Args:
            query (str): The query to execute
            params (tuple): List of parameter values to match the number of
                placeholder items in the query

        Returns:
            list/dict: For select queries, returns a list of dictionaries
                       For update/delete/insert queries, returns a dictionary
                       containing *rows* (count of affected rows) and
                       *lastrowid* which contains value of any
                       auto_increment field in case of an insert
        """

        if params is not None and not isinstance(params, tuple) and not isinstance(params, dict):
            params = (params,)
        conn = mysql.connector.connect(
            pool_name=self._pool_name, **self._config)
        cursor = conn.cursor(cursor_class=MySQLCursorDict)
        try:
            cursor.execute(query, params, **kwargs)
        except Exception as e:
            conn.close()
            raise e
        try:
            # if an insert or update query was called, an exception will
            # result. In that case, we return the count of inserted/updated
            # rows and the lastrowid in case there is an auto-increment field
            results = cursor.fetchall()
            conn.commit()
        except:
            conn.commit()
            results = {'rows': cursor.rowcount, 'lastrowid': cursor.lastrowid}
        conn.close()
        return results
