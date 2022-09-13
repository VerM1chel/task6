import snowflake.connector
from airflow.hooks.dbapi import DbApiHook


class SnowflakeHook(DbApiHook):
    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'test_snowflake_connector'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)
        self.account = kwargs.pop("account", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.database = kwargs.pop("database", None)

    def _get_conn_params(self):
        """
        one method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.snowflake_conn_id)
        account = conn.extra_dejson.get('account', None)
        warehouse = conn.extra_dejson.get('warehouse', None)
        database = conn.extra_dejson.get('database', None)

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": conn.schema or '',
            "database": self.database or database or '',
            "account": self.account or account or '',
            "warehouse": self.warehouse or warehouse or ''
        }
        return conn_config

    def get_conn(self):
        conn_config = self._get_conn_params()
        conn = snowflake.connector.connect(**conn_config)
        return conn