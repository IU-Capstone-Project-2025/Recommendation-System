from contextlib import contextmanager
from typing import Generator

from clickhouse_driver import Client as ClickhouseClient
from airflow.hooks.base import BaseHook


class CHConnect:
    """A class to manage ClickHouse database connections."""

    def __init__(
        self,
        host: str,
        port: str,
        db_name: str,
        user: str,
        pw: str,
        secure: bool = False,
    ):
        """
        Initialize ClickHouse connection parameters.

        Args:
            host: ClickHouse server hostname or IP address
            port: ClickHouse server port
            db_name: Database name to connect to
            user: Username for authentication
            pw: Password for authentication
            secure: Whether to use secure connection (SSL/TLS)
        """
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.secure = secure

    def client(self) -> ClickhouseClient:
        """
        Create and return a ClickHouse client instance.

        Returns:
            ClickhouseClient: A new ClickHouse client connection
        """
        return ClickhouseClient(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.pw,
            database=self.db_name,
            secure=self.secure,
        )

    @contextmanager
    def connection(self) -> Generator[ClickhouseClient, None, None]:
        """
        Context manager for ClickHouse connections.

        Yields:
            ClickhouseClient: An active ClickHouse connection

        Ensures:
            The connection is properly closed when the context exits
        """
        conn = self.client()
        try:
            yield conn
        finally:
            conn.disconnect()


class CHConnectionBuilder:
    """Factory class to build ClickHouse connections from Airflow connection IDs."""

    @staticmethod
    def ch_conn(conn_id: str) -> CHConnect:
        """
        Create a CHConnect instance from an Airflow connection ID.

        Args:
            conn_id: Airflow connection ID configured in the UI

        Returns:
            CHConnect: Configured ClickHouse connection helper

        Note:
            Looks for 'secure' parameter in the connection's extra JSON field
        """
        # Get Airflow connection details
        conn = BaseHook.get_connection(conn_id)

        # Check for secure connection flag in extras
        secure = False
        if "secure" in conn.extra_dejson:
            secure = conn.extra_dejson["secure"]

        return CHConnect(
            host=str(conn.host),
            port=str(conn.port),
            db_name=str(conn.schema),
            user=str(conn.login),
            pw=str(conn.password),
            secure=secure,
        )
