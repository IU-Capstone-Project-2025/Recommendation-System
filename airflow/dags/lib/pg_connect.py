from contextlib import contextmanager
from typing import Generator

import psycopg2
from airflow.hooks.base import BaseHook


class PgConnect:
    """
    A PostgreSQL database connection handler that manages connection parameters,
    connection creation, and provides context management for database sessions.
    """

    def __init__(
        self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: str
    ) -> None:
        """
        Initialize PostgreSQL connection parameters.

        Args:
            host: Database server hostname or IP address
            port: Database server port number
            db_name: Name of the database to connect to
            user: Username for authentication
            pw: Password for authentication
            sslmode: SSL mode for the connection (e.g., 'disable', 'require', 'verify-full')
        """
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        """
        Construct a PostgreSQL connection URL string with all parameters.

        Returns:
            str: Formatted connection string with all parameters including SSL mode
                 and target session attributes for read-write operations.
        """
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode,
        )

    def client(self) -> psycopg2.extensions.connection:
        """
        Create and return a new PostgreSQL database connection.

        Returns:
            psycopg2.extensions.connection: A new database connection object
        """
        return psycopg2.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Context manager for PostgreSQL database connections.

        Provides:
            - Automatic connection handling
            - Transaction management (commit on success, rollback on failure)
            - Guaranteed connection closure

        Yields:
            psycopg2.extensions.connection: An active database connection

        Raises:
            Exception: Propagates any exceptions that occur during the connection
        """
        conn = psycopg2.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class PgConnectionBuilder:
    """
    Factory class for creating PgConnect instances from Airflow connection IDs.
    """

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        """
        Create a PgConnect instance using parameters from an Airflow connection.

        Args:
            conn_id: The Airflow connection ID configured in the UI

        Returns:
            PgConnect: Configured PostgreSQL connection handler

        Note:
            - Extracts SSL mode from connection extras if specified
            - Defaults to 'disable' if SSL mode not specified
        """
        conn = BaseHook.get_connection(conn_id)

        # Get SSL mode from connection extras or default to 'disable'
        sslmode = "disable"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        return PgConnect(
            str(conn.host),
            str(conn.port),
            str(conn.schema),
            str(conn.login),
            str(conn.password),
            sslmode,
        )
