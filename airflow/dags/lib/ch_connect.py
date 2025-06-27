from contextlib import contextmanager
from typing import Generator

from clickhouse_driver import Client as ClickhouseClient
from airflow.hooks.base import BaseHook

class CHConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, secure: bool = False):
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.secure = secure

    def client(self):
        return ClickhouseClient(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.pw,
            database=self.db_name,
            secure=self.secure
        )

    @contextmanager
    def connection(self) -> Generator[ClickhouseClient, None, None]:
        conn = self.client()
        try:
            yield conn
        finally:
            conn.disconnect()

class CHConnectionBuilder:
    @staticmethod
    def ch_conn(conn_id: str) -> CHConnect:
        conn = BaseHook.get_connection(conn_id)
        
        secure = False
        if "secure" in conn.extra_dejson:
            secure = conn.extra_dejson["secure"]

        return CHConnect(
            host=str(conn.host),
            port=str(conn.port),
            db_name=str(conn.schema),
            user=str(conn.login),
            pw=str(conn.password),
            secure=secure
        )