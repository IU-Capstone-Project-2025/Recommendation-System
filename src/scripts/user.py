from src.scripts.pg_connect import PgConnectionBuilder


class User:
    username: str

    def __init__(self, username: str):
        self.username = username
        self._db = PgConnectionBuilder.pg_conn()

    def get_id(self) -> int | None:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                return None

            return res[0]

    def insert(self) -> None:
        with self._db.client().cursor() as cur:
            if self.get_id() is None:
                cur.execute(
                    'INSERT INTO "User" (username) VALUES (%(username)s)',
                    {"username": self.username},
                )
