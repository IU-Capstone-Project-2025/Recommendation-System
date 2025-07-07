from src.scripts.pg_connect import PgConnectionBuilder


class UserStats:
    username: str

    def __init__(self, username: str):
        self.username = username
        self._db = PgConnectionBuilder.pg_conn()
        
    def get_userid(self) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                return None

            return res[0]
    
    def insert_user(self) -> None:
        with self._db.client().cursor() as cur:
            if self.get_userid() is None:
                cur.execute(
                    'INSERT INTO "User" (username) VALUES (%(username)s)',
                    {"username": self.username},
                )
