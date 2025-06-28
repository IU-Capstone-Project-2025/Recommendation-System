from src.scripts.exceptions import ObjectNotFound
from src.scripts.pg_connect import PgConnectionBuilder


class Message:
    def __init__(self, username, bookId, message):
        self.username = username
        self.bookId = bookId
        self.message = message
        self.userid = self.get_userid()
        self.db = PgConnectionBuilder.pg_conn()

    def get_userid(self) -> str:
        with self.db.client().cursor() as cur:
            cur.execute(
                "SELECT id FROM user WHERE username = %(username)s",
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]
