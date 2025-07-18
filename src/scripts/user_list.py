from src.scripts.exceptions import ObjectNotFound
from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.book import Book
from src.scripts.score import Score


class UserList:
    def __init__(self, username: str):
        self.username = username
        self._db = PgConnectionBuilder.pg_conn()
        self.userid = self.get_userid()

    def get_userid(self) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def get_completed_list(self) -> list[tuple[Book, Score]]:
        with self._db.client().cursor() as cur:
            cur.execute(
                f"SELECT bookid FROM completed WHERE userid = %(userid)s AND isactual = true ORDER BY bookid ASC",
                {"userid": self.userid},
            )

            res = cur.fetchall()

            return [(Book(row[0]), Score(self.username, row[0])) for row in res]

    def get_planned_list(self) -> list[tuple[Book, Score]]:
        with self._db.client().cursor() as cur:
            cur.execute(
                f"SELECT bookid FROM planned WHERE userid = %(userid)s AND isactual = true ORDER BY bookid ASC",
                {"userid": self.userid},
            )

            res = cur.fetchall()

            return [(Book(row[0]), Score(self.username, row[0])) for row in res]

    def get_reading_list(self) -> list[tuple[Book, Score]]:
        with self._db.client().cursor() as cur:
            cur.execute(
                f"SELECT bookid FROM reading WHERE userid = %(userid)s AND isactual = true ORDER BY bookid ASC",
                {"userid": self.userid},
            )

            res = cur.fetchall()

            return [(Book(row[0]), Score(self.username, row[0])) for row in res]
