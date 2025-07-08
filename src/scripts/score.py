from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound


class Score:
    username: str
    bookId: int
    newscore: int | None
    score: int | None
    userid: int

    def __init__(self, username: str, bookId: int, score: int | None = None):
        self.username = username
        self.bookId = bookId
        self.newscore = score
        self._db = PgConnectionBuilder.pg_conn()
        self.userid = self.get_userid()
        self.score = self.get_score()

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

    def get_score(self) -> int | None:
        with self._db.client().cursor() as cur:
            cur.execute(
                "SELECT score FROM score WHERE userid = %(userid)s AND bookid = %(bookid)s",
                {"userid": self.userid, "bookid": self.bookId},
            )

            res = cur.fetchone()
            if not res:
                return None

            return res[0]

    def set_score(self) -> None:
        if self.get_score():
            client = self._db.client()
            with client.cursor() as cur:
                cur.execute(
                    "UPDATE score SET score = %(score)s, updatets = NOW() WHERE userid = %(userid)s AND bookid = %(bookid)s",
                    {
                        "userid": self.userid,
                        "bookid": self.bookId,
                        "score": self.newscore,
                    },
                )
                client.commit()
        else:
            client = self._db.client()
            with client.cursor() as cur:
                cur.execute(
                    "INSERT INTO score (userid, bookid, score) VALUES (%(userid)s, %(bookid)s, %(score)s)",
                    {
                        "userid": self.userid,
                        "bookid": self.bookId,
                        "score": self.newscore,
                    },
                )
                client.commit()

    def drop_score(self) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                "UPDATE score SET isactual = false WHERE userid = %(userid)s AND bookid = %(bookid)s",
                {"userid": self.userid, "bookid": self.bookId},
            )
