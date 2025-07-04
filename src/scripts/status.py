from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound
from src.constants import COMPLETED, READING, PLANNED


class Status:
    username: str
    bookId: int
    newstatus: str | None
    status: str | None
    userid: int
    
    def __init__(self, username: str, bookId: int, status: str | None):
        self.username = username
        self.bookId = bookId
        self.newstatus = status
        self._db = PgConnectionBuilder.pg_conn()
        self.status = self.get_status()
        self.userid = self.get_userid()

    def get_userid(self) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                "SELECT id FROM \"User\" WHERE username = %(username)s",
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def get_status(self) -> str | None:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT
                        CASE
                            WHEN EXISTS (SELECT 1 FROM completed WHERE userid = %(userid)s AND bookid = %(bookid)s AND isactual = true) THEN 'completed'
                            WHEN EXISTS (SELECT 1 FROM reading WHERE userid = %(userid)s AND bookid = %(bookid)s AND isactual = true) THEN 'reading'
                            WHEN EXISTS (SELECT 1 FROM planned WHERE userid = %(userid)s AND bookid = %(bookid)s AND isactual = true) THEN 'planned'
                            ELSE NULL
                        END AS status
                """,
                {"userid": self.get_userid(self.username), "bookid": self.bookId},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def get_actuality(self) -> bool | None:
        if self.newstatus in [COMPLETED, READING, PLANNED]:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"""
                        SELECT isactual
                        FROM {self.newstatus} WHERE userid = %(userid)s AND bookid = %(bookid)s
                    """,
                    {
                        "userid": self.userid,
                        "bookid": self.bookId,
                    },
                )

                res = cur.fetchone()
                if not res: 
                    return None

                return res[0]

    def set_status(self) -> None:
        actuality = self.get_actuality()
        if actuality == True:
            return
        if actuality == False:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"""
                        UPDATE
                            {self.newstatus}
                        SET
                            isactual = true
                        WHERE
                            userid = %(userid)s AND bookid = %(bookid)s;
                    """,
                    {
                        "userid": self.userid,
                        "bookid": self.bookId,
                    },
                )
                cur.execute(
                    f"""
                        UPDATE
                            {self.status}
                        SET
                            isactual = false
                        WHERE
                            userid = %(userid)s AND bookid = %(bookid)s;
                    """,
                    {
                        "userid": self.userid,
                        "bookid": self.bookId,
                    },
                )
        if actuality is None and self.newstatus in [COMPLETED, READING, PLANNED]:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"""
                        INSERT INTO
                            {self.newstatus} (userid, bookid, isactual)
                        VALUES
                            (%(userid)s, %(bookid)s, true);
                    """,
                    {
                        "userid": self.userid,
                        "bookid": self.bookId,
                    },
                )
                if self.status is not None:
                    cur.execute(
                        f"""
                            UPDATE
                                {self.status}
                            SET
                                isactual = false
                            WHERE
                                userid = %(userid)s AND bookid = %(bookid)s;
                        """,
                        {
                            "userid": self.userid,
                            "bookid": self.bookId,
                        },
                    )

    def drop_status(self) -> None:
        if self.status in [COMPLETED, READING, PLANNED]:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"UPDATE {self.status} SET isactual = false WHERE userid = %(userid)s AND bookid = %(bookid)s",
                    {"userid": self.userid, "bookid": self.bookId},
                )
