from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound


class Status:
    def __init__(self, username: str, bookId: int, status: str):
        self._username: str = username
        self._bookId = bookId
        self._newstatus = status
        self._db = PgConnectionBuilder.pg_conn()
        self._status = self.get_status()
        self._userid = self.get_userid(username)

    def get_userid(self, username: str | None = None) -> str:
        with self._db.client().cursor() as cur:
            cur.execute(
                "SELECT id FROM \"User\" WHERE username = %(username)s",
                {"username": username if username else self._username},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def get_status(self):
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
                {"userid": self.get_userid(self._username), "bookid": self._bookId},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def get_actuality(self):
        if self._newstatus in ["completed", "reading", "planned"]:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"""
                        SELECT isactual
                        FROM {self._newstatus} WHERE userid = %(userid)s AND bookid = %(bookid)s
                    """,
                    {
                        "userid": self._userid,
                        "bookid": self._bookId,
                    },
                )

                res = cur.fetchone()
                if not res: 
                    return None
                
                return res[0]

    def set_status(self):
        actuality = self.get_actuality()
        if actuality == True:
            return
        if actuality == False:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"""
                        UPDATE
                            {self._newstatus}
                        SET
                            isactual = true
                        WHERE
                            userid = %(userid)s AND bookid = %(bookid)s;
                    """,
                    {
                        "userid": self._userid,
                        "bookid": self._bookId,
                    },
                )
                cur.execute(
                    f"""
                        UPDATE
                            {self._status}
                        SET
                            isactual = false
                        WHERE
                            userid = %(userid)s AND bookid = %(bookid)s;
                    """,
                    {
                        "userid": self._userid,
                        "bookid": self._bookId,
                    },
                )
        if actuality is None and self._newstatus in ["completed", "reading", "planned"]:
            with self._db.client().cursor() as cur:
                cur.execute(
                    f"""
                        INSERT INTO
                            {self._newstatus} (userid, bookid, isactual)
                        VALUES
                            (%(userid)s, %(bookid)s, true);
                    """,
                    {
                        "userid": self._userid,
                        "bookid": self._bookId,
                    },
                )
                if self._status is not None:
                    cur.execute(
                        f"""
                            UPDATE
                                {self._status}
                            SET
                                isactual = false
                            WHERE
                                userid = %(userid)s AND bookid = %(bookid)s;
                        """,
                        {
                            "userid": self._userid,
                            "bookid": self._bookId,
                        },
                    )

