from src.scripts.pg_connect import PgConnectionBuilder

class Status:
    def __init__(self, username: str, bookId: int, status: str):
        self._username: str = username
        self._bookId = bookId
        self._newstatus = status
        self._status = self.get_status()
        self._userid = self.get_userid(username)
        self.db = PgConnectionBuilder.pg_conn("POSTGRES_DEFAULT")
    
    def get_userid(self) -> str:
        with self._db.client().cursor() as cur:
            cur.execute("SELECT id FROM user WHERE username = %(username)s", {"username": self._username})
            return cur.fetchone()[0]
    
    def get_status(self):
        with self._db.client().cursor() as cur:
            cur.execute("""
                            SELECT
                                CASE
                                    WHEN EXISTS (SELECT 1 FROM completed WHERE userid = %(userid)s AND bookid = %(bookid)s AND isactual = true) THEN 'completed'
                                    WHEN EXISTS (SELECT 1 FROM reading WHERE userid = %(userid)s AND bookid = %(bookid)s AND isactual = true) THEN 'reading'
                                    WHEN EXISTS (SELECT 1 FROM planned WHERE userid = %(userid)s AND bookid = %(bookid)s AND isactual = true) THEN 'planned'
                                    ELSE NULL
                                END AS status
                        """, {"userid": self._userid, "bookid": self._bookId})
            return cur.fetchone()[0]
        
    def get_actuality(self):
        if self._newstatus in ["completed", "reading", "planned"]:
            with self._db.client().cursor() as cur:
                cur.execute("""
                                SELECT isactual
                                FROM %(status)s WHERE userid = %(userid)s AND bookid = %(bookid)s
                            """, {"status": self._newstatus, "userid": self._userid, "bookid": self._bookId})
                return cur.fetchone()[0]

    def set_status(self):
        actuality = self.get_actuality()
        if actuality == True:
            return
        if actuality == False:
            with self._db.client().cursor() as cur:
                cur.execute("""
                                UPDATE
                                    %(status)s
                                SET
                                    isactual = true
                                WHERE
                                    userid = %(userid)s AND bookid = %(bookid)s;
                            """, {"status": self._newstatus, "userid": self._userid, "bookid": self._bookId})
                cur.execute("""
                                UPDATE
                                    %(status)s
                                SET
                                    isactual = false
                                WHERE
                                    userid = %(userid)s AND bookid = %(bookid)s;
                            """, {"status": self._status, "userid": self._userid, "bookid": self._bookId})
        if actuality is None and self._newstatus in ["completed", "reading", "planned"]:
            with self._db.client().cursor() as cur:
                cur.execute("""
                                INSERT INTO
                                    %(status)s (userid, bookid, isactual)
                                VALUES
                                    (%(userid)s, %(bookid)s, true);
                            """, {"status": self._newstatus, "userid": self._userid, "bookid": self._bookId})
                if self._status is not None:
                    cur.execute("""
                                    UPDATE
                                        %(status)s
                                    SET
                                        isactual = false
                                    WHERE
                                        userid = %(userid)s AND bookid = %(bookid)s;
                                """, {"status": self._status, "userid": self._userid, "bookid": self._bookId})