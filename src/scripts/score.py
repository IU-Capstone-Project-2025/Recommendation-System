from src.scripts.pg_connect import PgConnectionBuilder

class Score:
    def __init__(self, username: str, bookId: int, score: int):
        self._username: str = username
        self._bookId = bookId
        self._score = score
        self._userid = self.get_userid(username)
        self.db = PgConnectionBuilder.pg_conn("POSTGRES_DEFAULT")
    
    def get_userid(self) -> str:
        with self._db.client().cursor() as cur:
            cur.execute("SELECT id FROM user WHERE username = %(username)s", {"username": self._username})
            return cur.fetchone()[0]
    
    def get_score(self):
        with self._db.client().cursor() as cur:
            cur.execute("SELECT score FROM score WHERE userid = %(userid)s AND bookid = %(bookid)s", {"userid": self._userid, "bookid": self._bookId})
            return cur.fetchone()[0]

    def set_score(self):
        if self.get_score():
            with self._db.client().cursor() as cur:
                cur.execute("UPDATE score SET score = %(score)s, updatets = NOW() WHERE userid = %(userid)s AND bookid = %(bookid)s", {"userid": self._userid, "bookid": self._bookId, "score": self._score})
        else:
            with self._db.client().cursor() as cur:
                cur.execute("INSERT INTO score (userid, bookid, score) VALUES (%(userid)s, %(bookid)s, %(score)s)", {"userid": self._userid, "bookid": self._bookId, "score": self._score})
    
    def drop_score(self):
        with self._db.client().cursor() as cur:
            cur.execute("UPDATE score SET isactual = false WHERE userid = %(userid)s AND bookid = %(bookid)s", {"userid": self._userid, "bookid": self._bookId})

        