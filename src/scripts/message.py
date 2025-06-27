from src.scripts.pg_connect import PgConnectionBuilder

class Message: 
    def __init__(self, username, bookId, message):
        self.username = username
        self.bookId = bookId
        self.message = message
        self._userid = self.get_userid()
        self.db = PgConnectionBuilder.pg_conn("POSTGRES_DEFAULT")
    

    def get_userid(self) -> str:
        with self._db.client().cursor() as cur:
            cur.execute("SELECT id FROM user WHERE username = %(username)s", {"username": self._username})
            return cur.fetchone()[0]