from src.scripts.pg_connect import PgConnectionBuilder

class BookMessages:
    def __init__(self, book_id: int):
        self.book_id = book_id
        self._db = PgConnectionBuilder.pg_conn()
    
    def get_book_comments(self, page: int = 0) -> list[tuple[str, str]]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT
                        u.username,
                        m.message
                    FROM message m
                    INNER JOIN "User" u ON m.userid = u.id
                    WHERE m.bookid = %(bookid)s
                    ORDER BY m.id DESC
                    OFFSET %(page)s * 5 LIMIT 5
                """,
                {"bookid": self.book_id, "page": page},
            )
            
            res = cur.fetchall()
            
            return res
