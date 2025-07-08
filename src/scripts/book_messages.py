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
                    OFFSET %(page)s * 10 LIMIT 10
                """,
                {"bookid": self.book_id, "page": page},
            )

            res = cur.fetchall()

            return res

    def get_pages_count(self) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT COUNT(*) FROM message WHERE bookid = %(bookid)s
                """,
                {"bookid": self.book_id},
            )
            count: int = cur.fetchone()[0]  # pyright: ignore type
            return count // 10 + int(count % 10 > 0)
