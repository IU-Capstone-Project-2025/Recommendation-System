from src.scripts.exceptions import ObjectNotFound
from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.book import Book


class Search:
    def __init__(self, titles: list[str]):
        self.titles = titles
        self._db = PgConnectionBuilder.pg_conn()

    def get_search_result(self) -> list[Book]:
        with self._db.client().cursor() as cur:
            books = []
            for i in range(len(self.titles)):
                cur.execute(
                    """
                        SELECT
                            id
                        FROM Book b
                        WHERE title = %(title)s
                    """,
                    {"title": self.titles[i]},
                )

                res = cur.fetchone()
                if not res:
                    continue

                books.append(Book(res[0]))

            return books
