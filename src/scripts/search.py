from src.scripts.exceptions import ObjectNotFound
from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.book import Book


class Search:
    def __init__(self, titles: list[str]):
        self.titles = titles
        self._db = PgConnectionBuilder.pg_conn()

    def get_search_result(self) -> list[Book]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT
                        id
                    FROM Book b
                    WHERE title = ANY(%(titles)s)
                """,
                {"titles": self.titles},
            )

            res = cur.fetchall()

            return [Book(row[0]) for row in res]