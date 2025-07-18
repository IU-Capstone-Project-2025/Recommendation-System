from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound


class Book:
    id: int
    title: str
    author: str
    year: int
    avg_score: float
    scores_count: int
    cover: str
    description: str

    def __init__(self, book_id: int):
        self.id = book_id
        self._db = PgConnectionBuilder.pg_conn()
        self.book_data = self.get_book_info()
        self.title = self.book_data[0]
        self.author = self.book_data[1]
        self.year = self.book_data[2]
        self.avg_score = self.book_data[3]
        self.scores_count = self.book_data[4]
        self.cover = self.book_data[5]
        self.description = self.book_data[6]

    def get_book_info(self) -> tuple[str, str, int, float, int, str, str]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT
                        title,
                        author,
                        year,
                        AVG(s.score),
                        COUNT(s.score),
                        imgurl,
                        description
                    FROM Book b
                    LEFT JOIN Score s ON b.id = s.bookid
                    WHERE b.id = %(bookid)s
                    GROUP BY title, author, year, imgurl, description
                """,
                {"bookid": self.id},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res
