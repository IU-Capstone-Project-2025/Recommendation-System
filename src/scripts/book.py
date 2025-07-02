from typing import Any, Tuple
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

    def __init__(
        self, id, title, author, year, avg_score, scores_count, cover, description
    ):
        self.id = id
        self.title = title
        self.author = author
        self.year = year
        self.avg_score = avg_score
        self.scores_count = scores_count
        self.cover = cover
        self.description = description


def book_from_row(row: Tuple[Any, ...]) -> Book:
    return Book(
        id=row[0],
        title=row[1],
        author=row[2],
        year=row[3],
        avg_score=row[4],
        scores_count=row[5],
        cover=row[6],
        description=row[7],
    )


class BookShort:
    id: int
    title: str
    author: str
    cover: str

    def __init__(self, id, title, author, cover):
        self.id = id
        self.title = title
        self.author = author
        self.cover = cover


def book_short_from_row(row: Tuple[Any, ...]) -> BookShort:
    return BookShort(id=row[0], title=row[1], author=row[2], cover=row[3])


def get_book_info(book_id):
    with PgConnectionBuilder.pg_conn().client().cursor() as cur:
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
            {"bookid": book_id},
        )

        res = cur.fetchone()
        if not res:
            raise ObjectNotFound

        return book_from_row(res)


def get_book_comments(book_id, page):
    with PgConnectionBuilder.pg_conn().client().cursor() as cur:
        cur.execute(
            """
                SELECT
                    u.username,
                    m.message
                FROM message m
                INNER JOIN "User" u ON m.userid = u.id
                WHERE m.bookid = %(bookid)s
                LIMIT 5 OFFSET %(page)s*5
            """,
            {"bookid": book_id, "page": page},
        )


def get_books(page):
    with PgConnectionBuilder.pg_conn().client().cursor() as cur:
        cur.execute(
            """
                SELECT
                    id,
                    title,
                    author,
                    imgurl
                FROM Book
                LIMIT 9 OFFSET %(page)s*9
            """,
            {"page": page},
        )
        return [book_short_from_row(row) for row in cur.fetchall()]


# TODO get list of books. I am waiting for the analysis result
# def get_list_of_books(self, page:int):
#      with self._db.client().cursor() as cur:
#         cur.execute(

#         )
