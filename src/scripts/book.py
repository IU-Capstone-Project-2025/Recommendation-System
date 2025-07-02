from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound

class Book:
    def __init__(self, bookId):
        self._bookid = bookId
        self._db = PgConnectionBuilder.pg_conn()
    
    def get_book_info(self):
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
                {"bookid": self._bookid},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return (
                res[0],
                res[1],
                res[2],
                res[3],
                res[4],
                res[5],
                res[6],
            )
    
    def get_book_comments(self,page):
        with self._db.client().cursor() as cur:
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
                {"bookid": self._bookid, "page": page},
            )
    # TODO get list of books. I am waiting for the analysis result
    # def get_list_of_books(self, page:int):
    #      with self._db.client().cursor() as cur:
    #         cur.execute(

    #         )
        