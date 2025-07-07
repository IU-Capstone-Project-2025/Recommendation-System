from src.scripts.exceptions import ObjectNotFound, WrongListType
from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.ch_connect import CHConnectionBuilder
from src.scripts.book import Book
from src.constants import TOP_LIST, WEEKLY_TOP_LIST, RECOMMEND_LIST


class BookList:
    def __init__(self, username: str | None, list_type: str):
        self.username = username
        self.list_type = list_type
        self.db = PgConnectionBuilder.pg_conn()
        self.list_db = CHConnectionBuilder.ch_conn()
        self.userid = self.get_userid()

    def get_userid(self) -> int | None:
        with self.db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                return None

            return res[0]

    def get_book_list(self, page: int = 0) -> list[Book]:
        with self.list_db.client() as cur:
            if self.list_type in [TOP_LIST, WEEKLY_TOP_LIST]:
                offset = page * 9
                res = cur.execute(
                    f"SELECT bookid FROM {self.list_type} FINAL ORDER BY rank ASC LIMIT %(offset)s, 9",
                    {"offset": offset},
                )

                if not res:
                    raise ObjectNotFound

                return [Book(row[0]) for row in res]
            else:
                raise WrongListType

    def get_recommendation_book_list(self, page: int = 0) -> list[Book]:
        with self.list_db.client() as cur:
            if self.list_type in [RECOMMEND_LIST]:
                offset = page * 9
                res = cur.execute(
                    f"SELECT bookid FROM {self.list_type} FINAL WHERE userid = %(userid)s ORDER BY rank ASC LIMIT %(offset)s, 9",
                    {"userid": self.userid, "offset": offset},
                )

                if not res:
                    raise ObjectNotFound

                return [Book(row[0]) for row in res]
            else:
                raise WrongListType
