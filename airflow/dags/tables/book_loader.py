from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from pydantic import BaseModel


class BookObj(BaseModel):
    id: int
    title: str
    author: Optional[str]
    year: Optional[int]
    score: Optional[float]
    votes: Optional[int]
    imgurl: Optional[str]
    description: Optional[str]
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'BookObj':
        return cls(
            id=data[0],
            title=data[1],
            author=data[2],
            year=data[3],
            score=data[4],
            votes=data[5],
            imgurl=data[6],
            description=data[7],
            updatets=data[8]
        )


class BookOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_book(self, book_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[BookObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, title, author, year, score, votes, imgurl, description, updatets
                    FROM book
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, id ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": book_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [BookObj.from_dict(row) for row in rows]


class BookDestRepository:
    def insert_batch(self, conn: connection, book: List[BookObj]) -> None:
        if not book:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_book
                (LIKE book INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_book (id, title, author, year, score, votes, imgurl, description, updatets) VALUES %s",
                [(t.id, t.title, t.author, t.year, t.score, t.votes, t.imgurl, t.description, t.updatets) for t in book]
            )
            
            cur.execute("""
                UPDATE book u SET
                    title = t.title,
                    author = t.author,
                    year = t.year,
                    score = t.score,
                    votes = t.votes,
                    imgurl = t.imgurl,
                    description = t.description,
                    updatets = t.updatets
                FROM temp_book t
                WHERE u.id = t.id
            """)
            
            cur.execute("""
                INSERT INTO book (id, title, author, year, score, votes, imgurl, description, updatets)
                SELECT t.id, t.title, t.author, t.year, t.score, t.votes, t.imgurl, t.description, t.updatets
                FROM temp_book t
                LEFT JOIN book u ON t.id = u.id
                WHERE u.id IS NULL
            """)


class BookLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = BookOriginRepository(pg_origin)
        self.stg = BookDestRepository()
        self.log = log

    def load_book(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM book")
                last_loaded_date = cursor.fetchone()[0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            book_count = 0
            count = 1
            load_queue = self.origin.list_book(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} book to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                book_count += len(load_queue)
                load_queue = self.origin.list_book(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} book to load.")

            self.log.info(f"Load finished total updated book count: {book_count}.")
