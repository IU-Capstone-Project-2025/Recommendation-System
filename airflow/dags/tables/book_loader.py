from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
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
    def insert_batch(self, conn: ClickhouseClient, books: List[BookObj]) -> None:
        if not books:
            return

        data = [
            [
                book.id,
                book.title,
                book.author,
                book.year,
                book.score,
                book.votes,
                book.imgurl,
                book.description,
                book.updatets
            ]
            for book in books
        ]
        
        conn.execute(
            """
            INSERT INTO Book (id, title, author, year, score, votes, imgurl, description, updatets) VALUES
            """,
            data
        )


class BookLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = BookOriginRepository(pg_origin)
        self.stg = BookDestRepository()
        self.log = log

    def load_book(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Book")[0][0]
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
