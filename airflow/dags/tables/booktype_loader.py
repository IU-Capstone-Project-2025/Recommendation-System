from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class BooktypeObj(BaseModel):
    bookid: int
    typeid: int
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'BooktypeObj':
        return cls(
            bookid=data[0],
            typeid=data[1],
            updatets=data[2]
        )


class BooktypeOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_booktype(self, booktype_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[BooktypeObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT bookid, typeid, updatets
                    FROM booktype
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, bookid ASC, typeid ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": booktype_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [BooktypeObj.from_dict(row) for row in rows]


class BooktypeDestRepository:
    def insert_batch(self, conn: ClickhouseClient, booktypes: List[BooktypeObj]) -> None:
        if not booktypes:
            return

        data = [
            [
                booktype.bookid,
                booktype.typeid,
                booktype.updatets
            ]
            for booktype in booktypes
        ]
        
        conn.execute(
            """
            INSERT INTO BookType (bookid, typeid, updatets) VALUES
            """,
            data
        )


class BooktypeLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = BooktypeOriginRepository(pg_origin)
        self.stg = BooktypeDestRepository()
        self.log = log

    def load_booktype(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM BookType")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            booktype_count = 0
            count = 1
            load_queue = self.origin.list_booktype(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} booktype to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                booktype_count += len(load_queue)
                load_queue = self.origin.list_booktype(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} booktype to load.")

            self.log.info(f"Load finished total updated booktype count: {booktype_count}.")
