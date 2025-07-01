from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class BookgenreObj(BaseModel):
    bookid: int
    genreid: int
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'BookgenreObj':
        return cls(
            bookid=data[0],
            genreid=data[1],
            updatets=data[2]
        )


class BookgenreOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_bookgenre(self, bookgenre_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[BookgenreObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT bookid, genreid, updatets
                    FROM bookgenre
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, bookid ASC, genreid ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": bookgenre_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [BookgenreObj.from_dict(row) for row in rows]


class BookgenreDestRepository:
    def insert_batch(self, conn: ClickhouseClient, bookgenres: List[BookgenreObj]) -> None:
        if not bookgenres:
            return

        data = [
            [
                bookgenre.bookid,
                bookgenre.genreid,
                bookgenre.updatets
            ]
            for bookgenre in bookgenres
        ]
        
        conn.execute(
            """
            INSERT INTO BookGenre (bookid, genreid, updatets) VALUES
            """,
            data
        )


class BookgenreLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = BookgenreOriginRepository(pg_origin)
        self.stg = BookgenreDestRepository()
        self.log = log

    def load_bookgenre(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM BookGenre")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            bookgenre_count = 0
            count = 1
            load_queue = self.origin.list_bookgenre(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} bookgenre to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                bookgenre_count += len(load_queue)
                load_queue = self.origin.list_bookgenre(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} bookgenre to load.")

            self.log.info(f"Load finished total updated bookgenre count: {bookgenre_count}.")
