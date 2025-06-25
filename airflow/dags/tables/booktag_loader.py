from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from pydantic import BaseModel


class BooktagObj(BaseModel):
    bookid: int
    tagid: int
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'BooktagObj':
        return cls(
            bookid=data[0],
            tagid=data[1],
            updatets=data[2]
        )


class BooktagOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_booktag(self, booktag_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[BooktagObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT bookid, tagid, updatets
                    FROM booktag
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, bookid ASC, tagid ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": booktag_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [BooktagObj.from_dict(row) for row in rows]


class BooktagDestRepository:
    def insert_batch(self, conn: connection, booktag: List[BooktagObj]) -> None:
        if not booktag:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_booktag
                (LIKE booktag INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_booktag (bookid, tagid, updatets) VALUES %s",
                [(t.bookid, t.tagid, t.updatets) for t in booktag]
            )
            
            cur.execute("""
                UPDATE booktag u SET
                    updatets = t.updatets
                FROM temp_booktag t
                WHERE u.bookid = t.bookid AND u.tagid = t.tagid
            """)
            
            cur.execute("""
                INSERT INTO booktag (bookid, tagid, updatets)
                SELECT t.bookid, t.tagid, t.updatets
                FROM temp_booktag t
                LEFT JOIN booktag u ON t.bookid = u.bookid AND t.tagid = u.tagid
                WHERE u.bookid IS NULL AND u.tagid IS NULL
            """)


class BooktagLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = BooktagOriginRepository(pg_origin)
        self.stg = BooktagDestRepository()
        self.log = log

    def load_booktag(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM booktag")
                last_loaded_date = cursor.fetchone()[0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            booktag_count = 0
            count = 1
            load_queue = self.origin.list_booktag(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} booktag to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                booktag_count += len(load_queue)
                load_queue = self.origin.list_booktag(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} booktag to load.")

            self.log.info(f"Load finished total updated booktag count: {booktag_count}.")
