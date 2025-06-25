from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
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
    def insert_batch(self, conn: connection, booktype: List[BooktypeObj]) -> None:
        if not booktype:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_booktype
                (LIKE booktype INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_booktype (bookid, typeid, updatets) VALUES %s",
                [(t.bookid, t.typeid, t.updatets) for t in booktype]
            )
            
            cur.execute("""
                UPDATE booktype u SET
                    updatets = t.updatets
                FROM temp_booktype t
                WHERE u.bookid = t.bookid AND u.typeid = t.typeid
            """)
            
            cur.execute("""
                INSERT INTO booktype (bookid, typeid, updatets)
                SELECT t.bookid, t.typeid, t.updatets
                FROM temp_booktype t
                LEFT JOIN booktype u ON t.bookid = u.bookid AND t.typeid = u.typeid
                WHERE u.bookid IS NULL AND u.typeid IS NULL
            """)


class BooktypeLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = BooktypeOriginRepository(pg_origin)
        self.stg = BooktypeDestRepository()
        self.log = log

    def load_booktype(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM booktype")
                last_loaded_date = cursor.fetchone()[0]
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
