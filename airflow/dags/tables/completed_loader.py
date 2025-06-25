from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from pydantic import BaseModel


class CompletedObj(BaseModel):
    userid: int
    bookid: int
    isactual: bool
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'CompletedObj':
        return cls(
            userid=data[0],
            bookid=data[1],
            isactual=data[2],
            updatets=data[2]
        )


class CompletedOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_completed(self, completed_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[CompletedObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT userid, bookid, isactual, updatets
                    FROM completed
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, userid ASC, bookid ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": completed_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [CompletedObj.from_dict(row) for row in rows]


class CompletedDestRepository:
    def insert_batch(self, conn: connection, completed: List[CompletedObj]) -> None:
        if not completed:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_completed
                (LIKE completed INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_completed (userid, bookid, isactual, updatets) VALUES %s",
                [(t.userid, t.bookid, t.isactual, t.updatets) for t in completed]
            )
            
            cur.execute("""
                UPDATE completed u SET
                    isactual = t.isactual,
                    updatets = t.updatets
                FROM temp_completed t
                WHERE u.userid = t.userid AND u.bookid = t.bookid
            """)
            
            cur.execute("""
                INSERT INTO completed (userid, bookid, isactual, updatets)
                SELECT t.userid, t.bookid, t.isactual, t.updatets
                FROM temp_completed t
                LEFT JOIN completed u ON t.userid = u.userid AND t.bookid = u.bookid
                WHERE u.userid IS NULL AND u.bookid IS NULL
            """)


class CompletedLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CompletedOriginRepository(pg_origin)
        self.stg = CompletedDestRepository()
        self.log = log

    def load_completed(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM completed")
                last_loaded_date = cursor.fetchone()[0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            completed_count = 0
            count = 1
            load_queue = self.origin.list_completed(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} completed to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                completed_count += len(load_queue)
                load_queue = self.origin.list_completed(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} completed to load.")

            self.log.info(f"Load finished total updated completed count: {completed_count}.")
