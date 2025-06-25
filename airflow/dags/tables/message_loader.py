from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from pydantic import BaseModel


class MessageObj(BaseModel):
    id: int
    userid: int
    bookid: int
    message: str
    isactual: bool
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'MessageObj':
        return cls(
            id=data[0],
            userid=data[1],
            bookid=data[2],
            message=data[3],
            isactual=data[4],
            updatets=data[5]
        )


class MessageOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_message(self, message_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[MessageObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, userid, bookid, message, isactual, updatets
                    FROM message
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, id ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": message_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [MessageObj.from_dict(row) for row in rows]


class MessageDestRepository:
    def insert_batch(self, conn: connection, message: List[MessageObj]) -> None:
        if not message:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_message
                (LIKE message INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_message (id, userid, bookid, message, isactual, updatets) VALUES %s",
                [(t.id, t.userid, t.bookid, t.message, t.isactual, t.updatets) for t in message]
            )
            
            cur.execute("""
                UPDATE message u SET
                    userid = t.userid,
                    bookid = t.bookid,
                    message = t.message,
                    isactual = t.isactual,
                    updatets = t.updatets
                FROM temp_message t
                WHERE u.id = t.id
            """)
            
            cur.execute("""
                INSERT INTO message (id, userid, bookid, message, isactual, updatets)
                SELECT t.id, t.userid, t.bookid, t.message, t.isactual, t.updatets
                FROM temp_message t
                LEFT JOIN message u ON t.id = u.id
                WHERE u.id IS NULL
            """)


class MessageLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = MessageOriginRepository(pg_origin)
        self.stg = MessageDestRepository()
        self.log = log

    def load_message(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM message")
                last_loaded_date = cursor.fetchone()[0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            message_count = 0
            count = 1
            load_queue = self.origin.list_message(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} message to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                message_count += len(load_queue)
                load_queue = self.origin.list_message(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} message to load.")

            self.log.info(f"Load finished total updated message count: {message_count}.")
