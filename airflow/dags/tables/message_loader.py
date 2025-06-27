from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
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
    def insert_batch(self, conn: ClickhouseClient, messages: List[MessageObj]) -> None:
        if not messages:
            return

        data = [
            [
                message.id,
                message.userid,
                message.bookid,
                message.message,
                message.isactual,
                message.updatets
            ]
            for message in messages
        ]
        
        conn.execute(
            """
            INSERT INTO Message (id, userid, bookid, message, isactual, updatets) VALUES
            """,
            data
        )


class MessageLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = MessageOriginRepository(pg_origin)
        self.stg = MessageDestRepository()
        self.log = log

    def load_message(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Message")[0][0]
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
