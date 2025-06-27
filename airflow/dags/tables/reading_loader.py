from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class ReadingObj(BaseModel):
    userid: int
    bookid: int
    isactual: bool
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'ReadingObj':
        return cls(
            userid=data[0],
            bookid=data[1],
            isactual=data[2],
            updatets=data[2]
        )


class ReadingOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reading(self, reading_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[ReadingObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT userid, bookid, isactual, updatets
                    FROM reading
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, userid ASC, bookid ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": reading_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [ReadingObj.from_dict(row) for row in rows]


class ReadingDestRepository:
    def insert_batch(self, conn: ClickhouseClient, readings: List[ReadingObj]) -> None:
        if not readings:
            return

        data = [
            [
                reading.userid,
                reading.bookid,
                reading.isactual,
                reading.updatets
            ]
            for reading in readings
        ]
        
        conn.execute(
            """
            INSERT INTO Reading (userid, bookid, isactual, updatets) VALUES
            """,
            data
        )


class ReadingLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = ReadingOriginRepository(pg_origin)
        self.stg = ReadingDestRepository()
        self.log = log

    def load_reading(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Reading")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            reading_count = 0
            count = 1
            load_queue = self.origin.list_reading(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} reading to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                reading_count += len(load_queue)
                load_queue = self.origin.list_reading(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} reading to load.")

            self.log.info(f"Load finished total updated reading count: {reading_count}.")
