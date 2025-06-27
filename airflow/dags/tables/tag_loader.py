from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class TagObj(BaseModel):
    id: int
    name: str
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'TagObj':
        return cls(
            id=data[0],
            name=data[1],
            updatets=data[2]
        )


class TagOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_tag(self, tag_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[TagObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, name, updatets
                    FROM tag
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, id ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": tag_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [TagObj.from_dict(row) for row in rows]


class TagDestRepository:
    def insert_batch(self, conn: ClickhouseClient, tags: List[TagObj]) -> None:
        if not tags:
            return

        data = [
            [
                tag.id,
                tag.name,
                tag.updatets
            ]
            for tag in tags
        ]
        
        conn.execute(
            """
            INSERT INTO Tag (id, name, updatets) VALUES
            """,
            data
        )


class TagLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = TagOriginRepository(pg_origin)
        self.stg = TagDestRepository()
        self.log = log

    def load_tag(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Tag")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            tag_count = 0
            count = 1
            load_queue = self.origin.list_tag(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} tag to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                tag_count += len(load_queue)
                load_queue = self.origin.list_tag(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} tag to load.")

            self.log.info(f"Load finished total updated tag count: {tag_count}.")
