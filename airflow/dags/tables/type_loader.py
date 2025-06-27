from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class TypeObj(BaseModel):
    id: int
    name: str
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'TypeObj':
        return cls(
            id=data[0],
            name=data[1],
            updatets=data[2]
        )


class TypeOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_type(self, type_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[TypeObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, name, updatets
                    FROM type
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, id ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": type_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [TypeObj.from_dict(row) for row in rows]


class TypeDestRepository:
    def insert_batch(self, conn: ClickhouseClient, types: List[TypeObj]) -> None:
        if not types:
            return

        data = [
            [
                type.id,
                type.name,
                type.updatets
            ]
            for type in types
        ]
        
        conn.execute(
            """
            INSERT INTO Type (id, name, updatets) VALUES
            """,
            data
        )


class TypeLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = TypeOriginRepository(pg_origin)
        self.stg = TypeDestRepository()
        self.log = log

    def load_type(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Type")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            type_count = 0
            count = 1
            load_queue = self.origin.list_type(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} type to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                type_count += len(load_queue)
                load_queue = self.origin.list_type(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} type to load.")

            self.log.info(f"Load finished total updated type count: {type_count}.")
