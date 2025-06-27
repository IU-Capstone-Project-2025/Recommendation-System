from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class GenreObj(BaseModel):
    id: int
    name: str
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'GenreObj':
        return cls(
            id=data[0],
            name=data[1],
            updatets=data[2]
        )


class GenreOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_genre(self, genre_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[GenreObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, name, updatets
                    FROM genre
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, id ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": genre_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [GenreObj.from_dict(row) for row in rows]


class GenreDestRepository:
    def insert_batch(self, conn: ClickhouseClient, genres: List[GenreObj]) -> None:
        if not genres:
            return

        data = [
            [
                genre.id,
                genre.name,
                genre.updatets
            ]
            for genre in genres
        ]
        
        conn.execute(
            """
            INSERT INTO Genre (id, name, updatets) VALUES
            """,
            data
        )


class GenreLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = GenreOriginRepository(pg_origin)
        self.stg = GenreDestRepository()
        self.log = log

    def load_genre(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Genre")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            genre_count = 0
            count = 1
            load_queue = self.origin.list_genre(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} genre to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                genre_count += len(load_queue)
                load_queue = self.origin.list_genre(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} genre to load.")

            self.log.info(f"Load finished total updated genre count: {genre_count}.")
