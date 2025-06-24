from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
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
    def insert_batch(self, conn: connection, genre: List[GenreObj]) -> None:
        if not genre:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_genre
                (LIKE genre INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_genre (id, name, updatets) VALUES %s",
                [(t.id, t.name, t.updatets) for t in genre]
            )
            
            cur.execute("""
                UPDATE genre u SET
                    name = t.name,
                    updatets = t.updatets
                FROM temp_genre t
                WHERE u.id = t.id
            """)
            
            cur.execute("""
                INSERT INTO genre (id, name, updatets)
                SELECT t.id, t.name, t.updatets
                FROM temp_genre t
                LEFT JOIN genre u ON t.id = u.id
                WHERE u.id IS NULL
            """)


class GenreLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = GenreOriginRepository(pg_origin)
        self.stg = GenreDestRepository()
        self.log = log

    def load_genre(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM genre")
                last_loaded_date = cursor.fetchone()[0]
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
