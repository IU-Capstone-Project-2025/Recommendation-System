from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from pydantic import BaseModel


class ScoreObj(BaseModel):
    userid: int
    bookid: int
    score: int
    isactual: bool
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'ScoreObj':
        return cls(
            userid=data[0],
            bookid=data[1],
            score=data[2],
            isactual=data[3],
            updatets=data[4]
        )


class ScoreOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_score(self, score_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[ScoreObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT userid, bookid, score, isactual, updatets
                    FROM score
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, userid ASC, bookid ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": score_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [ScoreObj.from_dict(row) for row in rows]


class ScoreDestRepository:
    def insert_batch(self, conn: connection, score: List[ScoreObj]) -> None:
        if not score:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_score
                (LIKE score INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_score (userid, bookid, score, isactual, updatets) VALUES %s",
                [(t.userid, t.bookid, t.score, t.isactual, t.updatets) for t in score]
            )
            
            cur.execute("""
                UPDATE score u SET
                    score = t.score,
                    isactual = t.isactual,
                    updatets = t.updatets
                FROM temp_score t
                WHERE u.userid = t.userid AND u.bookid = t.bookid
            """)
            
            cur.execute("""
                INSERT INTO score (userid, bookid, score, isactual, updatets)
                SELECT t.userid, t.bookid, t.score, t.isactual, t.updatets
                FROM temp_score t
                LEFT JOIN score u ON t.userid = u.userid AND t.bookid = u.bookid
                WHERE u.userid IS NULL AND u.bookid IS NULL
            """)


class ScoreLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ScoreOriginRepository(pg_origin)
        self.stg = ScoreDestRepository()
        self.log = log

    def load_score(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM score")
                last_loaded_date = cursor.fetchone()[0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            score_count = 0
            count = 1
            load_queue = self.origin.list_score(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} score to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                score_count += len(load_queue)
                load_queue = self.origin.list_score(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} score to load.")

            self.log.info(f"Load finished total updated score count: {score_count}.")
