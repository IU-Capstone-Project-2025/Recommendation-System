from logging import Logger
from typing import List, Tuple, Any

from lib.pg_connect import PgConnect
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from pydantic import BaseModel


class UserObj(BaseModel):
    id: int
    username: str
    codedpass: str
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'UserObj':
        return cls(
            id=data[0],
            username=data[1],
            codedpass=data[2],
            updatets=data[3]
        )


class UserOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_user(self, user_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[UserObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, username, codedpass, updatets
                    FROM "User"
                    WHERE updatets > %(threshold)s
                    ORDER BY updatets ASC, id ASC
                    LIMIT %(batch_size)s
                    OFFSET %(offset)s
                """,
                {
                    "threshold": user_threshold,
                    "batch_size": batch_size,
                    "offset": offset
                }
            )
            rows = cur.fetchall()
        return [UserObj.from_dict(row) for row in rows]

class UserDestRepository:
    def insert_batch(self, conn: connection, user: List[UserObj]) -> None:
        if not user:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_user
                (LIKE "User" INCLUDING DEFAULTS) ON COMMIT DROP
            """)
            
            execute_values(
                cur,
                "INSERT INTO temp_user (id, username, codedpass, updatets) VALUES %s",
                [(t.id, t.username, t.codedpass, t.updatets) for t in user]
            )
            
            cur.execute("""
                UPDATE "User" u SET
                    username = t.username,
                    codedpass = t.codedpass,
                    updatets = t.updatets
                FROM temp_user t
                WHERE u.id = t.id
            """)
            
            cur.execute("""
                INSERT INTO "User" (id, username, codedpass, updatets)
                SELECT t.id, t.username, t.codedpass, t.updatets
                FROM temp_user t
                LEFT JOIN "User" u ON t.id = u.id
                WHERE u.id IS NULL
            """)


class UserLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UserOriginRepository(pg_origin)
        self.stg = UserDestRepository()
        self.log = log

    def load_user(self):
        with self.pg_dest.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(updatets) FROM \"User\"")
                last_loaded_date = cursor.fetchone()[0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)

            offset = 0
            user_count = 0
            count = 1
            load_queue = self.origin.list_user(last_loaded_date, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {count}: {len(load_queue)} user to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            while load_queue:
                self.stg.insert_batch(conn, load_queue)
                
                offset += self.BATCH_SIZE
                user_count += len(load_queue)
                load_queue = self.origin.list_user(last_loaded_date, self.BATCH_SIZE, offset)
                count += 1
                self.log.info(f"Batch {count}: {len(load_queue)} user to load.")

            self.log.info(f"Load finished total updated user count: {user_count}.")
