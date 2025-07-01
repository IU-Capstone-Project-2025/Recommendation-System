from logging import Logger
from typing import List, Tuple, Any, Optional

from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel


class UserObj(BaseModel):
    id: int
    username: str
    updatets: datetime
    
    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> 'UserObj':
        return cls(
            id=data[0],
            username=data[1],
            updatets=data[2]
        )


class UserOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_user(self, user_threshold: datetime, batch_size: int = 10000, offset: int = 0) -> List[UserObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, username, updatets
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
    def insert_batch(self, conn: ClickhouseClient, users: List[UserObj]) -> None:
        if not users:
            return

        data = [
            [
                user.id,
                user.username,
                user.updatets
            ]
            for user in users
        ]
        
        conn.execute(
            """
            INSERT INTO User (id, username, updatets) VALUES
            """,
            data
        )


class UserLoader:
    BATCH_SIZE = 10000
    
    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        self.ch_dest = ch_dest
        self.origin = UserOriginRepository(pg_origin)
        self.stg = UserDestRepository()
        self.log = log

    def load_user(self):
        with self.ch_dest.connection() as conn:

            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM User")[0][0]
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
