from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class User(BaseModel):
    """
    Data model representing a user with validation.

    Attributes:
        id: Unique identifier for the user
        username: Unique username for login/identification
        updatets: Timestamp of last update (for incremental loading)
    """

    id: int
    username: str
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "User":
        """
        Create User from database tuple.

        Args:
            data: Tuple containing (id, username, updatets)

        Returns:
            User: Validated user object
        """
        return cls(id=data[0], username=data[1], updatets=data[2])


class UserRepository:
    """
    Repository for retrieving user data from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_users(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[User]:
        """
        Get batch of users updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[User]: Batch of user objects
        """
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
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [User.from_dict(row) for row in rows]


class UserDestRepository:
    """
    Repository for loading user data into ClickHouse.
    Optimized for efficient batch inserts.
    """

    def insert_batch(self, conn: ClickhouseClient, users: List[User]) -> None:
        """
        Insert batch of users into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            users: List of user objects to insert

        Note:
            Silently returns if input list is empty
        """
        if not users:
            return

        # Convert to ClickHouse-compatible format
        data = [[user.id, user.username, user.updatets] for user in users]

        conn.execute(
            """
            INSERT INTO User (id, username, updatets) VALUES
            """,
            data,
        )


class UserLoader:
    """
    Orchestrates the complete ETL process for user data.
    Implements incremental loading with progress tracking.
    """

    BATCH_SIZE = 10000  # Optimal batch size for bulk operations

    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        """
        Initialize loader with connections and logger.

        Args:
            pg_origin: Source PostgreSQL connection
            ch_dest: Target ClickHouse connection
            log: Logger instance for progress tracking
        """
        self.ch_dest = ch_dest
        self.origin = UserRepository(pg_origin)
        self.stg = UserDestRepository()
        self.log = log

    def load_users(self) -> None:
        """
        Execute complete loading process:
        1. Gets last loaded timestamp from target
        2. Fetches batches from source updated since last load
        3. Inserts batches into target
        4. Repeats until all updates processed
        5. Logs progress and completion
        """
        with self.ch_dest.connection() as conn:
            # Get most recent update from target
            last_loaded = conn.execute("SELECT MAX(updatets) FROM User")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_users(last_loaded, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {batch_num}: {len(batch)} users to load")

            if not batch:
                self.log.info("No new users to load")
                return

            # Process batches until completion
            while batch:
                self.stg.insert_batch(conn, batch)

                # Update counters and get next batch
                offset += self.BATCH_SIZE
                total_loaded += len(batch)
                batch = self.origin.list_users(last_loaded, self.BATCH_SIZE, offset)
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} users to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE User FINAL")

            self.log.info(f"Load complete. Total users loaded: {total_loaded}")
