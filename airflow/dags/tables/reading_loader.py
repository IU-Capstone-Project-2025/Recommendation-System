from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class ReadingStatus(BaseModel):
    """
    Data model representing a user's reading status for a book.

    Attributes:
        userid: ID of the user
        bookid: ID of the book being read
        isactual: Boolean flag indicating if this is the current reading status
        updatets: Timestamp of when the status was last updated
    """

    userid: int
    bookid: int
    isactual: bool
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "ReadingStatus":
        """
        Create ReadingStatus from database tuple.

        Args:
            data: Tuple containing (userid, bookid, isactual, updatets)

        Returns:
            ReadingStatus: Validated reading status object
        """
        return cls(
            userid=data[0],
            bookid=data[1],
            isactual=data[2],
            updatets=data[3],  # Fixed index from original implementation
        )


class ReadingStatusRepository:
    """
    Repository for retrieving reading status data from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_reading_statuses(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[ReadingStatus]:
        """
        Get batch of reading statuses updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[ReadingStatus]: Batch of reading status objects
        """
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
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [ReadingStatus.from_dict(row) for row in rows]


class ReadingStatusDestRepository:
    """
    Repository for loading reading status data into ClickHouse.
    Optimized for efficient batch inserts.
    """

    def insert_batch(
        self, conn: ClickhouseClient, statuses: List[ReadingStatus]
    ) -> None:
        """
        Insert batch of reading statuses into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            statuses: List of reading status objects to insert

        Note:
            Silently returns if input list is empty
            Converts boolean isactual to ClickHouse-compatible UInt8
        """
        if not statuses:
            return

        # Convert to ClickHouse-compatible format
        data = [
            [status.userid, status.bookid, 1 if status.isactual else 0, status.updatets]
            for status in statuses
        ]

        conn.execute(
            """
            INSERT INTO Reading (userid, bookid, isactual, updatets) VALUES
            """,
            data,
        )


class ReadingStatusLoader:
    """
    Orchestrates the complete ETL process for reading status data.
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
        self.origin = ReadingStatusRepository(pg_origin)
        self.stg = ReadingStatusDestRepository()
        self.log = log

    def load_reading_statuses(self) -> None:
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
            last_loaded = conn.execute("SELECT MAX(updatets) FROM Reading")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_reading_statuses(
                last_loaded, self.BATCH_SIZE, offset
            )
            self.log.info(f"Batch {batch_num}: {len(batch)} statuses to load")

            if not batch:
                self.log.info("No new reading statuses to load")
                return

            # Process batches until completion
            while batch:
                self.stg.insert_batch(conn, batch)

                # Update counters and get next batch
                offset += self.BATCH_SIZE
                total_loaded += len(batch)
                batch = self.origin.list_reading_statuses(
                    last_loaded, self.BATCH_SIZE, offset
                )
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} statuses to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE Reading FINAL")

            self.log.info(
                f"Load complete. Total reading statuses loaded: {total_loaded}"
            )
