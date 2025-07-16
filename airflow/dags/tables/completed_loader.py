from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class CompletedBookRecord(BaseModel):
    """
    Data model representing a user's completed book record.

    Attributes:
        userid: ID of the user who completed the book
        bookid: ID of the completed book
        isactual: Flag indicating if this is the current status (True) or historical (False)
        updatets: Timestamp of when this record was last updated
    """

    userid: int
    bookid: int
    isactual: bool
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "CompletedBookRecord":
        """
        Create CompletedBookRecord from database tuple.

        Args:
            data: Tuple containing (userid, bookid, isactual, updatets)

        Returns:
            CompletedBookRecord: Validated completed book record
        """
        return cls(
            userid=data[0],
            bookid=data[1],
            isactual=data[2],
            updatets=data[3],  # Fixed index from original implementation
        )


class CompletedBooksRepository:
    """
    Repository for retrieving completed book records from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_completed_books(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[CompletedBookRecord]:
        """
        Get batch of completed book records updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[CompletedBookRecord]: Batch of completed book records
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT userid, bookid, isactual, updatets
                FROM completed
                WHERE updatets > %(threshold)s
                ORDER BY updatets ASC, userid ASC, bookid ASC
                LIMIT %(batch_size)s
                OFFSET %(offset)s
                """,
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [CompletedBookRecord.from_dict(row) for row in rows]


class CompletedBooksDestRepository:
    """
    Repository for loading completed book records into ClickHouse.
    Optimized for efficient batch inserts.
    """

    def insert_batch(
        self, conn: ClickhouseClient, records: List[CompletedBookRecord]
    ) -> None:
        """
        Insert batch of completed book records into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            records: List of completed book records to insert

        Note:
            Silently returns if input list is empty
        """
        if not records:
            return

        # Convert to ClickHouse-compatible format
        data = [
            [rec.userid, rec.bookid, int(rec.isactual), rec.updatets] for rec in records
        ]

        conn.execute(
            """
            INSERT INTO Completed (userid, bookid, isactual, updatets) VALUES
            """,
            data,
        )


class CompletedBooksLoader:
    """
    Orchestrates the complete ETL process for completed book records.
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
        self.origin = CompletedBooksRepository(pg_origin)
        self.stg = CompletedBooksDestRepository()
        self.log = log

    def load_completed_books(self) -> None:
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
            last_loaded = conn.execute("SELECT MAX(updatets) FROM Completed")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_completed_books(
                last_loaded, self.BATCH_SIZE, offset
            )
            self.log.info(f"Batch {batch_num}: {len(batch)} records to load")

            if not batch:
                self.log.info("No new completed book records to load")
                return

            # Process batches until completion
            while batch:
                self.stg.insert_batch(conn, batch)

                # Update counters and get next batch
                offset += self.BATCH_SIZE
                total_loaded += len(batch)
                batch = self.origin.list_completed_books(
                    last_loaded, self.BATCH_SIZE, offset
                )
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} records to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE Completed FINAL")

            self.log.info(
                f"Load complete. Total completed book records loaded: {total_loaded}"
            )
