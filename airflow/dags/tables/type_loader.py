from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class BookType(BaseModel):
    """
    Data model representing a book type with validation.

    Attributes:
        id: Unique identifier for the book type
        name: Name of the type (e.g., "Hardcover", "Ebook")
        updatets: Timestamp of last update (for incremental loading)
    """

    id: int
    name: str
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "BookType":
        """
        Create BookType from database tuple.

        Args:
            data: Tuple containing (id, name, updatets)

        Returns:
            BookType: Validated book type object
        """
        return cls(id=data[0], name=data[1], updatets=data[2])


class BookTypeRepository:
    """
    Repository for retrieving book type data from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_book_types(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[BookType]:
        """
        Get batch of book types updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[BookType]: Batch of book type objects
        """
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
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [BookType.from_dict(row) for row in rows]


class BookTypeDestRepository:
    """
    Repository for loading book type data into ClickHouse.
    Optimized for efficient batch inserts.
    """

    def insert_batch(self, conn: ClickhouseClient, types: List[BookType]) -> None:
        """
        Insert batch of book types into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            types: List of book type objects to insert

        Note:
            Silently returns if input list is empty
        """
        if not types:
            return

        # Convert to ClickHouse-compatible format
        data = [
            [book_type.id, book_type.name, book_type.updatets] for book_type in types
        ]

        conn.execute(
            """
            INSERT INTO Type (id, name, updatets) VALUES
            """,
            data,
        )


class BookTypeLoader:
    """
    Orchestrates the complete ETL process for book type data.
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
        self.origin = BookTypeRepository(pg_origin)
        self.stg = BookTypeDestRepository()
        self.log = log

    def load_book_types(self) -> None:
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
            last_loaded = conn.execute("SELECT MAX(updatets) FROM Type")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_book_types(last_loaded, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {batch_num}: {len(batch)} types to load")

            if not batch:
                self.log.info("No new book types to load")
                return

            # Process batches until completion
            while batch:
                self.stg.insert_batch(conn, batch)

                # Update counters and get next batch
                offset += self.BATCH_SIZE
                total_loaded += len(batch)
                batch = self.origin.list_book_types(
                    last_loaded, self.BATCH_SIZE, offset
                )
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} types to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE Type FINAL")

            self.log.info(f"Load complete. Total book types loaded: {total_loaded}")
