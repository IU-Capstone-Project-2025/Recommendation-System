from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class BookTypeObj(BaseModel):
    """
    Data model representing a book-type relationship with validation.

    Attributes:
        bookid: Foreign key reference to the book
        typeid: Foreign key reference to the book type
        updatets: Timestamp of last update (for incremental loading)
    """

    bookid: int
    typeid: int
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "BookTypeObj":
        """
        Create BookTypeObj from database tuple.

        Args:
            data: Tuple containing (bookid, typeid, updatets)

        Returns:
            BookTypeObj: Validated relationship object
        """
        return cls(bookid=data[0], typeid=data[1], updatets=data[2])


class BookTypeOriginRepository:
    """
    Repository for retrieving book-type relationships from PostgreSQL.
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
    ) -> List[BookTypeObj]:
        """
        Get batch of book-type relationships updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[BookTypeObj]: Batch of relationship objects
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT bookid, typeid, updatets
                FROM booktype
                WHERE updatets > %(threshold)s
                ORDER BY updatets ASC, bookid ASC, typeid ASC
                LIMIT %(batch_size)s
                OFFSET %(offset)s
                """,
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [BookTypeObj.from_dict(row) for row in rows]


class BookTypeDestRepository:
    """
    Repository for loading book-type relationships into ClickHouse.
    Optimized for efficient batch inserts.
    """

    def insert_batch(
        self, conn: ClickhouseClient, relationships: List[BookTypeObj]
    ) -> None:
        """
        Insert batch of relationships into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            relationships: List of relationship objects to insert

        Note:
            Silently returns if input list is empty
        """
        if not relationships:
            return

        # Convert to ClickHouse-compatible format
        data = [[rel.bookid, rel.typeid, rel.updatets] for rel in relationships]

        conn.execute(
            """
            INSERT INTO BookType (bookid, typeid, updatets) VALUES
            """,
            data,
        )


class BookTypeLoader:
    """
    Orchestrates the complete ETL process for book-type relationships.
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
        self.origin = BookTypeOriginRepository(pg_origin)
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
            last_loaded = conn.execute("SELECT MAX(updatets) FROM BookType")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_book_types(last_loaded, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {batch_num}: {len(batch)} relationships to load")

            if not batch:
                self.log.info("No new relationships to load")
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
                self.log.info(f"Batch {batch_num}: {len(batch)} relationships to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE BookType FINAL")

            self.log.info(f"Load complete. Total relationships loaded: {total_loaded}")
