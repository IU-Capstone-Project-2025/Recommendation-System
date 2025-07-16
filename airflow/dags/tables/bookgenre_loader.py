from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class BookGenreObj(BaseModel):
    """
    Pydantic model representing a book-genre relationship.

    Attributes:
        bookid: Foreign key referencing the book
        genreid: Foreign key referencing the genre
        updatets: Timestamp of last update (used for incremental loading)
    """

    bookid: int
    genreid: int
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "BookGenreObj":
        """
        Factory method to create BookGenreObj from database tuple.

        Args:
            data: Tuple containing (bookid, genreid, updatets)

        Returns:
            BookGenreObj: Validated relationship object
        """
        return cls(bookid=data[0], genreid=data[1], updatets=data[2])


class BookGenreOriginRepository:
    """
    Repository for accessing book-genre relationships from PostgreSQL source.
    Implements batched retrieval with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_book_genre(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[BookGenreObj]:
        """
        Retrieve batch of book-genre relationships updated after specified threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[BookGenreObj]: Batch of relationship objects
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT bookid, genreid, updatets
                FROM bookgenre
                WHERE updatets > %(threshold)s
                ORDER BY updatets ASC, bookid ASC, genreid ASC
                LIMIT %(batch_size)s
                OFFSET %(offset)s
                """,
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [BookGenreObj.from_dict(row) for row in rows]


class BookGenreDestRepository:
    """
    Repository for loading book-genre relationships into ClickHouse.
    Optimized for batch inserts.
    """

    def insert_batch(
        self, conn: ClickhouseClient, relationships: List[BookGenreObj]
    ) -> None:
        """
        Insert batch of relationships into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            relationships: List of relationship objects to insert

        Note:
            Silently skips empty batches
        """
        if not relationships:
            return

        # Convert objects to ClickHouse-compatible format
        data = [[rel.bookid, rel.genreid, rel.updatets] for rel in relationships]

        conn.execute(
            """
            INSERT INTO BookGenre (bookid, genreid, updatets) VALUES
            """,
            data,
        )


class BookGenreLoader:
    """
    Orchestrates the ETL process for book-genre relationships.
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
        self.origin = BookGenreOriginRepository(pg_origin)
        self.stg = BookGenreDestRepository()
        self.log = log

    def load_book_genre(self) -> None:
        """
        Execute complete loading process:
        1. Determine last loaded timestamp from target
        2. Fetch batches from source updated since last load
        3. Insert batches into target
        4. Repeat until all updates are processed
        5. Log progress and completion
        """
        with self.ch_dest.connection() as conn:
            # Get most recent update from target
            last_loaded = conn.execute("SELECT MAX(updatets) FROM BookGenre")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_book_genre(last_loaded, self.BATCH_SIZE, offset)
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
                batch = self.origin.list_book_genre(
                    last_loaded, self.BATCH_SIZE, offset
                )
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} relationships to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE BookGenre FINAL")

            self.log.info(f"Load complete. Total relationships loaded: {total_loaded}")
