from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class BookRating(BaseModel):
    """
    Data model representing a user's rating for a book.

    Attributes:
        userid: ID of the user who rated the book
        bookid: ID of the rated book
        score: Numeric rating score (e.g., 1-5 stars)
        isactual: Flag indicating if this is the current rating (True) or historical (False)
        updatets: Timestamp of when the rating was last updated
    """

    userid: int
    bookid: int
    score: int
    isactual: bool
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "BookRating":
        """
        Create BookRating from database tuple.

        Args:
            data: Tuple containing (userid, bookid, score, isactual, updatets)

        Returns:
            BookRating: Validated book rating object
        """
        return cls(
            userid=data[0],
            bookid=data[1],
            score=data[2],
            isactual=data[3],
            updatets=data[4],
        )


class BookRatingRepository:
    """
    Repository for retrieving book rating data from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_book_ratings(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[BookRating]:
        """
        Get batch of book ratings updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[BookRating]: Batch of book rating objects
        """
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
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [BookRating.from_dict(row) for row in rows]


class BookRatingDestRepository:
    """
    Repository for loading book rating data into ClickHouse.
    Optimized for efficient batch inserts.
    """

    def insert_batch(self, conn: ClickhouseClient, ratings: List[BookRating]) -> None:
        """
        Insert batch of book ratings into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            ratings: List of book rating objects to insert

        Note:
            Silently returns if input list is empty
            Converts boolean isactual to ClickHouse-compatible UInt8
        """
        if not ratings:
            return

        # Convert to ClickHouse-compatible format
        data = [
            [
                rating.userid,
                rating.bookid,
                rating.score,
                1 if rating.isactual else 0,  # Convert bool to ClickHouse UInt8
                rating.updatets,
            ]
            for rating in ratings
        ]

        conn.execute(
            """
            INSERT INTO Score (userid, bookid, score, isactual, updatets) VALUES
            """,
            data,
        )


class BookRatingLoader:
    """
    Orchestrates the complete ETL process for book rating data.
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
        self.origin = BookRatingRepository(pg_origin)
        self.stg = BookRatingDestRepository()
        self.log = log

    def load_book_ratings(self) -> None:
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
            last_loaded = conn.execute("SELECT MAX(updatets) FROM Score")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_book_ratings(last_loaded, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {batch_num}: {len(batch)} ratings to load")

            if not batch:
                self.log.info("No new book ratings to load")
                return

            # Process batches until completion
            while batch:
                self.stg.insert_batch(conn, batch)

                # Update counters and get next batch
                offset += self.BATCH_SIZE
                total_loaded += len(batch)
                batch = self.origin.list_book_ratings(
                    last_loaded, self.BATCH_SIZE, offset
                )
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} ratings to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE Score FINAL")

            self.log.info(f"Load complete. Total book ratings loaded: {total_loaded}")
