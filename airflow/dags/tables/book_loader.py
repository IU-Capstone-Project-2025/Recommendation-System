from logging import Logger
from typing import List, Tuple, Any, Optional
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class BookObj(BaseModel):
    """
    Pydantic model representing a book entity with validation.

    Attributes:
        id: Unique book identifier
        title: Title of the book (required)
        author: Author name (optional)
        year: Publication year (optional)
        imgurl: Cover image URL (optional)
        description: Book description text (optional)
        updatets: Timestamp of last update
    """

    id: int
    title: str
    author: Optional[str]
    year: Optional[int]
    imgurl: Optional[str]
    description: Optional[str]
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "BookObj":
        """
        Factory method to create BookObj from database tuple.

        Args:
            data: Tuple containing book data in order:
                  (id, title, author, year, imgurl, description, updatets)

        Returns:
            BookObj: Validated book object
        """
        return cls(
            id=data[0],
            title=data[1],
            author=data[2],
            year=data[3],
            imgurl=data[4],
            description=data[5],
            updatets=data[6],
        )


class BookOriginRepository:
    """
    Repository for accessing book data from PostgreSQL source.

    Handles batch retrieval of books updated after a specific threshold.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_book(
        self, book_threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[BookObj]:
        """
        Retrieve batch of books updated after specified threshold.

        Args:
            book_threshold: Minimum update timestamp to include
            batch_size: Number of records to fetch per batch
            offset: Pagination offset

        Returns:
            List[BookObj]: Batch of book objects
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, title, author, year, imgurl, description, updatets
                FROM book
                WHERE updatets > %(threshold)s
                ORDER BY updatets ASC, id ASC
                LIMIT %(batch_size)s
                OFFSET %(offset)s
                """,
                {
                    "threshold": book_threshold,
                    "batch_size": batch_size,
                    "offset": offset,
                },
            )
            rows = cur.fetchall()
        return [BookObj.from_dict(row) for row in rows]


class BookDestRepository:
    """
    Repository for loading book data into ClickHouse destination.

    Handles batch inserts with proper type conversion.
    """

    def insert_batch(self, conn: ClickhouseClient, books: List[BookObj]) -> None:
        """
        Insert batch of books into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            books: List of book objects to insert

        Note:
            Silently skips if books list is empty
        """
        if not books:
            return

        # Convert objects to ClickHouse-compatible data format
        data = [
            [
                book.id,
                book.title,
                book.author,
                book.year,
                book.imgurl,
                book.description,
                book.updatets,
            ]
            for book in books
        ]

        conn.execute(
            """
            INSERT INTO Book (id, title, author, year, imgurl, description, updatets) VALUES
            """,
            data,
        )


class BookLoader:
    """
    Orchestrates the ETL process for loading books from PostgreSQL to ClickHouse.

    Features:
    - Batch processing for memory efficiency
    - Incremental loading based on update timestamps
    - Progress logging
    """

    BATCH_SIZE = 10000  # Optimal batch size for bulk inserts

    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        """
        Initialize loader with connections and logger.

        Args:
            pg_origin: Source PostgreSQL connection
            ch_dest: Target ClickHouse connection
            log: Logger instance for progress tracking
        """
        self.ch_dest = ch_dest
        self.origin = BookOriginRepository(pg_origin)
        self.stg = BookDestRepository()
        self.log = log

    def load_book(self) -> None:
        """
        Execute complete book loading process.

        Steps:
        1. Determine last loaded timestamp from target
        2. Fetch batches from source updated since last load
        3. Insert batches into target
        4. Repeat until all updates are processed
        5. Log progress and completion
        """
        with self.ch_dest.connection() as conn:
            # Get most recent update timestamp from target
            last_loaded_date = conn.execute("SELECT MAX(updatets) FROM Book")[0][0]
            if not last_loaded_date:
                last_loaded_date = datetime(1970, 1, 1)  # Initial load

            offset = 0
            book_count = 0
            batch_count = 1

            # Fetch first batch
            load_queue = self.origin.list_book(
                last_loaded_date, self.BATCH_SIZE, offset
            )
            self.log.info(f"Batch {batch_count}: {len(load_queue)} books to load.")

            if not load_queue:
                self.log.info("No new books to load.")
                return

            # Process batches until queue is empty
            while load_queue:
                self.stg.insert_batch(conn, load_queue)

                # Update counters and fetch next batch
                offset += self.BATCH_SIZE
                book_count += len(load_queue)
                load_queue = self.origin.list_book(
                    last_loaded_date, self.BATCH_SIZE, offset
                )
                batch_count += 1
                self.log.info(f"Batch {batch_count}: {len(load_queue)} books to load.")
            
            # Optimize table
            conn.execute("OPTIMIZE TABLE Book FINAL")

            self.log.info(f"Load completed. Total books updated: {book_count}.")
