from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class Message(BaseModel):
    """
    Data model representing a user message with validation.

    Attributes:
        id: Unique message identifier
        userid: ID of the user who sent the message
        bookid: ID of the book the message relates to
        message: Content of the message
        isactual: Flag indicating if message is active/visible (True) or deleted (False)
        updatets: Timestamp of last update (for incremental loading)
    """

    id: int
    userid: int
    bookid: int
    message: str
    isactual: bool
    updatets: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "Message":
        """
        Create Message from database tuple.

        Args:
            data: Tuple containing (id, userid, bookid, message, isactual, updatets)

        Returns:
            Message: Validated message object
        """
        return cls(
            id=data[0],
            userid=data[1],
            bookid=data[2],
            message=data[3],
            isactual=data[4],
            updatets=data[5],
        )


class MessageRepository:
    """
    Repository for retrieving message data from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def list_messages(
        self, threshold: datetime, batch_size: int = 10000, offset: int = 0
    ) -> List[Message]:
        """
        Get batch of messages updated after threshold.

        Args:
            threshold: Minimum update timestamp to include
            batch_size: Number of records per batch (default: 10,000)
            offset: Pagination offset

        Returns:
            List[Message]: Batch of message objects
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, userid, bookid, message, isactual, updatets
                FROM message
                WHERE updatets > %(threshold)s
                ORDER BY updatets ASC, id ASC
                LIMIT %(batch_size)s
                OFFSET %(offset)s
                """,
                {"threshold": threshold, "batch_size": batch_size, "offset": offset},
            )
            rows = cur.fetchall()
        return [Message.from_dict(row) for row in rows]


class MessageDestinationRepository:
    """
    Repository for loading message data into ClickHouse.
    Optimized for efficient batch inserts of message content.
    """

    def insert_batch(self, conn: ClickhouseClient, messages: List[Message]) -> None:
        """
        Insert batch of messages into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            messages: List of message objects to insert

        Note:
            Silently returns if input list is empty
            Converts boolean isactual to ClickHouse-compatible UInt8
        """
        if not messages:
            return

        # Convert to ClickHouse-compatible format
        data = [
            [
                msg.id,
                msg.userid,
                msg.bookid,
                msg.message,
                1 if msg.isactual else 0,  # Convert bool to ClickHouse UInt8
                msg.updatets,
            ]
            for msg in messages
        ]

        conn.execute(
            """
            INSERT INTO Message (id, userid, bookid, message, isactual, updatets) VALUES
            """,
            data,
        )


class MessageLoader:
    """
    Orchestrates the complete ETL process for message data.
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
        self.origin = MessageRepository(pg_origin)
        self.stg = MessageDestinationRepository()
        self.log = log

    def load_messages(self) -> None:
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
            last_loaded = conn.execute("SELECT MAX(updatets) FROM Message")[0][0]
            if not last_loaded:
                last_loaded = datetime(1970, 1, 1)  # Initial load marker

            offset = 0
            total_loaded = 0
            batch_num = 1

            # Fetch initial batch
            batch = self.origin.list_messages(last_loaded, self.BATCH_SIZE, offset)
            self.log.info(f"Batch {batch_num}: {len(batch)} messages to load")

            if not batch:
                self.log.info("No new messages to load")
                return

            # Process batches until completion
            while batch:
                self.stg.insert_batch(conn, batch)

                # Update counters and get next batch
                offset += self.BATCH_SIZE
                total_loaded += len(batch)
                batch = self.origin.list_messages(last_loaded, self.BATCH_SIZE, offset)
                batch_num += 1
                self.log.info(f"Batch {batch_num}: {len(batch)} messages to load")

            # Optimize table
            conn.execute("OPTIMIZE TABLE Message FINAL")

            self.log.info(f"Load complete. Total messages loaded: {total_loaded}")
